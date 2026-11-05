package cluster

import (
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"errors"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/raft"

	"github.com/PowerPenguini/murder/internal/throttle"
)

// handshakeTimeout bounds a TLS handshake, so a peer that never finishes
// one holds nothing for long; handshakes bounds how many run at once, so
// connections that never prove anything cannot use up the agent's files.
const (
	handshakeTimeout = 10 * time.Second
	handshakes       = 64
	// joins bounds the connections of hosts with an invitation, which
	// prove nothing in their handshake, and joinTimeout how long each
	// lasts.
	joins       = 8
	joinTimeout = 30 * time.Second
)

// listener accepts nodes of the cluster on one port and hands each
// connection, by the protocol it negotiated, to Raft or to the HTTP server.
type listener struct {
	inner       net.Listener
	credentials *Credentials
	// cluster names the cluster this node belongs to; "" before it
	// belongs to one.
	cluster          func() string
	log              *slog.Logger
	pending          chan struct{}
	joining          chan struct{}
	raft, http, join chan net.Conn
	done             chan struct{}
	once             sync.Once
	// raftPeer, once set, checks the node a Raft connection comes from:
	// none is taken before.
	raftPeer atomic.Pointer[func(name string, key ed25519.PublicKey) error]
	// raftConns are the Raft connections taken and made, with the node at
	// the other end of each, so that one with a node no longer a member, or
	// no longer with that key, is closed.
	mu        sync.Mutex
	raftConns map[*raftConn]struct{}
	// refusals says which Raft connections were refused, without a line
	// for each time a peer tries again.
	refusals throttle.Log
}

func listen(address string, credentials *Credentials, cluster func() string, log *slog.Logger) (*listener, error) {
	inner, err := listenConfig().Listen(context.Background(), "tcp", address)
	if err != nil {
		return nil, err
	}
	l := &listener{inner: inner, credentials: credentials, cluster: cluster, log: log, pending: make(chan struct{}, handshakes), joining: make(chan struct{}, joins), raft: make(chan net.Conn), http: make(chan net.Conn), join: make(chan net.Conn), done: make(chan struct{}), raftConns: map[*raftConn]struct{}{}}
	go l.accept()
	return l, nil
}

func (l *listener) accept() {
	config := l.credentials.server()
	for {
		conn, err := l.inner.Accept()
		if err != nil {
			select {
			case <-l.done:
				return
			default:
			}
			var temporary interface{ Temporary() bool }
			if errors.As(err, &temporary) && temporary.Temporary() {
				time.Sleep(50 * time.Millisecond)
				continue
			}
			l.log.Error("the cluster port stopped accepting connections", "error", err)
			l.close()
			return
		}
		select {
		case l.pending <- struct{}{}:
			go l.route(tls.Server(conn, config))
		default:
			_ = conn.Close()
		}
	}
}

func (l *listener) route(conn *tls.Conn) {
	_ = conn.SetDeadline(time.Now().Add(handshakeTimeout))
	err := conn.Handshake()
	<-l.pending
	if err != nil {
		_ = conn.Close()
		return
	}
	target := l.http
	var routed net.Conn = conn
	if conn.ConnectionState().NegotiatedProtocol == protocolJoin {
		select {
		case l.joining <- struct{}{}:
		default:
			_ = conn.Close()
			return
		}
		var release sync.Once
		wrapped := &handshaken{Conn: conn, release: func() { release.Do(func() { <-l.joining }) }}
		// The whole exchange is one request and its answer.
		_ = conn.SetDeadline(time.Now().Add(joinTimeout))
		select {
		case l.join <- wrapped:
		case <-l.done:
			_ = wrapped.Close()
		}
		return
	}
	if conn.ConnectionState().NegotiatedProtocol == protocolRaft {
		// Raft takes entries, and votes, from its own cluster only: not
		// from another cluster with the same master key, such as the one
		// a backup was restored from, nor from any cluster while this
		// node belongs to none.
		theirs, err := readClusterHeader(conn)
		if ours := l.cluster(); err != nil || ours == "" || theirs != ours {
			state := conn.ConnectionState()
			name, _, _ := peerIdentity(&state)
			l.refusals.Warn(l.log, refusedSource(name, conn.RemoteAddr().String()), "refused a Raft connection of another cluster", "from", conn.RemoteAddr().String(), "node", name, "theirs", theirs, "ours", ours, "error", err)
			_ = conn.Close()
			return
		}
		// Nor from a node that is not one of its members, or names one in
		// a certificate without the member's identity key: the master key
		// alone makes a certificate for any name.
		state := conn.ConnectionState()
		name, key, err := peerIdentity(&state)
		if check := l.raftPeer.Load(); err == nil {
			if check == nil {
				err = errors.New("the node is starting")
			} else {
				err = (*check)(name, key)
			}
		}
		if err != nil {
			l.refusals.Warn(l.log, refusedSource(name, conn.RemoteAddr().String()), "refused a Raft connection of a node that is not a member", "from", conn.RemoteAddr().String(), "node", name, "error", err)
			_ = conn.Close()
			return
		}
		routed = l.track(conn, name, key)
		target = l.raft
	}
	_ = conn.SetDeadline(time.Time{})
	select {
	case target <- routed:
	case <-l.done:
		_ = routed.Close()
	}
}

// refusedSource names a refused peer for the log of refusals: by its
// node name, else by its address without the port, which changes each
// time it connects.
func refusedSource(node, address string) string {
	if node != "" {
		return node
	}
	if host, _, err := net.SplitHostPort(address); err == nil {
		return host
	}
	return address
}

// raftConn is a Raft connection taken from node, or made to it, with
// identity key key.
type raftConn struct {
	net.Conn
	node string
	key  ed25519.PublicKey
	l    *listener
	once sync.Once
}

func (r *raftConn) Close() error {
	r.once.Do(func() {
		r.l.mu.Lock()
		delete(r.l.raftConns, r)
		r.l.mu.Unlock()
	})
	return r.Conn.Close()
}

// track keeps a Raft connection taken from node, until it closes.
func (l *listener) track(conn net.Conn, node string, key ed25519.PublicKey) net.Conn {
	tracked := &raftConn{Conn: conn, node: node, key: key, l: l}
	l.mu.Lock()
	l.raftConns[tracked] = struct{}{}
	l.mu.Unlock()
	return tracked
}

// sweep closes the Raft connections, taken or made, with nodes check no
// longer accepts, such as one removed from the cluster since, or given
// another key.
func (l *listener) sweep(check func(name string, key ed25519.PublicKey) error) {
	l.mu.Lock()
	var refused []*raftConn
	for conn := range l.raftConns {
		if check(conn.node, conn.key) != nil {
			refused = append(refused, conn)
		}
	}
	l.mu.Unlock()
	for _, conn := range refused {
		l.log.Info("closed a Raft connection of a node that is no longer a member", "node", conn.node)
		_ = conn.Close()
	}
}

func (l *listener) close() error {
	var err error
	l.once.Do(func() {
		close(l.done)
		err = l.inner.Close()
	})
	return err
}

// raftLayer is the stream layer Raft's transport uses.
func (l *listener) raftLayer() raft.StreamLayer {
	return &stream{l: l, conns: l.raft, protocol: protocolRaft}
}

// httpListener is what the cluster's HTTP server serves.
func (l *listener) httpListener() net.Listener {
	return &stream{l: l, conns: l.http, protocol: protocolHTTP}
}

// joinListener is where hosts bring invitations.
func (l *listener) joinListener() net.Listener {
	return &stream{l: l, conns: l.join, protocol: protocolJoin}
}

type stream struct {
	l        *listener
	conns    chan net.Conn
	protocol string
}

func (s *stream) Accept() (net.Conn, error) {
	select {
	case conn := <-s.conns:
		return conn, nil
	case <-s.l.done:
		return nil, net.ErrClosed
	}
}

// Close closes the port for both: Raft and the HTTP server stop together.
func (s *stream) Close() error   { return s.l.close() }
func (s *stream) Addr() net.Addr { return s.l.inner.Addr() }

func (s *stream) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	conn, err := dial(ctx, s.l.credentials, s.protocol, string(address))
	if err != nil || s.protocol != protocolRaft {
		return conn, err
	}
	_ = conn.SetWriteDeadline(time.Now().Add(timeout))
	if _, err := io.WriteString(conn, clusterHeaderPrefix+s.l.cluster()+"\n"); err != nil {
		_ = conn.Close()
		return nil, err
	}
	_ = conn.SetWriteDeadline(time.Time{})
	// Kept with the node it reached, so the sweep closes it once that node
	// is no member any more, or no longer has that key: Raft dials again,
	// and the dial checks it.
	state := conn.(*tls.Conn).ConnectionState()
	name, key, err := peerIdentity(&state)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	return s.l.track(conn, name, key), nil
}

// handshaken is a TLS connection of a host with an invitation, its
// handshake done, served as a plain one: net/http closes a TLS connection
// of a protocol it does not know. Closing it frees its place.
type handshaken struct {
	net.Conn
	release func()
}

func (h *handshaken) Close() error {
	defer h.release()
	return h.Conn.Close()
}

// A Raft connection begins with the name of the dialing node's cluster.
const clusterHeaderPrefix = "murder-cluster "

// readClusterHeader reads the cluster name a Raft connection begins with,
// a byte at a time, so nothing of what follows is read.
func readClusterHeader(conn net.Conn) (string, error) {
	var line []byte
	one := make([]byte, 1)
	for len(line) < 256 {
		if _, err := io.ReadFull(conn, one); err != nil {
			return "", err
		}
		if one[0] == '\n' {
			name, ok := strings.CutPrefix(string(line), clusterHeaderPrefix)
			if !ok {
				return "", errors.New("no cluster name")
			}
			return name, nil
		}
		line = append(line, one[0])
	}
	return "", errors.New("cluster name too long")
}

// dial connects to a node of the cluster for protocol; ctx bounds the
// connection and the handshake.
func dial(ctx context.Context, credentials *Credentials, protocol, address string) (net.Conn, error) {
	dialer := &tls.Dialer{NetDialer: netDialer(), Config: credentials.client(protocol)}
	conn, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return nil, err
	}
	if conn.(*tls.Conn).ConnectionState().NegotiatedProtocol != protocol {
		_ = conn.Close()
		return nil, errors.New("the peer does not speak " + protocol)
	}
	return conn, nil
}
