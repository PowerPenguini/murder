package cluster

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/raft"
)

// handshakeTimeout bounds a TLS handshake, so a peer that never finishes
// one holds nothing for long; handshakes bounds how many run at once, so
// connections that never prove anything cannot use up the agent's files.
const (
	handshakeTimeout = 10 * time.Second
	handshakes       = 64
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
	raft, http, join chan net.Conn
	done             chan struct{}
	once             sync.Once
}

func listen(address string, credentials *Credentials, cluster func() string, log *slog.Logger) (*listener, error) {
	inner, err := listenConfig().Listen(context.Background(), "tcp", address)
	if err != nil {
		return nil, err
	}
	l := &listener{inner: inner, credentials: credentials, cluster: cluster, log: log, pending: make(chan struct{}, handshakes), raft: make(chan net.Conn), http: make(chan net.Conn), join: make(chan net.Conn), done: make(chan struct{})}
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
	if conn.ConnectionState().NegotiatedProtocol == protocolJoin {
		target = l.join
	} else if conn.ConnectionState().NegotiatedProtocol == protocolRaft {
		// Raft takes entries, and votes, from its own cluster only: not
		// from another cluster with the same master key, such as the one
		// a backup was restored from, nor from any cluster while this
		// node belongs to none.
		theirs, err := readClusterHeader(conn)
		if ours := l.cluster(); err != nil || ours == "" || theirs != ours {
			l.log.Debug("refused a Raft connection of another cluster", "from", conn.RemoteAddr().String(), "theirs", theirs, "ours", ours, "error", err)
			_ = conn.Close()
			return
		}
		target = l.raft
	}
	_ = conn.SetDeadline(time.Time{})
	select {
	case target <- conn:
	case <-l.done:
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
		if s.protocol == protocolJoin {
			// net/http closes a TLS connection of a protocol it does not
			// know; the invitation's is HTTP/1.1 over the handshake made.
			return handshaken{conn}, nil
		}
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
	return conn, nil
}

// handshaken is a TLS connection whose handshake is done, served as a
// plain one.
type handshaken struct{ net.Conn }

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
