package cluster

import (
	"context"
	"crypto/tls"
	"errors"
	"log/slog"
	"net"
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
	log         *slog.Logger
	pending     chan struct{}
	raft, http  chan net.Conn
	done        chan struct{}
	once        sync.Once
}

func listen(address string, credentials *Credentials, log *slog.Logger) (*listener, error) {
	inner, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}
	l := &listener{inner: inner, credentials: credentials, log: log, pending: make(chan struct{}, handshakes), raft: make(chan net.Conn), http: make(chan net.Conn), done: make(chan struct{})}
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
	_ = conn.SetDeadline(time.Time{})
	target := l.http
	if conn.ConnectionState().NegotiatedProtocol == protocolRaft {
		target = l.raft
	}
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
	return dial(ctx, s.l.credentials, s.protocol, string(address))
}

// dial connects to a node of the cluster for protocol; ctx bounds the
// connection and the handshake.
func dial(ctx context.Context, credentials *Credentials, protocol, address string) (net.Conn, error) {
	dialer := &tls.Dialer{Config: credentials.client(protocol)}
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
