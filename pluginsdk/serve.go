package pluginsdk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// DefaultSocket is where the plugin listens in its container, unless
// MURDER_PLUGIN_SOCKET says another path.
const DefaultSocket = "/run/murder-plugin/plugin.sock"

var (
	registered *plugin
	once       sync.Mutex
)

// Register makes the program a plugin: m is its manifest, f says where
// each request goes, or refuses it (see Refuse). f is called for many
// requests at once.
func Register(m Manifest, f func(*Request) (*Forward, error)) {
	once.Lock()
	defer once.Unlock()
	m.ABI = ABI
	registered = newPlugin(m, f)
}

// Run runs the registered plugin: with -manifest, it prints the manifest
// for the image's label (see the package's doc) and exits; otherwise it
// serves the edge on its socket until it is stopped.
func Run() {
	once.Lock()
	p := registered
	once.Unlock()
	if p == nil {
		fmt.Fprintln(os.Stderr, "the program registers no plugin: call pluginsdk.Register before pluginsdk.Run")
		os.Exit(2)
	}
	if len(os.Args) > 1 && (os.Args[1] == "-manifest" || os.Args[1] == "--manifest") {
		encoded, _ := json.Marshal(p.manifest)
		fmt.Println(string(encoded))
		return
	}
	path := os.Getenv("MURDER_PLUGIN_SOCKET")
	if path == "" {
		path = DefaultSocket
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	if err := Serve(ctx, path, p.handler()); err != nil {
		fmt.Fprintln(os.Stderr, "the plugin stops:", err)
		os.Exit(1)
	}
}

// Handler is the registered plugin's interface to the edge, for a test
// that serves it on a socket of its own.
func Handler() http.Handler {
	once.Lock()
	defer once.Unlock()
	if registered == nil {
		panic("pluginsdk.Handler: no plugin registered")
	}
	return registered.handler()
}

// Serve serves handler on a unix socket at path, which any user may
// connect to (the edge's is not the plugin's), until ctx ends; it then
// finishes the calls under way.
func Serve(ctx context.Context, path string, handler http.Handler) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	listener, err := net.Listen("unix", path)
	if err != nil {
		return err
	}
	if err := os.Chmod(path, 0o666); err != nil {
		listener.Close()
		return err
	}
	server := &http.Server{Handler: handler, ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 2 * time.Minute}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return server.Shutdown(shutdown)
}

// Log writes to the container's log, which `docker logs` shows.
func Log(format string, args ...any) { fmt.Fprintf(os.Stderr, format+"\n", args...) }

// The most a call from the edge brings: a request's, with a body of
// 16 MB in base64; and a chunk of an answer's body.
const (
	maxRequestCall = 16<<20*4/3 + 1<<20
	maxCall        = 1 << 20
)

// sessionIdle is how long a request's meter is kept without a call from
// the edge: the edge ends each, unless it stopped.
const sessionIdle = 10 * time.Minute

// plugin serves a registered plugin.
type plugin struct {
	manifest Manifest
	forward  func(*Request) (*Forward, error)

	mu       sync.Mutex
	sessions map[string]*session
	swept    time.Time
}

// session is a request the plugin meters, from its forward to its end.
type session struct {
	mu      sync.Mutex
	meter   Meter
	counter Counter
	reading bool
	// used is when the edge last called, in Unix nanoseconds.
	used atomic.Int64
}

func newPlugin(m Manifest, f func(*Request) (*Forward, error)) *plugin {
	return &plugin{manifest: m, forward: f, sessions: map[string]*session{}, swept: time.Now()}
}

func (p *plugin) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /manifest", func(w http.ResponseWriter, r *http.Request) { answer(w, p.manifest) })
	mux.HandleFunc("POST /request", p.request)
	mux.HandleFunc("POST /response", p.response)
	mux.HandleFunc("POST /body", p.body)
	mux.HandleFunc("POST /end", p.end)
	return mux
}

func answer(w http.ResponseWriter, value any) {
	encoded, err := json.Marshal(value)
	if err != nil {
		encoded, _ = json.Marshal(map[string]string{"error": err.Error()})
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(encoded)
}

// panicked is a panic of the plugin's, which the edge takes as the
// plugin failing, not as the request refused.
type panicked struct{ value any }

func (p panicked) Error() string { return fmt.Sprintf("panic: %v", p.value) }

// guard turns a panic into an error the edge is answered.
func guard(err *error) {
	if r := recover(); r != nil {
		*err = panicked{r}
	}
}

type forwarded struct {
	URL         string              `json:"url"`
	Header      map[string][]string `json:"header,omitempty"`
	Drop        []string            `json:"drop,omitempty"`
	ReplaceBody bool                `json:"replaceBody,omitempty"`
	Body        []byte              `json:"body,omitempty"`
	Detail      string              `json:"detail,omitempty"`
	Meter       bool                `json:"meter,omitempty"`
	Refuse      *Refusal            `json:"refuse,omitempty"`
	Error       string              `json:"error,omitempty"`
}

func (p *plugin) request(w http.ResponseWriter, r *http.Request) {
	var request Request
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxRequestCall)).Decode(&request); err != nil {
		answer(w, forwarded{Error: "the edge's request is not JSON: " + err.Error()})
		return
	}
	request.ctx = r.Context()
	to, err := func() (to *Forward, err error) {
		defer guard(&err)
		return p.forward(&request)
	}()
	var refusal *Refusal
	switch {
	case errors.As(err, new(panicked)):
		answer(w, forwarded{Error: err.Error()})
		return
	case errors.As(err, &refusal):
		answer(w, forwarded{Refuse: refusal})
		return
	case err != nil:
		answer(w, forwarded{Refuse: &Refusal{Status: 400, Message: err.Error()}})
		return
	case to == nil:
		answer(w, forwarded{Error: "the plugin forwards the request nowhere"})
		return
	}
	if to.Meter != nil {
		id := r.Header.Get("Murder-Session")
		if id == "" {
			answer(w, forwarded{Error: "the edge names no session to meter the request in"})
			return
		}
		p.mu.Lock()
		p.sweep()
		s := &session{meter: to.Meter}
		s.used.Store(time.Now().UnixNano())
		p.sessions[id] = s
		p.mu.Unlock()
	}
	answer(w, forwarded{URL: to.URL, Header: to.Header, Drop: to.Drop, ReplaceBody: to.Body != nil, Body: to.Body, Detail: to.Detail, Meter: to.Meter != nil})
}

// sweep lets go of sessions the edge left, once a minute; p.mu is held.
func (p *plugin) sweep() {
	if time.Since(p.swept) < time.Minute {
		return
	}
	p.swept = time.Now()
	for id, s := range p.sessions {
		if time.Since(time.Unix(0, s.used.Load())) > sessionIdle {
			delete(p.sessions, id)
		}
	}
}

// session finds the session a call is of, held.
func (p *plugin) session(r *http.Request) (*session, error) {
	p.mu.Lock()
	s := p.sessions[r.Header.Get("Murder-Session")]
	p.mu.Unlock()
	if s == nil {
		return nil, errors.New("no such session: the plugin was restarted, or the request is not metered")
	}
	s.used.Store(time.Now().UnixNano())
	s.mu.Lock()
	return s, nil
}

func (p *plugin) forget(r *http.Request) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.sessions, r.Header.Get("Murder-Session"))
}

type metered struct {
	Counts []count `json:"counts,omitempty"`
	Body   bool    `json:"body,omitempty"`
	Error  string  `json:"error,omitempty"`
}

func (p *plugin) response(w http.ResponseWriter, r *http.Request) {
	var response Response
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxCall)).Decode(&response); err != nil {
		answer(w, metered{Error: "the edge's response is not JSON: " + err.Error()})
		return
	}
	s, err := p.session(r)
	if err != nil {
		answer(w, metered{Error: err.Error()})
		return
	}
	defer s.mu.Unlock()
	err = func() (err error) {
		defer guard(&err)
		s.reading = s.meter.Response(&response, &s.counter)
		return nil
	}()
	if err != nil {
		answer(w, metered{Error: err.Error()})
		return
	}
	answer(w, metered{Counts: s.counter.take(), Body: s.reading})
}

type chunked struct {
	More  bool   `json:"more"`
	Error string `json:"error,omitempty"`
}

func (p *plugin) body(w http.ResponseWriter, r *http.Request) {
	chunk, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxCall))
	if err != nil {
		answer(w, chunked{Error: "the edge's chunk: " + err.Error()})
		return
	}
	s, err := p.session(r)
	if err != nil {
		answer(w, chunked{Error: err.Error()})
		return
	}
	defer s.mu.Unlock()
	if !s.reading {
		answer(w, chunked{})
		return
	}
	var more bool
	err = func() (err error) {
		defer guard(&err)
		more = s.meter.Body(chunk, &s.counter)
		return nil
	}()
	if err != nil {
		answer(w, chunked{Error: err.Error()})
		return
	}
	answer(w, chunked{More: more})
}

func (p *plugin) end(w http.ResponseWriter, r *http.Request) {
	var end struct {
		Complete bool `json:"complete"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxCall)).Decode(&end); err != nil {
		answer(w, metered{Error: "the edge's end is not JSON: " + err.Error()})
		return
	}
	s, err := p.session(r)
	if err != nil {
		answer(w, metered{Error: err.Error()})
		return
	}
	defer s.mu.Unlock()
	defer p.forget(r)
	err = nil
	if s.reading {
		err = func() (err error) {
			defer guard(&err)
			s.meter.End(end.Complete, &s.counter)
			return nil
		}()
	}
	if err != nil {
		answer(w, metered{Error: err.Error()})
		return
	}
	answer(w, metered{Counts: s.counter.take()})
}
