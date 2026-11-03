package edge

import (
	"io"
	"net/http"
	"sync"
	"time"
)

// UploadIdle is how long a client may send nothing of a request's body
// before the edge gives up on it. An upload that keeps moving, however
// slowly, is never cut off; nor is a response, such as a stream.
const UploadIdle = 30 * time.Second

// UploadDeadline gives up on a request whose client sent its headers but
// stopped sending its body for idle: it would hold the request, and what
// it buffered, for as long as it liked. Requests that switch protocols
// (WebSockets) have no body to wait for; their connections are the
// proxy's.
func UploadDeadline(next http.Handler, idle time.Duration) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Body == nil || r.Body == http.NoBody || r.ContentLength == 0 || r.Header.Get("Upgrade") != "" || r.Method == http.MethodConnect {
			next.ServeHTTP(w, r)
			return
		}
		body := &idleBody{ReadCloser: r.Body, controller: http.NewResponseController(w), idle: idle}
		if !body.extend() {
			next.ServeHTTP(w, r) // a writer that cannot tell
			return
		}
		r.Body = body
		defer body.finish()
		next.ServeHTTP(w, r)
	})
}

// idleBody pushes the connection's read deadline on each time some of the
// body arrives, and lifts it once the body is read: the server then reads
// the connection for the next request, or to notice the client leave.
type idleBody struct {
	io.ReadCloser
	controller *http.ResponseController
	idle       time.Duration
	mu         sync.Mutex
	// done is set once the handler returned: the connection may serve
	// another request by then, whose deadlines are not the body's.
	done bool
}

func (b *idleBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	switch {
	case err == io.EOF:
		b.lift()
	case err != nil:
		// The client stopped sending, or left: what is left of the body
		// is not waited for either.
	case n > 0:
		b.extend()
	}
	return n, err
}

// extend gives the client idle more to send the rest; false if the
// connection's deadlines cannot be set.
func (b *idleBody) extend() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.done || b.controller.SetReadDeadline(time.Now().Add(b.idle)) == nil
}

func (b *idleBody) lift() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.done {
		_ = b.controller.SetReadDeadline(time.Time{})
	}
}

// finish leaves the deadline as it is: a body not read yet is still read,
// up to a limit, before the next request, and must not hang there.
func (b *idleBody) finish() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.done = true
}
