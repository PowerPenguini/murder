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
// it buffered, for as long as it liked. Every request with a body is
// watched, whatever its headers say: an Upgrade header alone switches
// nothing. A protocol switch (a WebSocket) has no body to wait for, and
// the server lifts every deadline from a connection taken over for one.
//
// The time counts from when the body is first read, not from when the
// request came: a request may wait for its tenant to arrive first, and a
// client that asked to be told to go on (Expect: 100-continue) sends
// nothing until then.
func UploadDeadline(next http.Handler, idle time.Duration) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Body == nil || r.Body == http.NoBody || r.ContentLength == 0 || r.Method == http.MethodConnect {
			next.ServeHTTP(w, r)
			return
		}
		body := &idleBody{ReadCloser: r.Body, controller: http.NewResponseController(w), idle: idle}
		r.Body = body
		defer body.finish()
		next.ServeHTTP(w, r)
	})
}

// idleBody pushes the connection's read deadline on each time some of the
// body arrives, starting with the first read, and lifts it once the body
// is read: the server then reads the connection for the next request, or
// to notice the client leave.
type idleBody struct {
	io.ReadCloser
	controller *http.ResponseController
	idle       time.Duration
	mu         sync.Mutex
	// done is set once the handler returned: the connection may serve
	// another request by then, whose deadlines are not the body's.
	done bool
	// lifted is set once the body was read whole.
	lifted bool
}

func (b *idleBody) Read(p []byte) (int, error) {
	// The client has until idle from now to send something.
	b.extend()
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

// extend gives the client idle more to send the rest. A writer that cannot
// set deadlines leaves the body unwatched.
func (b *idleBody) extend() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.done && !b.lifted {
		_ = b.controller.SetReadDeadline(time.Now().Add(b.idle))
	}
}

func (b *idleBody) lift() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.done {
		b.lifted = true
		_ = b.controller.SetReadDeadline(time.Time{})
	}
}

// finish gives what is left of a body not read whole idle to come: the
// server still reads it, up to a limit, before the next request, and must
// not hang there. The deadline is left as it is after that.
func (b *idleBody) finish() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.done && !b.lifted {
		_ = b.controller.SetReadDeadline(time.Now().Add(b.idle))
	}
	b.done = true
}
