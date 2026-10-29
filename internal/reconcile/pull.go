package reconcile

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// errPulling reports an image that is being fetched: the tenant waits, and
// whatever runs keeps running.
var errPulling = errors.New("pulling the image")

// Delays between pull attempts of one image: the first retry after
// pullRetry, doubling up to pullRetryMax.
const (
	pullRetry    = 10 * time.Second
	pullRetryMax = 10 * time.Minute
)

// pulls fetches images in the background, so a slow or failing pull holds
// up neither the other tenants nor the API, and retries a failed pull with
// growing delays instead of on every pass.
type pulls struct {
	ctx  context.Context // ends with the reconciler
	pull func(ctx context.Context, image string) error
	now  func() time.Time
	// done is signalled when a pull ends, so a pass can use the image.
	done chan struct{}

	mu      sync.Mutex
	images  map[string]*pullState
	started int
	running sync.WaitGroup
}

type pullState struct {
	running  bool
	failures int
	err      error
	retryAt  time.Time
}

func newPulls(ctx context.Context, pull func(context.Context, string) error) *pulls {
	return &pulls{ctx: ctx, pull: pull, now: time.Now, done: make(chan struct{}, 1), images: map[string]*pullState{}}
}

// fetch starts a pull of image unless one runs, and returns errPulling.
// After a failure it returns that failure until the next attempt is due.
func (p *pulls) fetch(image string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	state := p.images[image]
	if state == nil {
		state = &pullState{}
		p.images[image] = state
	}
	if state.running {
		return errPulling
	}
	if state.err != nil && p.now().Before(state.retryAt) {
		return fmt.Errorf("%w (attempt %d; next at %s)", state.err, state.failures, state.retryAt.UTC().Format(time.TimeOnly))
	}
	state.running = true
	p.started++
	p.running.Add(1)
	go func() {
		defer p.running.Done()
		err := p.pull(p.ctx, image)
		p.mu.Lock()
		state.running = false
		if err == nil {
			delete(p.images, image)
		} else {
			state.failures++
			state.err = err
			state.retryAt = p.now().Add(min(pullRetry<<min(state.failures-1, 16), pullRetryMax))
		}
		p.mu.Unlock()
		select {
		case p.done <- struct{}{}:
		default:
		}
	}()
	return errPulling
}

// present forgets the failures of an image that is now present.
func (p *pulls) present(image string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if state := p.images[image]; state != nil && !state.running {
		delete(p.images, image)
	}
}

// retryNow lets every failed image be pulled again at once, say after the
// registry credentials were fixed.
func (p *pulls) retryNow() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, state := range p.images {
		state.retryAt = time.Time{}
	}
}

// keep forgets the failures of images no tenant wants any more.
func (p *pulls) keep(wanted map[string]bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for image, state := range p.images {
		if !wanted[image] && !state.running {
			delete(p.images, image)
		}
	}
}

// startedCount counts the pulls started so far.
func (p *pulls) startedCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.started
}

// pulling reports whether image is being pulled.
func (p *pulls) pulling(image string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	state := p.images[image]
	return state != nil && state.running
}

// waitContext waits for the running pulls, and reports false if ctx ended
// first.
func (p *pulls) waitContext(ctx context.Context) bool {
	done := make(chan struct{})
	go func() {
		p.wait()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-ctx.Done():
		return false
	}
}

// wait returns once no pull runs.
func (p *pulls) wait() { p.running.Wait() }
