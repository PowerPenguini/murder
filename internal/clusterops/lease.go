package clusterops

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/store"
)

// LeaseClient is what holding the cluster's operation lease asks of the
// agent.
type LeaseClient interface {
	TakeLease(ctx context.Context, kind, detail string) (store.Lease, error)
	RenewLease(ctx context.Context, id string) (store.Lease, error)
	ReleaseLease(ctx context.Context, id string) error
}

// renewEvery is how often a holder renews its lease: a few renewals fit
// in store.LeaseTTL, so one or two that fail (a leader changing, a node
// rebooting) do not lose it. A variable for tests.
var renewEvery = store.LeaseTTL / 4

// releaseTimeout bounds the release at the end of an operation; one that
// fails leaves the lease to expire.
const releaseTimeout = 30 * time.Second

// Leased runs work while an operation of kind on detail holds the
// cluster's lease, which it takes first and releases once work returns,
// however it ends. The context work gets acts under the lease, so the
// agent takes its moves, removals and reboots while it refuses others',
// and is cancelled if the lease is lost (broken by a person, or expired
// and taken by another): work then stops at its next request, and the
// error says why. Work run with ctx already under a lease runs under that
// one: a replace is a drain and a removal, under one lease.
func Leased(ctx context.Context, client LeaseClient, kind, detail string, out io.Writer, work func(ctx context.Context) error) error {
	if api.LeaseOf(ctx) != "" {
		return work(ctx)
	}
	lease, err := client.TakeLease(ctx, kind, detail)
	if err != nil {
		if status(err) == http.StatusConflict {
			return fmt.Errorf("%w\nnothing was changed", err)
		}
		return fmt.Errorf("take the cluster's operation lease: %w", err)
	}
	held, lose := context.WithCancelCause(api.WithLease(ctx, lease.ID))
	renewed := make(chan struct{})
	stop := make(chan struct{})
	go func() {
		defer close(renewed)
		renew(held, client, lease.ID, stop, lose)
	}()
	err = work(held)
	close(stop)
	<-renewed
	lost := context.Cause(held)
	lose(nil)
	if lost != nil && !errors.Is(lost, context.Canceled) && !errors.Is(lost, context.DeadlineExceeded) {
		if err == nil {
			err = lost
		} else {
			err = fmt.Errorf("%w\n%w", lost, err)
		}
	}
	release, cancel := context.WithTimeout(context.WithoutCancel(ctx), releaseTimeout)
	defer cancel()
	if releaseErr := client.ReleaseLease(release, lease.ID); releaseErr != nil {
		fmt.Fprintf(out, "the cluster's operation lease %s was not released (%v): it expires within %v\n", lease.ID, releaseErr, store.LeaseTTL)
	}
	return err
}

// renew keeps the lease id until stop closes. A renewal the cluster could
// not take is tried again at the next; one refused as gone, because the
// lease is no longer id, loses it, cancelling ctx with why.
func renew(ctx context.Context, client LeaseClient, id string, stop <-chan struct{}, lose context.CancelCauseFunc) {
	ticker := time.NewTicker(renewEvery)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		ask, cancel := context.WithTimeout(ctx, renewEvery)
		_, err := client.RenewLease(ask, id)
		cancel()
		if err != nil && status(err) == http.StatusGone {
			lose(fmt.Errorf("this operation lost the cluster's operation lease, so it stopped where it was: %w", err))
			return
		}
	}
}
