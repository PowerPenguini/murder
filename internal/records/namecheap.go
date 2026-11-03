package records

import (
	"context"
	"errors"
	"net/netip"
	"slices"
	"sync/atomic"
	"time"

	"github.com/libdns/libdns"
	"github.com/libdns/namecheap"
)

// Namecheap writes the records through Namecheap's API, which sets a
// domain's whole host list in one call: the other hosts stay as they are,
// as libdns reads them back. The records are written only when they
// differ. Namecheap answers only addresses it allows, so every node's
// public address must be on the account's API allow list.
type Namecheap struct {
	User, APIKey string
	// ClientIP is this node's public IPv4 address, which Namecheap must
	// allow; without it the provider asks a web service for it, with no
	// time limit.
	ClientIP   string
	Zone, Name string
	TTL        time.Duration
	// api is the libdns provider; Namecheap's if nil.
	api namecheapAPI
	// busy is set while a call of the provider runs, which a context
	// cannot stop: one more waits for none to end, it is refused.
	busy atomic.Bool
}

// MaxNamecheapTTL is the longest TTL Namecheap takes.
const MaxNamecheapTTL = 60000 * time.Second

type namecheapAPI interface {
	libdns.RecordGetter
	libdns.RecordSetter
}

func (n *Namecheap) Publish(ctx context.Context, addresses []netip.Addr) error {
	ttl := n.TTL
	if ttl == 0 {
		ttl = TTL
	}
	v4, v6 := families(addresses)
	var records []libdns.Record
	for _, family := range [][]string{v4, v6} {
		for _, value := range family {
			records = append(records, libdns.Address{Name: n.Name, TTL: ttl, IP: netip.MustParseAddr(value)})
		}
	}
	if len(records) == 0 {
		return nil
	}
	if n.api == nil {
		n.api = &namecheap.Provider{User: n.User, APIKey: n.APIKey, ClientIP: n.ClientIP}
	}
	// Unchanged records are not written: a write sets the whole host list.
	var existing []libdns.Record
	if err := n.withContext(ctx, func() (err error) {
		existing, err = n.api.GetRecords(ctx, n.Zone)
		return err
	}); err != nil {
		return err
	}
	if sameAddresses(existing, records, n.Name) {
		return nil
	}
	return n.withContext(ctx, func() error {
		_, err := n.api.SetRecords(ctx, n.Zone, records)
		return err
	})
}

// errBusy refuses a call while one the provider cannot stop still runs.
var errBusy = errors.New("an earlier call to Namecheap has not ended")

// withContext runs call, returning when ctx ends if call has not: the
// provider makes some requests without a context. A call still running
// from before refuses the next, so they do not pile up.
func (n *Namecheap) withContext(ctx context.Context, call func() error) error {
	if !n.busy.CompareAndSwap(false, true) {
		return errBusy
	}
	done := make(chan error, 1)
	go func() {
		err := call()
		// Cleared before the result is sent: the caller's next call, as
		// SetRecords right after GetRecords, finds the provider free.
		n.busy.Store(false)
		done <- err
	}()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// sameAddresses reports whether the zone's A and AAAA records of name, of
// the families in want, are want, TTLs included.
func sameAddresses(zone, want []libdns.Record, name string) bool {
	key := func(rr libdns.RR) string { return rr.Type + " " + rr.Data + " " + rr.TTL.String() }
	types := map[string]bool{}
	var wanted []string
	for _, record := range want {
		rr := record.RR()
		types[rr.Type] = true
		wanted = append(wanted, key(rr))
	}
	var have []string
	for _, record := range zone {
		rr := record.RR()
		if rr.Name == name && types[rr.Type] {
			have = append(have, key(rr))
		}
	}
	slices.Sort(wanted)
	slices.Sort(have)
	return slices.Equal(have, wanted)
}
