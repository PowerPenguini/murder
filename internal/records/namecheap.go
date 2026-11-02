package records

import (
	"context"
	"net/netip"
	"slices"
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
	Zone, Name   string
	TTL          time.Duration
	// api is the libdns provider; Namecheap's if nil.
	api namecheapAPI
}

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
	api := n.api
	if api == nil {
		api = &namecheap.Provider{User: n.User, APIKey: n.APIKey}
	}
	// Unchanged records are not written: a write sets the whole host list.
	existing, err := api.GetRecords(ctx, n.Zone)
	if err != nil {
		return err
	}
	if sameAddresses(existing, records, n.Name) {
		return nil
	}
	_, err = api.SetRecords(ctx, n.Zone, records)
	return err
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
