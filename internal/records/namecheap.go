package records

import (
	"context"
	"net/netip"
	"time"

	"github.com/libdns/libdns"
	"github.com/libdns/namecheap"
)

// Namecheap writes the records through Namecheap's API, which sets a
// domain's whole host list in one call: the other hosts stay as they are.
// Namecheap answers only addresses it allows, so every node's public
// address must be on the account's API allow list.
type Namecheap struct {
	User, APIKey string
	Zone, Name   string
	TTL          time.Duration
	// setter is the libdns provider; Namecheap's if nil.
	setter libdns.RecordSetter
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
	setter := n.setter
	if setter == nil {
		setter = &namecheap.Provider{User: n.User, APIKey: n.APIKey}
	}
	_, err := setter.SetRecords(ctx, n.Zone, records)
	return err
}
