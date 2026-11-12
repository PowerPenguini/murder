package panel

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/netip"
	"testing"
	"time"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/store"
)

// claimer is a cluster's state of nodes' tunnel ends, refusing an address
// another node has, as the store does.
type claimer struct {
	view api.Nodes
	// takenMeanwhile is an address another node takes just before this
	// one asks for it.
	takenMeanwhile netip.Addr
	set            []netip.Addr
}

func (c *claimer) Nodes(context.Context) (api.Nodes, error) { return c.view, nil }

func (c *claimer) SetTunnel(_ context.Context, end store.Tunnel) error {
	c.set = append(c.set, end.Address)
	if end.Address == c.takenMeanwhile {
		c.view.Nodes = append(c.view.Nodes, api.NodeState{Node: store.Node{Name: "n9", Tunnel: &store.Tunnel{Address: end.Address}}})
		c.takenMeanwhile = netip.Addr{}
		return fmt.Errorf("node n9 has the tunnel address %s already", end.Address)
	}
	return nil
}

func TestANodeTakesTheLowestFreeAddressInTheTunnel(t *testing.T) {
	network := netip.MustParsePrefix("10.77.0.0/24")
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	c := &claimer{view: api.Nodes{Self: "n2", Nodes: []api.NodeState{
		{Node: store.Node{Name: "n1", Tunnel: &store.Tunnel{Address: netip.MustParseAddr("10.77.0.1")}}},
		{Node: store.Node{Name: "n2"}},
	}}, takenMeanwhile: netip.MustParseAddr("10.77.0.2")}
	address, err := ClaimTunnelAddress(context.Background(), c, network, store.Tunnel{Key: "k", Port: 51820}, time.Millisecond, log)
	if err != nil || address != netip.MustParseAddr("10.77.0.3") || len(c.set) != 2 {
		t.Fatalf("address %s, %v, tried %v", address, err, c.set)
	}
	// The address the state has for this node stays its own.
	c.view.Nodes[1].Tunnel = &store.Tunnel{Address: netip.MustParseAddr("10.77.0.7")}
	c.set = nil
	if address, err := ClaimTunnelAddress(context.Background(), c, network, store.Tunnel{Key: "k", Port: 51820}, time.Millisecond, log); err != nil || address != netip.MustParseAddr("10.77.0.7") || c.set != nil {
		t.Fatalf("address %s, %v, set %v", address, err, c.set)
	}
}
