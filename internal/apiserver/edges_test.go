package apiserver

import (
	"testing"
	"time"

	"github.com/PowerPenguini/murder/internal/edge"
)

// A tenant's requests are held once every edge serving says it serves from
// a table that holds them; an edge on an older table, or none at all, is no
// hold.
func TestEdgesHoldOnceEveryEdgeServesTheHold(t *testing.T) {
	var e edges
	now := time.Now()
	before := edge.NewTable(map[string]edge.Route{"acme": {Backend: "10.0.0.2:8080"}})
	after := edge.NewTable(map[string]edge.Route{"acme": {Moving: true}})
	e.sent(before)
	e.sent(after)
	if held, _ := e.hold("acme", now); held {
		t.Fatal("held with no edge serving")
	}
	e.says("one", before.Version, now)
	if held, _ := e.hold("acme", now); held {
		t.Fatal("held while the edge serves the table before")
	}
	e.says("one", after.Version, now)
	e.says("two", before.Version, now)
	if held, _ := e.hold("acme", now); held {
		t.Fatal("held while a second edge serves the table before")
	}
	e.says("two", after.Version, now)
	if held, _ := e.hold("acme", now); !held {
		t.Fatal("not held once both serve the hold")
	}
	// An edge that stopped asking stops counting.
	e.says("three", before.Version, now)
	if held, _ := e.hold("acme", now.Add(edgeLive()+time.Second)); held {
		t.Fatal("held with every edge gone quiet")
	}
}
