package apiserver

import (
	"context"
	"errors"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/cluster"
	"github.com/PowerPenguini/murder/internal/reboot"
	"github.com/PowerPenguini/murder/internal/store"
)

// fakeRebooter takes reboots as reboot.Node does, without rebooting.
type fakeRebooter struct {
	err      error
	requests []reboot.Request
}

func (f *fakeRebooter) Reboot(_ context.Context, request reboot.Request) error {
	if f.err != nil {
		return f.err
	}
	f.requests = append(f.requests, request)
	return nil
}

// rebootCluster passes reboots on to other nodes, as the cluster does.
type rebootCluster struct {
	Cluster
	asked map[string]reboot.Request
	err   error
}

func (c rebootCluster) RebootOn(_ context.Context, node string, request reboot.Request) error {
	if c.err != nil {
		return c.err
	}
	c.asked[node] = request
	return nil
}

func TestANodeRebootsItselfOrAsksAnother(t *testing.T) {
	f := newAPIFixture(t)
	ctx := context.Background()
	// A node that takes no reboots says so.
	var apiErr *api.Error
	if err := f.client.Reboot(ctx, "-", reboot.Request{Check: true}); !errors.As(err, &apiErr) || apiErr.Status != http.StatusConflict {
		t.Fatalf("without a rebooter: %v", err)
	}
	rebooter := &fakeRebooter{}
	f.server.WithRebooter(rebooter)
	if err := f.client.Reboot(ctx, "-", reboot.Request{Check: true}); err != nil {
		t.Fatal(err)
	}
	if err := f.client.Reboot(ctx, "test-node", reboot.Request{Away: time.Minute}); err != nil {
		t.Fatal(err)
	}
	if len(rebooter.requests) != 2 || !rebooter.requests[0].Check || rebooter.requests[1].Away != time.Minute {
		t.Fatalf("requests %+v", rebooter.requests)
	}
	// The reboot is in the audit log; the check is not.
	events, err := f.store.Events(store.EventQuery{})
	rebooting := 0
	for _, event := range events {
		if event.Action == "node.rebooting" && event.Detail == "test-node" {
			rebooting++
		}
	}
	if err != nil || rebooting != 1 {
		t.Fatalf("%d reboots recorded in %+v, %v", rebooting, events, err)
	}
	// One that would not come back refuses (422); one rebooting already is
	// a conflict.
	rebooter.err = &reboot.NotReadyError{Problems: []string{"murder-edge.service is disabled"}}
	if err := f.client.Reboot(ctx, "-", reboot.Request{}); !errors.As(err, &apiErr) || apiErr.Status != http.StatusUnprocessableEntity {
		t.Fatalf("not ready: %v", err)
	}
	rebooter.err = reboot.ErrRebooting
	if err := f.client.Reboot(ctx, "-", reboot.Request{}); !errors.As(err, &apiErr) || apiErr.Status != http.StatusConflict {
		t.Fatalf("rebooting: %v", err)
	}
	// Another node is asked through the cluster; what it refused stays a
	// refusal.
	asked := map[string]reboot.Request{}
	f.server.WithCluster(rebootCluster{asked: asked})
	if err := f.client.Reboot(ctx, "n2", reboot.Request{Away: time.Hour}); err != nil || asked["n2"].Away != time.Hour {
		t.Fatalf("n2: %v, asked %+v", err, asked)
	}
	f.server.WithCluster(rebootCluster{asked: asked, err: &cluster.PeerError{Status: http.StatusUnprocessableEntity, Message: "docker.service is disabled"}})
	if err := f.client.Reboot(ctx, "n2", reboot.Request{}); !errors.As(err, &apiErr) || apiErr.Status != http.StatusUnprocessableEntity {
		t.Fatalf("n2 not ready: %v", err)
	}
}

func TestOnlyRootReboots(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("runs as root")
	}
	defer func(uid uint32) { agentUID = uid }(agentUID)
	agentUID++
	f := newAPIFixture(t)
	rebooter := &fakeRebooter{}
	f.server.WithRebooter(rebooter)
	socket := filepath.Join(t.TempDir(), "agent.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: f.handler, ConnContext: ConnContext}
	go server.Serve(listener)
	t.Cleanup(func() { _ = server.Close() })
	var apiErr *api.Error
	if err := api.NewClient(socket).Reboot(context.Background(), "-", reboot.Request{}); !errors.As(err, &apiErr) || apiErr.Status != http.StatusForbidden {
		t.Fatalf("by a non-root user: %v", err)
	}
	if len(rebooter.requests) != 0 {
		t.Fatal("rebooted for a non-root user")
	}
}
