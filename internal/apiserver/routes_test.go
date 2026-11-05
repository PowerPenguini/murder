package apiserver

import (
	"context"
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/store"
)

func TestRoutesWaitForAChange(t *testing.T) {
	f := newAPIFixture(t)
	ctx := context.Background()
	app := appSpec()
	app.Image = "sha256:" + strings.Repeat("a", 64)
	if _, err := f.store.PutApp(store.App{Name: "web", Spec: app}, "test"); err != nil {
		t.Fatal(err)
	}
	first, err := f.client.Routes(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Routes) != 0 || first.Version == "" {
		t.Fatalf("routes = %+v", first)
	}

	// A request naming the current version is answered once the routes
	// change.
	answered := make(chan error, 1)
	var next = first
	go func() {
		var err error
		next, err = f.client.Routes(ctx, first.Version)
		answered <- err
	}()
	select {
	case err := <-answered:
		t.Fatalf("answered before a change: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	if _, err := f.store.Create(store.NewTenant{Name: "acme", App: "web"}, "test"); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-answered:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no answer after a change")
	}
	if next.Version == first.Version || next.Routes["acme"].Backend != "10.240.0.2:8080" {
		t.Fatalf("routes after a change = %+v", next)
	}

	// A change that leaves the routes as they are is not one; the same
	// routes come back after a while.
	routesWait = 200 * time.Millisecond
	defer func() { routesWait = 25 * time.Second }()
	if _, err := f.store.Rotate("acme", "SESSION_SECRET", "test"); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	same, err := f.client.Routes(ctx, next.Version)
	if err != nil || same.Version != next.Version || time.Since(start) < 150*time.Millisecond {
		t.Fatalf("after %v: %+v, %v", time.Since(start), same, err)
	}
	// Archived, the tenant is no longer routed.
	if err := f.store.Archive("acme", "test"); err != nil {
		t.Fatal(err)
	}
	gone, err := f.client.Routes(ctx, next.Version)
	if err != nil || len(gone.Routes) != 0 {
		t.Fatalf("after archiving: %+v, %v", gone, err)
	}
}

func TestAStoppingAgentAnswersWaitingEdges(t *testing.T) {
	f := newAPIFixture(t)
	server := NewServer(f.store, f.reconciler, nil, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	httpServer := httptest.NewServer(server.Handler())
	defer httpServer.Close()
	client := api.NewClientURL(httpServer.URL)
	first, err := client.Routes(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	answered := make(chan error, 1)
	go func() {
		_, err := client.Routes(context.Background(), first.Version)
		answered <- err
	}()
	time.Sleep(100 * time.Millisecond)
	server.Stop()
	select {
	case err := <-answered:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a waiting edge held the stopping agent")
	}
}

// A tenant whose container is being replaced is routed as moving: the edge
// holds its requests until the new container answers.
func TestARouteWaitsWhileItsContainerIsReplaced(t *testing.T) {
	f := newAPIFixture(t)
	ctx := context.Background()
	if _, err := f.client.PutApp(ctx, "web", appSpec()); err != nil {
		t.Fatal(err)
	}
	if _, err := f.client.Create(ctx, tenantOf("acme")); err != nil {
		t.Fatal(err)
	}
	replacing := false
	routes := Routes{Store: f.store, Replacing: func(tenant string) bool { return tenant == "acme" && replacing }}
	if route, ok := routes.Route("acme"); !ok || route.Moving || route.Backend == "" {
		t.Fatalf("route = %+v, %v", route, ok)
	}
	before := routes.Table().Version
	replacing = true
	if route, ok := routes.Route("acme"); !ok || !route.Moving || route.Backend != "" {
		t.Fatalf("route while replaced = %+v, %v", route, ok)
	}
	if routes.Table().Version == before {
		t.Fatal("the table's version did not change")
	}
}

// A node of a cluster names no routes before it has caught up with the
// cluster since it started: its state may be from before it went down.
func TestRoutesWaitUntilTheNodeCaughtUp(t *testing.T) {
	f := newAPIFixture(t)
	if err := f.store.PutNode(store.Node{Name: f.store.Node(), Address: "10.0.0.1:7400"}, "test"); err != nil {
		t.Fatal(err)
	}
	if err := f.store.NameCluster("TESTCLUSTER", "test"); err != nil {
		t.Fatal(err)
	}
	f.server.WithCluster(catchUpCluster{})
	routesWait = 200 * time.Millisecond
	defer func() { routesWait = 25 * time.Second }()
	start := time.Now()
	if _, err := f.client.Routes(context.Background(), ""); err == nil || !strings.Contains(err.Error(), "not caught up") || time.Since(start) < 150*time.Millisecond {
		t.Fatalf("routes of a node that has not caught up, after %v: %v", time.Since(start), err)
	}
	// One waiting is answered as soon as the node has caught up.
	routesWait = 5 * time.Second
	answered := make(chan error, 1)
	go func() {
		_, err := f.client.Routes(context.Background(), "")
		answered <- err
	}()
	select {
	case err := <-answered:
		t.Fatalf("answered before the node caught up: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	f.server.CaughtUp()
	select {
	case err := <-answered:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("no answer once the node caught up")
	}
}
