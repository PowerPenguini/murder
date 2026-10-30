package api

import (
	"context"
	"strings"
	"testing"
	"time"

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
