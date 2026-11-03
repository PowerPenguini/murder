package releases

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/PowerPenguini/murder/internal/metrics"
	"github.com/PowerPenguini/murder/internal/spec"
	"github.com/PowerPenguini/murder/internal/store"
)

const (
	v1 = "ghcr.io/example/web@sha256:1111111111111111111111111111111111111111111111111111111111111111"
	v2 = "ghcr.io/example/web@sha256:2222222222222222222222222222222222222222222222222222222222222222"
)

type fakeStore struct {
	apps []store.App
	set  []string
	err  error
}

func (f *fakeStore) Apps() []store.App { return f.apps }

func (f *fakeStore) SetLatest(app, tag, latest string, _ int64, actor string) (bool, error) {
	f.set = append(f.set, app+" "+tag+" "+latest+" "+actor)
	return f.err == nil, f.err
}

type fakeRegistry struct {
	mu     sync.Mutex
	images map[string]string
	asked  []string
}

func (f *fakeRegistry) Latest(_ context.Context, tag string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.asked = append(f.asked, tag)
	if image, ok := f.images[tag]; ok {
		return image, nil
	}
	return "", errors.New("denied")
}

func app(name, tag, image string) store.App {
	return store.App{Name: name, Tag: tag, Spec: spec.Tenant{Image: image}}
}

func TestTheLeaderAsksAboutEachTagOnce(t *testing.T) {
	st := &fakeStore{apps: []store.App{
		app("web", "ghcr.io/example/web:stable", v1),
		app("web2", "ghcr.io/example/web:stable", v1),
		app("pinned", "", v1),
		app("gone", "ghcr.io/example/gone:1", v1),
	}}
	registry := &fakeRegistry{images: map[string]string{"ghcr.io/example/web:stable": v2}}
	var logs bytes.Buffer
	leading := false
	w := &Watcher{Store: st, Registry: registry, Leader: func() bool { return leading }, Every: time.Minute, Log: slog.New(slog.NewTextHandler(&logs, nil))}
	w.Check(context.Background())
	if len(registry.asked) != 0 {
		t.Fatalf("a node that does not lead asked %v", registry.asked)
	}
	leading = true
	w.Check(context.Background())
	if strings.Join(registry.asked, ",") != "ghcr.io/example/web:stable,ghcr.io/example/gone:1" {
		t.Fatalf("asked %v", registry.asked)
	}
	want := []string{"web ghcr.io/example/web:stable " + v2 + " murder", "web2 ghcr.io/example/web:stable " + v2 + " murder"}
	if strings.Join(st.set, "|") != strings.Join(want, "|") {
		t.Fatalf("recorded %v", st.set)
	}
	if failing := w.Failing(); len(failing) != 1 || failing["gone"] != "denied" {
		t.Fatalf("failing %v", failing)
	}
	// A failure is logged once, not at every check.
	w.Check(context.Background())
	if n := strings.Count(logs.String(), "ask the registry"); n != 1 {
		t.Fatalf("logged the failure %d times:\n%s", n, logs.String())
	}
	var out bytes.Buffer
	st.apps[0].Latest = v2
	w.WriteMetrics(metrics.NewWriter(&out))
	for _, line := range []string{
		`murder_app_newer_image{app="web"} 1`,
		`murder_app_newer_image{app="web2"} 0`,
		`murder_app_image_check_failing{app="gone"} 1`,
		`murder_app_image_check_failing{app="web"} 0`,
	} {
		if !strings.Contains(out.String(), line+"\n") {
			t.Errorf("metrics lack %s:\n%s", line, out.String())
		}
	}
	if strings.Contains(out.String(), `app="pinned"`) {
		t.Errorf("an app without a tag is reported:\n%s", out.String())
	}
	// A node that no longer leads forgets what failed.
	leading = false
	w.Check(context.Background())
	if len(w.Failing()) != 0 {
		t.Fatal("a node that does not lead reports failures")
	}
}

func TestAFrozenStateIsAskedAgainLater(t *testing.T) {
	st := &fakeStore{apps: []store.App{app("web", "ghcr.io/example/web:stable", v1)}, err: store.ErrFrozen}
	var logs bytes.Buffer
	w := &Watcher{Store: st, Registry: &fakeRegistry{images: map[string]string{"ghcr.io/example/web:stable": v2}}, Every: time.Minute, Log: slog.New(slog.NewTextHandler(&logs, nil))}
	w.Check(context.Background())
	if len(st.set) != 1 || logs.Len() != 0 {
		t.Fatalf("set %v, logs %s", st.set, logs.String())
	}
}

func TestRunChecksUntilItsContextEnds(t *testing.T) {
	registry := &fakeRegistry{images: map[string]string{}}
	w := &Watcher{Store: &fakeStore{apps: []store.App{app("web", "ghcr.io/example/web:stable", v1)}}, Registry: registry, Every: 10 * time.Millisecond, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { w.Run(ctx); close(done) }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		registry.mu.Lock()
		asked := len(registry.asked)
		registry.mu.Unlock()
		if asked >= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no checks ran")
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	<-done
}

func TestDockerHubIsAskedAtMostHourly(t *testing.T) {
	st := &fakeStore{apps: []store.App{app("db", "postgres:17", v1), app("web", "ghcr.io/example/web:stable", v1), app("gone", "example/gone:1", v1)}}
	registry := &fakeRegistry{images: map[string]string{"postgres:17": v2, "ghcr.io/example/web:stable": v2}}
	now := time.Date(2026, 11, 3, 10, 0, 0, 0, time.UTC)
	w := &Watcher{Store: st, Registry: registry, Every: 5 * time.Minute, Now: func() time.Time { return now }, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	w.Check(context.Background())
	now = now.Add(5 * time.Minute)
	w.Check(context.Background())
	if got := strings.Join(registry.asked, ","); got != "postgres:17,ghcr.io/example/web:stable,example/gone:1,ghcr.io/example/web:stable" {
		t.Fatalf("asked %s", got)
	}
	// A Hub tag that failed is still reported failing between questions.
	if w.Failing()["gone"] != "denied" {
		t.Fatalf("failing %v", w.Failing())
	}
	now = now.Add(time.Hour)
	w.Check(context.Background())
	if n := strings.Count(strings.Join(registry.asked, ","), "postgres:17"); n != 2 {
		t.Fatalf("postgres asked %d times in an hour and five minutes", n)
	}
	for tag, hub := range map[string]bool{"postgres": true, "library/postgres:17": true, "docker.io/library/postgres:17": true, "index.docker.io/x/y:1": true, "ghcr.io/x/y:1": false, "localhost:5000/x:1": false, "registry.example.com/x:1": false} {
		if OnDockerHub(tag) != hub {
			t.Errorf("OnDockerHub(%s) = %v", tag, !hub)
		}
	}
}
