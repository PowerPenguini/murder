package panel

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// A panel acting on the demo cluster's nodes, as a browser on its pages.
func newClusterPanel(t *testing.T) (*DemoCluster, *Server, *httptest.Server, *http.Client) {
	t.Helper()
	cluster := NewDemoCluster()
	cluster.Pace = 0
	s, err := New(cluster, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	s.Actions, s.Cluster = cluster.As, cluster.AsCluster
	server := httptest.NewServer(s.Handler())
	t.Cleanup(server.Close)
	jar, _ := cookiejar.New(nil)
	return cluster, s, server, &http.Client{Jar: jar}
}

// waitForJob follows the page a job's form sent to until the job ends,
// and returns that page.
func waitForJob(t *testing.T, s *Server, client *http.Client, response *http.Response) string {
	t.Helper()
	if !strings.HasPrefix(response.Request.URL.Path, "/jobs/") {
		t.Fatalf("not sent to a job, but to %s", response.Request.URL)
	}
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		if !s.Working() {
			break
		}
	}
	page, err := client.Get(response.Request.URL.String())
	if err != nil {
		t.Fatal(err)
	}
	defer page.Body.Close()
	body, _ := io.ReadAll(page.Body)
	return string(body)
}

func TestActingOnTheCluster(t *testing.T) {
	cluster, s, server, client := newClusterPanel(t)
	ctx := context.Background()
	_, page := get(t, server.URL+"/cluster")
	for _, want := range []string{"fsn-1", "nbg-1", "hel-1", "schema 13", `placeholder="hel-1"`, "none yet"} {
		if !strings.Contains(page, want) {
			t.Fatalf("the cluster page lacks %q:\n%s", want, page)
		}
	}
	// hel-1 failed over already, and this node is not removed from itself.
	if strings.Contains(page, "it is off") || strings.Contains(page, `placeholder="fsn-1"`) {
		t.Fatalf("the cluster page offers what does not apply:\n%s", page)
	}

	// Removing a node takes its name typed, and what still runs there
	// keeps it.
	_, body := submit(t, client, server, "/cluster", url.Values{"do": {"remove"}, "node": {"hel-1"}, "confirm": {"hel"}}, server.URL)
	if !strings.Contains(body, "type the node&#39;s name") {
		t.Fatalf("removed without the name typed:\n%s", body)
	}
	_, body = submit(t, client, server, "/cluster", url.Values{"do": {"remove"}, "node": {"nbg-1"}, "confirm": {"nbg-1"}}, server.URL)
	if !strings.Contains(body, "drain it first") {
		t.Fatalf("removed a node that runs tenants:\n%s", body)
	}
	_, body = submit(t, client, server, "/cluster", url.Values{"do": {"remove"}, "node": {"hel-1"}, "confirm": {"hel-1"}}, server.URL)
	if !strings.Contains(body, "node hel-1 left the cluster") {
		t.Fatalf("after removing hel-1:\n%s", body)
	}
	if nodes, _ := cluster.Nodes(ctx); len(nodes.Nodes) != 2 {
		t.Fatalf("nodes after the removal: %+v", nodes.Nodes)
	}

	// A drain is a job: its page tells each move.
	response, _ := submit(t, client, server, "/cluster", url.Values{"do": {"drain"}, "node": {"nbg-1"}, "to": {"fsn-1"}}, server.URL)
	body = waitForJob(t, s, client, response)
	if !strings.Contains(body, "drain nbg-1 to fsn-1") || !strings.Contains(body, ">done<") || !strings.Contains(body, "moved") {
		t.Fatalf("the drain's page:\n%s", body)
	}
	tenants, _ := cluster.List(ctx)
	for _, tenant := range tenants {
		if tenant.Node == "nbg-1" {
			t.Fatalf("%s still runs on nbg-1 after the drain", tenant.Spec.Name)
		}
	}
	if _, page = get(t, server.URL+"/cluster"); !strings.Contains(page, "drain nbg-1 to fsn-1") {
		t.Fatalf("the cluster page does not list the drain:\n%s", page)
	}

	// Backing up is a job too, listing each tenant's backup.
	response, _ = submit(t, client, server, "/cluster", url.Values{"do": {"backups"}}, server.URL)
	if body = waitForJob(t, s, client, response); !strings.Contains(body, "back up the tenants of fsn-1") || !strings.Contains(body, ".db.zst") {
		t.Fatalf("the backups' page:\n%s", body)
	}

	// Taking changes again takes "thaw" typed.
	agent := cluster.AsCluster("test")
	_ = agent.Freeze(ctx, "an upgrade stopped")
	if _, page = get(t, server.URL+"/cluster"); !strings.Contains(page, "an upgrade stopped") {
		t.Fatalf("the cluster page does not say changes are frozen:\n%s", page)
	}
	_, body = submit(t, client, server, "/cluster", url.Values{"do": {"thaw"}, "confirm": {"yes"}}, server.URL)
	if nodes, _ := cluster.Nodes(ctx); nodes.Frozen == "" || !strings.Contains(body, "type thaw") {
		t.Fatalf("thawed without thaw typed:\n%s", body)
	}
	_, body = submit(t, client, server, "/cluster", url.Values{"do": {"thaw"}, "confirm": {"thaw"}}, server.URL)
	if nodes, _ := cluster.Nodes(ctx); nodes.Frozen != "" || !strings.Contains(body, "changes are taken again") {
		t.Fatalf("after thawing:\n%s", body)
	}
}

// uploadBinary submits the upgrade form with the file at path.
func uploadBinary(t *testing.T, client *http.Client, server *httptest.Server, path, confirm string) *http.Response {
	t.Helper()
	var body bytes.Buffer
	form := multipart.NewWriter(&body)
	_ = form.WriteField("do", "upgrade")
	_ = form.WriteField("confirm", confirm)
	part, _ := form.CreateFormFile("binary", "murder")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = part.Write(data)
	_ = form.Close()
	request, _ := http.NewRequest(http.MethodPost, server.URL+"/cluster", &body)
	request.Header.Set("Content-Type", form.FormDataContentType())
	request.Header.Set("Origin", server.URL)
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { response.Body.Close() })
	return response
}

func TestUpgradingFromThePanel(t *testing.T) {
	cluster, s, server, client := newClusterPanel(t)
	ctx := context.Background()
	dir := t.TempDir()
	notBinary := filepath.Join(dir, "notes.txt")
	_ = os.WriteFile(notBinary, []byte("hello"), 0o600)
	// A stand-in for a murder binary: it says its version as one does.
	binary := filepath.Join(dir, "murder")
	_ = os.WriteFile(binary, []byte("#!/bin/sh\necho '{\"binary\":\"self\",\"schema\":13}'\n"), 0o700)

	read := func(response *http.Response) string {
		data, _ := io.ReadAll(response.Body)
		return string(data)
	}
	if body := read(uploadBinary(t, client, server, binary, "yes")); !strings.Contains(body, "type upgrade") {
		t.Fatalf("upgraded without upgrade typed:\n%s", body)
	}
	if body := read(uploadBinary(t, client, server, notBinary, "upgrade")); !strings.Contains(body, "not a murder binary") {
		t.Fatalf("took a file that is no murder binary:\n%s", body)
	}
	if s.Working() {
		t.Fatal("a refused upgrade left a job running")
	}

	// A node that does not answer stops it before anything changes.
	body := waitForJob(t, s, client, uploadBinary(t, client, server, binary, "upgrade"))
	if !strings.Contains(body, "hel-1 does not answer, so nothing was changed") || !strings.Contains(body, ">failed<") {
		t.Fatalf("the upgrade's page with hel-1 down:\n%s", body)
	}
	if nodes, _ := cluster.Nodes(ctx); nodes.Frozen != "" {
		t.Fatalf("a refused upgrade left changes frozen: %s", nodes.Frozen)
	}

	// With hel-1 gone, every node takes it, this one last; the uploaded
	// file goes once the job ends.
	if err := cluster.AsCluster("test").RemoveNode(ctx, "hel-1"); err != nil {
		t.Fatal(err)
	}
	body = waitForJob(t, s, client, uploadBinary(t, client, server, binary, "upgrade"))
	if !strings.Contains(body, "every node runs the new binary") || !strings.Contains(body, ">done<") {
		t.Fatalf("the upgrade's page:\n%s", body)
	}
	if strings.Index(body, "nbg-1: installed") > strings.Index(body, "fsn-1: installed") {
		t.Fatalf("this node was not upgraded last:\n%s", body)
	}
	if nodes, _ := cluster.Nodes(ctx); nodes.Frozen != "" {
		t.Fatalf("changes stay frozen after the upgrade: %s", nodes.Frozen)
	}
	left, _ := filepath.Glob(filepath.Join(os.TempDir(), "murder-upgrade-*"))
	for _, path := range left {
		if info, err := os.Stat(path); err == nil && time.Since(info.ModTime()) < time.Minute {
			t.Errorf("the uploaded binary %s stayed", path)
		}
	}
}

func TestAJobOnlyItsPanelKnows(t *testing.T) {
	_, _, server, _ := newClusterPanel(t)
	response, body := get(t, server.URL+"/jobs/0123456789abcdef")
	if response.StatusCode != http.StatusNotFound || !strings.Contains(body, "no such job") {
		t.Fatalf("an unknown job: %d\n%s", response.StatusCode, body)
	}
}

// A new release's static files are fetched anew: their addresses change.
func TestStaticFilesAreVersioned(t *testing.T) {
	_, _, server, _ := newClusterPanel(t)
	_, page := get(t, server.URL+"/")
	link := "/static/panel.css?v=" + staticVersion
	if len(staticVersion) != 12 || !strings.Contains(page, link) {
		t.Fatalf("the page does not ask for %s:\n%s", link, page)
	}
	response, _ := get(t, server.URL+link)
	if response.StatusCode != http.StatusOK || response.Header.Get("Cache-Control") != "public, max-age=86400" {
		t.Fatalf("%s: %d, Cache-Control %q", link, response.StatusCode, response.Header.Get("Cache-Control"))
	}
}
