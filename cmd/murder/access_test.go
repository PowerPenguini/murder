package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/PowerPenguini/murder/deploy"
	"github.com/PowerPenguini/murder/internal/access"
	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/apiserver"
	"github.com/PowerPenguini/murder/internal/secrets"
	"github.com/PowerPenguini/murder/internal/spec"
	"github.com/PowerPenguini/murder/internal/store"
)

// registerAgent is an agent with tenant acme, on a unix socket that names
// the user at its other end, as a node's does.
func registerAgent(t *testing.T) (string, *store.SQLite) {
	t.Helper()
	// Neither run by systemd, nor through sudo, but where a test says.
	t.Setenv("INVOCATION_ID", "")
	t.Setenv("SUDO_USER", "")
	dir := t.TempDir()
	keyPath := filepath.Join(dir, "master.key")
	if err := secrets.CreateKey(keyPath); err != nil {
		t.Fatal(err)
	}
	key, _ := secrets.LoadKey(keyPath)
	database := filepath.Join(dir, "murder.db")
	if err := store.Init(database, "node-1"); err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(database, store.Options{NetworkPool: netip.MustParsePrefix("10.240.0.0/16"), Sealer: key.NewSealed})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	app := spec.Tenant{Image: "sha256:" + strings.Repeat("a", 64), HealthPath: "/health", User: "1000:1000", Secrets: spec.Secrets{"SESSION": {}}}
	if _, err := st.PutApp(store.App{Name: "web", Spec: app}, "test"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Create(store.NewTenant{Name: "acme", App: "web"}, "test"); err != nil {
		t.Fatal(err)
	}
	socket := filepath.Join(socketDir(t), "agent.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: apiserver.NewServer(st, nil, nil, key, slog.New(slog.NewTextHandler(io.Discard, nil))).Handler(), ConnContext: apiserver.ConnContext}
	go server.Serve(listener)
	t.Cleanup(func() { server.Close() })
	return socket, st
}

func registered(t *testing.T, socket, tenant string) []access.Entry {
	t.Helper()
	entries, err := api.NewClient(socket).Accesses(context.Background(), tenant, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	return entries
}

func me(t *testing.T) string {
	t.Helper()
	current, err := user.Current()
	if err != nil {
		t.Fatal(err)
	}
	return current.Username
}

// A secret is printed once its read is recorded, in the name of the person
// sudo names.
func TestTenantSecretIsRecorded(t *testing.T) {
	socket, _ := registerAgent(t)
	out := capture(t)
	if err := run([]string{"tenant", "secret", "acme", "SESSION", "-socket", socket}); err == nil || !strings.Contains(err.Error(), "-reason") {
		t.Fatalf("read without a reason: %v", err)
	}
	if err := run([]string{"tenant", "secret", "acme", "SESSION", "-socket", socket, "-reason", "login fails"}); err == nil || !strings.Contains(err.Error(), "-ticket") {
		t.Fatalf("read without a ticket: %v", err)
	}
	if out.Len() != 0 || len(registered(t, socket, "acme")) != 0 {
		t.Fatalf("refused reads: %q", out)
	}
	t.Setenv("SUDO_USER", "jan")
	if err := run([]string{"tenant", "secret", "acme", "SESSION", "-socket", socket, "-reason", "login fails", "-ticket", "ZD-1234"}); err != nil {
		t.Fatal(err)
	}
	if len(strings.TrimSpace(out.String())) != 64 {
		t.Fatalf("printed %q", out)
	}
	got := registered(t, socket, "acme")
	if len(got) != 1 || got[0].What != "SESSION" || got[0].Actor != "jan via "+me(t) || got[0].Ticket != "ZD-1234" {
		t.Fatalf("recorded: %+v", got)
	}
}

// A copy of the database made by hand is recorded; the timer's is not,
// also when an older unit runs it without -scheduled.
func TestBackupByHandIsRecorded(t *testing.T) {
	socket, _ := registerAgent(t)
	capture(t)
	dir := t.TempDir()
	if err := run([]string{"backup", "-socket", socket, dir}); err == nil || !strings.Contains(err.Error(), "-reason") || !strings.Contains(err.Error(), "-scheduled") {
		t.Fatalf("a copy by hand without a reason: %v", err)
	}
	if err := run([]string{"backup", "-socket", socket, "-scheduled", "-reason", "x", dir}); err == nil {
		t.Fatal("a scheduled copy with a reason")
	}
	if err := run([]string{"backup", "-socket", socket, "-reason", "new hardware", "-ticket", "incident", dir}); err != nil {
		t.Fatal(err)
	}
	if got := registered(t, socket, "acme"); len(got) != 1 || got[0].Kind != access.State || got[0].Tenant != "" {
		t.Fatalf("recorded: %+v", got)
	}
	if err := run([]string{"backup", "-socket", socket, "-scheduled", dir}); err != nil {
		t.Fatal(err)
	}
	// Run by another unit: a copy by hand still.
	other := filepath.Join(t.TempDir(), "cgroup")
	if err := os.WriteFile(other, []byte("0::/system.slice/something-else.service\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cgroupFile = other
	t.Cleanup(func() { cgroupFile = "/proc/self/cgroup" })
	if err := run([]string{"backup", "-socket", socket, dir}); err == nil {
		t.Fatal("a copy by another unit taken as the timer's")
	}
	// The unit of a release before, without -scheduled: systemd runs it in
	// its own control group.
	if err := os.WriteFile(other, []byte("0::/system.slice/murder-backup.service\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"backup", "-socket", socket, dir}); err != nil {
		t.Fatal(err)
	}
	if got := registered(t, socket, ""); len(got) != 1 {
		t.Fatalf("scheduled copies recorded: %+v", got)
	}
	if entries, _ := os.ReadDir(dir); len(entries) == 0 {
		t.Fatal("no copies")
	}
	unit, err := deploy.Files.ReadFile("murder-backup.service")
	if err != nil || !strings.Contains(string(unit), "murder backup -scheduled ") {
		t.Fatalf("the unit does not say -scheduled: %v", err)
	}
}

// An access outside Murder is recorded by hand, when it was, and the
// register is a customer's extract: theirs and the copies of the whole
// state, in a table, CSV a spreadsheet takes as text, or JSON.
func TestAccessLogAndList(t *testing.T) {
	socket, st := registerAgent(t)
	out := capture(t)
	if err := run([]string{"access", "log", "-socket", socket, "-tenant", "acme", "-reason", "x", "-ticket", "incident"}); err == nil || !strings.Contains(err.Error(), "-what") {
		t.Fatalf("logged without what: %v", err)
	}
	if err := run([]string{"access", "log", "-socket", socket, "-tenant", "acme", "-what", "docker exec into its container", "-reason", "=HYPERLINK(\"x\")", "-ticket", "ZD-9", "-at", "2026-11-06T08:00:00Z"}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "recorded access") {
		t.Fatalf("said: %q", out)
	}
	if _, err := st.Access("", "access.state", (access.Request{Kind: access.State, Purpose: "restore drill", Ticket: "incident"}).Detail(), "root", false); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Access("globex", "access.logs", (access.Request{Tenant: "globex", Kind: access.Logs, Purpose: "theirs", Ticket: "ZD-2"}).Detail(), "root", false); err != nil {
		t.Fatal(err)
	}
	out.Reset()
	if err := run([]string{"access", "list", "-socket", socket, "-tenant", "acme", "-format", "csv"}); err != nil {
		t.Fatal(err)
	}
	rows, err := csv.NewReader(strings.NewReader(out.String())).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 || strings.Join(rows[0], ",") != strings.Join(accessColumns, ",") {
		t.Fatalf("rows: %q", rows)
	}
	if rows[1][2] != "2026-11-06T08:00:00Z" || rows[1][4] != "acme" || rows[1][5] != "manual" || rows[1][7] != "'=HYPERLINK(\"x\")" || rows[1][8] != "ZD-9" {
		t.Fatalf("the manual access: %q", rows[1])
	}
	if rows[2][4] != "*" || rows[2][5] != "state" {
		t.Fatalf("the copy of the state: %q", rows[2])
	}
	out.Reset()
	if err := run([]string{"access", "list", "-socket", socket, "-format", "json", "-since", "2000-01-01"}); err != nil {
		t.Fatal(err)
	}
	var all []access.Entry
	if err := json.Unmarshal(out.Bytes(), &all); err != nil || len(all) != 3 {
		t.Fatalf("all: %v %s", err, out)
	}
	out.Reset()
	if err := run([]string{"access", "list", "-socket", socket, "-tenant", "acme"}); err != nil || !strings.Contains(out.String(), "docker exec into its container") {
		t.Fatalf("the table: %v\n%s", err, out)
	}
	if err := run([]string{"access", "list", "-socket", socket, "-format", "xml"}); err == nil {
		t.Fatal("an unknown format")
	}
	// murder events says an access as a line, not JSON.
	out.Reset()
	if err := run([]string{"events", "-socket", socket, "-tenant", "acme"}); err != nil || !strings.Contains(out.String(), `manual docker exec into its container, for "=HYPERLINK(\"x\")" (ticket ZD-9)`) {
		t.Fatalf("events: %v\n%s", err, out)
	}
}

// With no agent, an access made with -offline is written to the node's
// offline file, said loudly; a sync records each once, also after a sync
// that stopped halfway.
func TestOfflineAccessesAreSyncedOnce(t *testing.T) {
	socket, _ := registerAgent(t)
	out := capture(t)
	offline := filepath.Join(t.TempDir(), "access-offline.jsonl")
	nowhere := filepath.Join(socketDir(t), "none.sock")
	logArgs := []string{"access", "log", "-socket", nowhere, "-offline-file", offline, "-tenant", "acme", "-what", "read its dataset", "-reason", "the cluster is down", "-ticket", "incident"}
	if err := run(logArgs); err == nil || !strings.Contains(err.Error(), "could not record") {
		t.Fatalf("without -offline: %v", err)
	}
	if _, err := os.Stat(offline); !os.IsNotExist(err) {
		t.Fatal("written offline unasked")
	}
	t.Setenv("SUDO_USER", "jan")
	stderr := captureStderr(t)
	if err := run(append(logArgs, "-offline")); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stderr(), "WARNING") || !strings.Contains(stderr(), "murder access sync") {
		t.Fatalf("said quietly: %q", stderr())
	}
	records, _, err := access.ReadOffline(offline)
	if err != nil || len(records) != 1 || records[0].Operator != "jan" || records[0].What != "read its dataset" {
		t.Fatalf("offline: %+v %v", records, err)
	}
	// A refusal is not taken offline: the agent answers, and refuses.
	if err := run([]string{"access", "log", "-socket", socket, "-offline", "-offline-file", offline, "-tenant", "acme", "-what", "x", "-reason", "x", "-ticket", "x", "-at", "2999-01-01T00:00:00Z"}); err == nil {
		t.Fatal("-at with -offline")
	}
	if err := run([]string{"access", "log", "-socket", socket, "-offline-file", offline, "-tenant", "acme", "-what", "x", "-reason", "x", "-ticket", "x", "-at", "2999-01-01T00:00:00Z"}); err == nil || !strings.Contains(err.Error(), "refused") {
		t.Fatalf("an access in the future: %v", err)
	}
	t.Setenv("SUDO_USER", "anna")
	out.Reset()
	if err := run([]string{"access", "sync", "-socket", socket, "-offline-file", offline}); err != nil {
		t.Fatal(err)
	}
	got := registered(t, socket, "acme")
	if len(got) != 1 || got[0].Offline != records[0].ID || !got[0].When().Equal(records[0].At) || got[0].Actor != "jan via "+me(t) || got[0].What != "read its dataset" {
		t.Fatalf("synced: %+v", got)
	}
	if _, err := os.Stat(offline); !os.IsNotExist(err) {
		t.Fatal("the offline file stays after the sync")
	}
	// The file written back as if the sync stopped before it cut it: the
	// record is recorded once still.
	line, _ := json.Marshal(records[0])
	if err := os.WriteFile(offline, append(line, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"access", "sync", "-socket", socket, "-offline-file", offline}); err != nil {
		t.Fatal(err)
	}
	if got := registered(t, socket, "acme"); len(got) != 1 {
		t.Fatalf("synced twice: %+v", got)
	}
	out.Reset()
	if err := run([]string{"access", "sync", "-socket", socket, "-offline-file", offline}); err != nil || !strings.Contains(out.String(), "no accesses wait") {
		t.Fatalf("nothing to sync: %v %q", err, out)
	}
	// A record its writer wrote as themselves, not through sudo, is theirs
	// when root syncs it; a damaged line stays, and the sync says so.
	t.Setenv("SUDO_USER", "")
	theirs := access.OfflineRecord{ID: "THEIRS", At: time.Date(2026, 11, 7, 9, 0, 0, 0, time.UTC), Actor: "ewa",
		Request: access.Request{Tenant: "acme", Kind: access.Manual, What: "read its files", Purpose: "x", Ticket: "incident"}}
	line, _ = json.Marshal(theirs)
	if err := os.WriteFile(offline, append(append(line, '\n'), []byte(`{"id":"TORN","purp`)...), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"access", "sync", "-socket", socket, "-offline-file", offline}); err == nil || !strings.Contains(err.Error(), "1 lines") {
		t.Fatalf("a damaged line unsaid: %v", err)
	}
	if got := registered(t, socket, "acme"); len(got) != 2 || got[1].Actor != "ewa via "+me(t) || got[1].Offline != "THEIRS" {
		t.Fatalf("synced: %+v", got)
	}
	if records, damaged, _ := access.ReadOffline(offline); len(records) != 0 || len(damaged) != 1 {
		t.Fatalf("left: %+v %q", records, damaged)
	}
}

// Backups commands that decrypt a tenant's data record the access first,
// and refuse -offline where they need the agent anyway.
func TestBackupsReadsAreRecordedFirst(t *testing.T) {
	_, _ = registerAgent(t)
	dir := t.TempDir()
	for _, name := range []string{"s3.access", "s3.secret"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("key\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	identity := filepath.Join(dir, "identity.txt")
	if err := os.WriteFile(identity, []byte("AGE-SECRET-KEY-1GFPYYSJZGFPYYSJZGFPYYSJZGFPYYSJZGFPYYSJZGFPYYSJZGFPQ4EGAEX\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	nowhere := filepath.Join(socketDir(t), "none.sock")
	configPath := filepath.Join(dir, "agent.yaml")
	config := "baseDomain: example.com\nsocket: " + nowhere + "\nvolumes: {driver: zfs, parent: tank/murder/tenants}\nbackups:\n  mode: test\n" +
		"  endpoint: https://127.0.0.1:1\n  region: hel1\n  bucket: b\n" +
		"  accessKeyFile: " + filepath.Join(dir, "s3.access") + "\n  secretKeyFile: " + filepath.Join(dir, "s3.secret") + "\n" +
		"  recipient: age1ql3z7hjy54pw3hyww5ayyfg7zqgvc7w3j2elw8zmrj2kg5sfn9aqmcac8p\n"
	if err := os.WriteFile(configPath, []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	offline := filepath.Join(dir, "access-offline.jsonl")
	if err := runBackups([]string{"verify", "acme", "-config", configPath, "-identity", identity}); err == nil || !strings.Contains(err.Error(), "-reason") {
		t.Fatalf("verify without a reason: %v", err)
	}
	if err := runBackups([]string{"restore", "acme", "-config", configPath, "-identity", identity, "-offline"}); err == nil || !strings.Contains(err.Error(), "-offline is for verify and state") {
		t.Fatalf("restore offline: %v", err)
	}
	// No agent: refused, until -offline writes it down first.
	err := runBackups([]string{"verify", "acme", "-config", configPath, "-identity", identity, "-reason", "drill", "-ticket", "incident", "-offline-file", offline})
	if err == nil || !strings.Contains(err.Error(), "could not record") {
		t.Fatalf("verify with no agent: %v", err)
	}
	if _, err := os.Stat(offline); !os.IsNotExist(err) {
		t.Fatal("written offline unasked")
	}
	captureStderr(t)
	// The bucket is nowhere either: the read fails after the record.
	_ = runBackups([]string{"verify", "acme", "-config", configPath, "-identity", identity, "-reason", "drill", "-ticket", "incident", "-offline", "-offline-file", offline})
	if records, _, err := access.ReadOffline(offline); err != nil || len(records) != 1 || records[0].Kind != access.BackupVerify || records[0].Tenant != "acme" {
		t.Fatalf("offline: %+v %v", records, err)
	}
}

// captureStderr collects what is written to os.Stderr until the test
// ends; the function returns it so far.
func captureStderr(t *testing.T) func() string {
	t.Helper()
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	original := os.Stderr
	os.Stderr = writer
	var collected bytes.Buffer
	done := make(chan struct{})
	go func() {
		_, _ = io.Copy(&collected, reader)
		close(done)
	}()
	closed := false
	finish := func() {
		if !closed {
			closed = true
			os.Stderr = original
			writer.Close()
			<-done
		}
	}
	t.Cleanup(finish)
	return func() string {
		finish()
		return collected.String()
	}
}
