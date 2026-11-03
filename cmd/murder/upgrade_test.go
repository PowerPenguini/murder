package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/cluster"
	"github.com/PowerPenguini/murder/internal/spec"
	"github.com/PowerPenguini/murder/internal/store"
	"github.com/PowerPenguini/murder/internal/upgrade"
)

// upgradeAgent fakes a cluster of three nodes behind the agent's API, so
// "murder upgrade" is tested over the wire; internal/clusterops tests what
// an upgrade does. refuse names nodes that refuse the binary.
type upgradeAgent struct {
	mu        sync.Mutex
	running   map[string]string
	refuse    map[string]bool
	installed []string
	frozen    string
	thawed    int
}

func (a *upgradeAgent) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	a.mu.Lock()
	defer a.mu.Unlock()
	node := strings.TrimPrefix(r.URL.Path, "/v1/upgrade/nodes/")
	if node == "-" {
		node = "n1"
	}
	switch {
	case r.URL.Path == "/v1/cluster":
		_ = json.NewEncoder(w).Encode(cluster.Status{Node: "n1", Members: []cluster.Member{{Name: "n1"}, {Name: "n2"}, {Name: "n3"}}})
	case r.URL.Path == "/v1/nodes":
		_ = json.NewEncoder(w).Encode(api.Nodes{Self: "n1", Frozen: a.frozen})
	case r.URL.Path == "/v1/upgrade/freeze":
		if a.frozen != "" && a.frozen != r.URL.Query().Get("replaces") {
			w.WriteHeader(http.StatusConflict)
			_, _ = w.Write([]byte(`{"error":"changes are frozen"}`))
			return
		}
		a.frozen = r.URL.Query().Get("reason")
		w.WriteHeader(http.StatusNoContent)
	case r.URL.Path == "/v1/upgrade/thaw":
		if reason := r.URL.Query().Get("reason"); reason != "" && a.frozen != reason {
			w.WriteHeader(http.StatusConflict)
			_, _ = w.Write([]byte(`{"error":"changes are frozen for another"}`))
			return
		}
		a.frozen = ""
		a.thawed++
		w.WriteHeader(http.StatusNoContent)
	case r.Method == http.MethodGet:
		now := time.Now()
		_ = json.NewEncoder(w).Encode(upgrade.Info{Node: node, Version: upgrade.Version{Binary: a.running[node], Schema: 5}, Started: now.Add(-time.Second), LastPass: now})
	case r.Method == http.MethodPut:
		data, _ := io.ReadAll(r.Body)
		sum := sha256.Sum256(data)
		if r.Header.Get(api.FreezeHeader) != a.frozen {
			w.WriteHeader(http.StatusPreconditionFailed)
			_, _ = w.Write([]byte(`{"error":"the upgrade no longer holds the freeze"}`))
			return
		}
		if a.refuse[node] || r.Header.Get(api.ShaHeader) != hex.EncodeToString(sum[:]) {
			w.WriteHeader(http.StatusUnprocessableEntity)
			_, _ = w.Write([]byte(`{"error":"the edge did not hand over"}`))
			return
		}
		a.installed = append(a.installed, node)
		a.running[node] = r.Header.Get(api.ShaHeader)
		w.WriteHeader(http.StatusNoContent)
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func startUpgradeAgent(t *testing.T, agent *upgradeAgent) (*api.Client, string) {
	t.Helper()
	socket := filepath.Join(t.TempDir(), "agent.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: agent}
	go server.Serve(listener)
	t.Cleanup(func() { _ = server.Close() })
	binary := filepath.Join(t.TempDir(), "murder-new")
	if err := os.WriteFile(binary, []byte("#!/bin/sh\necho '{\"binary\":\"says-anything\",\"schema\":5}'\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	return api.NewClient(socket), binary
}

func TestUpgradeGoesNodeByNodeThisOneLast(t *testing.T) {
	agent := &upgradeAgent{running: map[string]string{"n1": "old", "n2": "old", "n3": "old"}}
	client, binary := startUpgradeAgent(t, agent)
	capture(t)
	if err := upgradeCluster(context.Background(), client, binary, 5*time.Second, false); err != nil {
		t.Fatal(err)
	}
	sum, _ := upgrade.Hash(binary)
	if !slices.Equal(agent.installed, []string{"n2", "n3", "n1"}) || agent.frozen != "" || agent.thawed != 1 || agent.running["n1"] != sum {
		t.Fatalf("installed %v, frozen %q, thawed %d, n1 runs %s", agent.installed, agent.frozen, agent.thawed, agent.running["n1"])
	}
}

func TestARefusalBeforeAnyNodeChangedThaws(t *testing.T) {
	agent := &upgradeAgent{running: map[string]string{"n1": "old", "n2": "old", "n3": "old"}, refuse: map[string]bool{"n2": true}}
	client, binary := startUpgradeAgent(t, agent)
	capture(t)
	err := upgradeCluster(context.Background(), client, binary, 5*time.Second, false)
	if err == nil || !strings.Contains(err.Error(), "no node was upgraded; changes are taken again") || agent.frozen != "" {
		t.Fatalf("upgrade: %v, frozen %q", err, agent.frozen)
	}
}

// A node takes a binary only under an upgrade's freeze: one that has not
// applied it yet refuses, and installs nothing.
func TestANodeInstallsOnlyUnderAnUpgradeFreeze(t *testing.T) {
	path := filepath.Join(t.TempDir(), "murder.db")
	if err := store.Init(path, "n1"); err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(path, store.Options{NetworkPool: netip.MustParsePrefix("10.240.0.0/16"), Sealer: func(string, string, spec.Secret) (string, error) { return "sealed", nil }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	executable := filepath.Join(t.TempDir(), "murder")
	if err := os.WriteFile(executable, []byte("old"), 0o755); err != nil {
		t.Fatal(err)
	}
	caughtUp := 0
	upgrader := &agentUpgrader{store: st, Node: &upgrade.Node{Executable: executable}, catchUp: func() error { caughtUp++; return nil }}
	err = upgrader.Install(context.Background(), strings.NewReader("new"), "sha", "")
	if err == nil || !strings.Contains(err.Error(), "not frozen for an upgrade") || caughtUp != 1 {
		t.Fatalf("install without a freeze: %v (caught up %d times)", err, caughtUp)
	}
	if data, _ := os.ReadFile(executable); string(data) != "old" {
		t.Fatal("a binary was installed")
	}
}

// Over the wire too, an upgrade leaves another's freeze alone unless told
// -take-over.
func TestAnUpgradeTakesOverOnlyWhenTold(t *testing.T) {
	agent := &upgradeAgent{running: map[string]string{"n1": "old", "n2": "old", "n3": "old"}, frozen: store.UpgradeFreeze + "x from node n2, operation 01"}
	client, binary := startUpgradeAgent(t, agent)
	capture(t)
	err := upgradeCluster(context.Background(), client, binary, 5*time.Second, false)
	if err == nil || !strings.Contains(err.Error(), "-take-over") || len(agent.installed) != 0 || !strings.HasSuffix(agent.frozen, "operation 01") {
		t.Fatalf("upgrade: %v, installed %v, frozen %q", err, agent.installed, agent.frozen)
	}
	if err := upgradeCluster(context.Background(), client, binary, 5*time.Second, true); err != nil || len(agent.installed) != 3 || agent.frozen != "" {
		t.Fatalf("a takeover: %v, installed %v, frozen %q", err, agent.installed, agent.frozen)
	}
}

// A node takes a binary only under the freeze of the upgrade that sends
// it: one whose freeze another took over, or a person thawed, installs
// nothing more.
func TestANodeInstallsOnlyUnderTheSendersFreeze(t *testing.T) {
	st := openStore(t)
	executable := filepath.Join(t.TempDir(), "murder")
	if err := os.WriteFile(executable, []byte("old"), 0o755); err != nil {
		t.Fatal(err)
	}
	upgrader := &agentUpgrader{store: st, Node: &upgrade.Node{Executable: executable}}
	if err := st.Freeze(store.UpgradeFreeze+"abc from node n1, operation 02", "root"); err != nil {
		t.Fatal(err)
	}
	err := upgrader.Install(context.Background(), strings.NewReader("new"), "sha", store.UpgradeFreeze+"abc from node n1, operation 01")
	if !errors.Is(err, upgrade.ErrNotOwner) {
		t.Fatalf("install under another's freeze: %v", err)
	}
	if err := st.Thaw("root"); err != nil {
		t.Fatal(err)
	}
	if err := upgrader.Install(context.Background(), strings.NewReader("new"), "sha", store.UpgradeFreeze+"abc from node n1, operation 02"); !errors.Is(err, upgrade.ErrNotOwner) {
		t.Fatalf("install once thawed: %v", err)
	}
	if data, _ := os.ReadFile(executable); string(data) != "old" {
		t.Fatal("a binary was installed")
	}
}

// takeOverReader takes the freeze over as the binary arrives, as another
// upgrade may while an upload stalls.
type takeOverReader struct {
	io.Reader
	takeOver func()
	once     sync.Once
}

func (r *takeOverReader) Read(p []byte) (int, error) {
	r.once.Do(r.takeOver)
	return r.Reader.Read(p)
}

// The freeze is checked again, caught up, once the binary has arrived:
// one another upgrade took over, or a person thawed, while it arrived
// installs nothing and restarts nothing.
func TestAFreezeTakenOverDuringTheUploadInstallsNothing(t *testing.T) {
	ours := store.UpgradeFreeze + "abc from node n1, operation 01"
	for _, test := range []struct {
		name   string
		freeze string // as the sender says; "" as releases before
		change func(*store.SQLite) error
	}{
		{"taken over", ours, func(st *store.SQLite) error {
			return st.TakeOverFreeze(ours, store.UpgradeFreeze+"def from node n2, operation 02", "root")
		}},
		{"taken over, the sender of a release before", "", func(st *store.SQLite) error {
			return st.TakeOverFreeze(ours, store.UpgradeFreeze+"def from node n2, operation 02", "root")
		}},
		{"thawed", ours, func(st *store.SQLite) error { return st.Thaw("root") }},
	} {
		t.Run(test.name, func(t *testing.T) {
			st := openStore(t)
			dir := t.TempDir()
			old := []byte("#!/bin/sh\necho '{\"binary\":\"old\",\"schema\":" + fmt.Sprint(store.SchemaVersion) + "}'\n")
			executable := filepath.Join(dir, "murder")
			if err := os.WriteFile(executable, old, 0o755); err != nil {
				t.Fatal(err)
			}
			current, _ := upgrade.Hash(executable)
			if err := st.Freeze(ours, "root"); err != nil {
				t.Fatal(err)
			}
			restarted := 0
			caughtUp := 0
			upgrader := &agentUpgrader{store: st, catchUp: func() error { caughtUp++; return nil }, Node: &upgrade.Node{
				Executable: executable, Current: upgrade.Version{Binary: current, Schema: store.SchemaVersion},
				EdgePIDFile: filepath.Join(dir, "edge.pid"), Database: filepath.Join(dir, "murder.db"),
				Snapshot: func() (string, error) {
					path := filepath.Join(dir, "copy")
					return path, os.WriteFile(path, []byte("copy"), 0o600)
				},
				Restart: func() { restarted++ },
				Log:     slog.New(slog.NewTextHandler(io.Discard, nil)),
			}}
			binary := []byte("#!/bin/sh\necho '{\"binary\":\"new\",\"schema\":" + fmt.Sprint(store.SchemaVersion) + "}'\n")
			sum := sha256.Sum256(binary)
			body := &takeOverReader{Reader: strings.NewReader(string(binary)), takeOver: func() {
				if err := test.change(st); err != nil {
					t.Error(err)
				}
			}}
			err := upgrader.Install(context.Background(), body, hex.EncodeToString(sum[:]), test.freeze)
			if !errors.Is(err, upgrade.ErrNotOwner) || caughtUp != 2 {
				t.Fatalf("install whose freeze went as it arrived: %v (caught up %d times)", err, caughtUp)
			}
			if data, _ := os.ReadFile(executable); string(data) != string(old) || restarted != 0 {
				t.Fatalf("installed %q, restarted %d", data, restarted)
			}
			if _, err := upgrade.LastInstall(executable); !errors.Is(err, upgrade.ErrNothingToRollBack) {
				t.Fatalf("a record of an install: %v", err)
			}
		})
	}
}

func openStore(t *testing.T) *store.SQLite {
	t.Helper()
	path := filepath.Join(t.TempDir(), "murder.db")
	if err := store.Init(path, "n1"); err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(path, store.Options{NetworkPool: netip.MustParsePrefix("10.240.0.0/16"), Sealer: func(string, string, spec.Secret) (string, error) { return "sealed", nil }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

// nodeConfig writes an agent config with its database and master key in
// dir, and returns its path.
func nodeConfig(t *testing.T, dir, more string) string {
	t.Helper()
	return writeFile(t, dir, "agent.yaml", "baseDomain: example.test\nnode: n1\nfirewall: off\ndatabase: "+filepath.Join(dir, "murder.db")+"\nmasterKeyFile: "+filepath.Join(dir, "master.key")+"\n"+more)
}

// A rollback whose database cannot go back changes nothing, and says why;
// one that stopped after the binary went back finishes when run again.
func TestARollbackChecksTheDatabaseFirstAndFinishes(t *testing.T) {
	dir := t.TempDir()
	cfg := nodeConfig(t, dir, "")
	database := filepath.Join(dir, "murder.db")
	if err := store.Init(database, "n1"); err != nil {
		t.Fatal(err)
	}
	// The new release left the database of a schema the old one does not
	// open.
	db, err := sql.Open("sqlite", database)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(fmt.Sprintf("PRAGMA user_version = %d", store.SchemaVersion+1))
	if err = errors.Join(err, db.Close()); err != nil {
		t.Fatal(err)
	}
	executable := filepath.Join(dir, "murder")
	_ = os.WriteFile(executable, []byte("new"), 0o755)
	_ = os.WriteFile(executable+".prev", []byte("old"), 0o755)
	old, _ := upgrade.Hash(executable + ".prev")
	installed, _ := upgrade.Hash(executable)
	record, _ := json.Marshal(upgrade.Record{From: upgrade.Version{Binary: old, Schema: store.SchemaVersion}, To: upgrade.Version{Binary: installed, Schema: store.SchemaVersion + 1}})
	_ = os.WriteFile(executable+".upgrade", record, 0o600)
	capture(t)
	rollback := func() error { return runRollback([]string{"-config", cfg, "-binary", executable, "-force"}) }

	// No copy of the database: nothing changes, and again so.
	for range 2 {
		err := rollback()
		if err == nil || !strings.Contains(err.Error(), "nothing was rolled back") {
			t.Fatalf("rollback without the database's copy: %v", err)
		}
		if data, _ := os.ReadFile(executable); string(data) != "new" {
			t.Fatalf("the binary went back without its database: %q", data)
		}
		if _, err := upgrade.LastInstall(executable); err != nil {
			t.Fatalf("the record is gone: %v", err)
		}
	}
	if err := store.Init(database+".before-upgrade", "n1"); err != nil {
		t.Fatal(err)
	}
	// It stopped once the binary was back: run again, it puts the
	// database back.
	if _, err := upgrade.Rollback(executable); err != nil {
		t.Fatal(err)
	}
	if err := rollback(); err != nil {
		t.Fatal(err)
	}
	if data, _ := os.ReadFile(executable); string(data) != "old" {
		t.Fatalf("after rollback %q is in place", data)
	}
	if schema, err := store.DatabaseSchema(database); err != nil || schema != store.SchemaVersion {
		t.Fatalf("after rollback the database is of schema %d: %v", schema, err)
	}
	if err := rollback(); !errors.Is(err, upgrade.ErrNothingToRollBack) {
		t.Fatalf("a rollback once done: %v", err)
	}
}

// A rollback puts the database back before the binary: if that fails, the
// new binary, whose rollback resumes, is still in place, and the error
// says to run it again, not the old binary (one of v0.1.0 drops the
// record and cannot finish).
func TestARollbackPutsTheDatabaseBackBeforeTheBinary(t *testing.T) {
	dir := t.TempDir()
	cfg := nodeConfig(t, dir, "")
	database := filepath.Join(dir, "murder.db")
	if err := store.Init(database, "n1"); err != nil {
		t.Fatal(err)
	}
	if err := store.Init(database+".before-upgrade", "n1"); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", database)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(fmt.Sprintf("PRAGMA user_version = %d", store.SchemaVersion+1))
	if err = errors.Join(err, db.Close()); err != nil {
		t.Fatal(err)
	}
	executable := filepath.Join(dir, "murder")
	_ = os.WriteFile(executable, []byte("new"), 0o755)
	_ = os.WriteFile(executable+".prev", []byte("old"), 0o755)
	old, _ := upgrade.Hash(executable + ".prev")
	installed, _ := upgrade.Hash(executable)
	record, _ := json.Marshal(upgrade.Record{From: upgrade.Version{Binary: old, Schema: store.SchemaVersion}, To: upgrade.Version{Binary: installed, Schema: store.SchemaVersion + 1}})
	_ = os.WriteFile(executable+".upgrade", record, 0o600)
	capture(t)
	rollback := func() error { return runRollback([]string{"-config", cfg, "-binary", executable, "-force"}) }

	restoreDatabase = func(string, string) (string, error) { return "", errors.New("disk full") }
	defer func() { restoreDatabase = store.Restore }()
	err = rollback()
	if err == nil || !strings.Contains(err.Error(), "disk full") || !strings.Contains(err.Error(), `run "`+executable+` upgrade rollback" again`) || strings.Contains(err.Error(), ".prev") {
		t.Fatalf("a rollback whose database did not go back: %v", err)
	}
	if data, _ := os.ReadFile(executable); string(data) != "new" {
		t.Fatalf("the binary went back before the database: %q in place", data)
	}
	if data, _ := os.ReadFile(executable + ".prev"); string(data) != "old" {
		t.Fatalf("murder.prev holds %q", data)
	}
	if _, err := upgrade.LastInstall(executable); err != nil {
		t.Fatalf("the record is gone: %v", err)
	}

	// Run again, as the error says, it finishes.
	restoreDatabase = store.Restore
	if err := rollback(); err != nil {
		t.Fatal(err)
	}
	if data, _ := os.ReadFile(executable); string(data) != "old" {
		t.Fatalf("after rollback %q is in place", data)
	}
	if data, _ := os.ReadFile(executable + ".failed"); string(data) != "new" {
		t.Fatalf("the new binary is not kept: %q", data)
	}
	if schema, err := store.DatabaseSchema(database); err != nil || schema != store.SchemaVersion {
		t.Fatalf("after rollback the database is of schema %d: %v", schema, err)
	}
	if _, err := upgrade.LastInstall(executable); !errors.Is(err, upgrade.ErrNothingToRollBack) {
		t.Fatalf("the record stays after the rollback finished: %v", err)
	}
}
