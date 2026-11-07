package access

import (
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/PowerPenguini/murder/internal/store"
)

func TestARequestSaysWhyAndForWhom(t *testing.T) {
	good := Request{Tenant: " acme ", Kind: Logs, Purpose: " export fails ", Ticket: " INCIDENT "}
	if err := good.Normalize(); err != nil {
		t.Fatal(err)
	}
	if good.Tenant != "acme" || good.Purpose != "export fails" || good.Ticket != "incident" {
		t.Fatalf("normalized: %+v", good)
	}
	for name, request := range map[string]Request{
		"no purpose":         {Tenant: "acme", Kind: Logs, Ticket: "ZD-1"},
		"no ticket":          {Tenant: "acme", Kind: Logs, Purpose: "x"},
		"blank purpose":      {Tenant: "acme", Kind: Logs, Purpose: "  ", Ticket: "ZD-1"},
		"no tenant":          {Kind: Logs, Purpose: "x", Ticket: "ZD-1"},
		"a state of one":     {Tenant: "acme", Kind: State, Purpose: "x", Ticket: "ZD-1"},
		"a secret unnamed":   {Tenant: "acme", Kind: Secret, Purpose: "x", Ticket: "ZD-1"},
		"a manual untold":    {Tenant: "acme", Kind: Manual, Purpose: "x", Ticket: "ZD-1"},
		"an unknown kind":    {Tenant: "acme", Kind: "everything", Purpose: "x", Ticket: "ZD-1"},
		"a line break":       {Tenant: "acme", Kind: Logs, Purpose: "x\nforged line", Ticket: "ZD-1"},
		"an escape":          {Tenant: "acme", Kind: Logs, Purpose: "x", Ticket: "ZD-1\x1b[2J"},
		"a separator":        {Tenant: "acme", Kind: Logs, Purpose: "x y", Ticket: "ZD-1"},
		"a purpose too long": {Tenant: "acme", Kind: Logs, Purpose: strings.Repeat("ą", maxPurpose+1), Ticket: "ZD-1"},
		"invalid UTF-8":      {Tenant: "acme", Kind: Logs, Purpose: "\xff", Ticket: "ZD-1"},
	} {
		if err := request.Normalize(); err == nil {
			t.Errorf("%s: taken", name)
		}
	}
	// Polish and the like are text, as long as the limit counts letters.
	if err := (&Request{Tenant: "acme", Kind: Logs, Purpose: strings.Repeat("ą", maxPurpose), Ticket: "ZD-1"}).Normalize(); err != nil {
		t.Fatal(err)
	}
	if err := (&Request{Kind: State, Purpose: "restore drill", Ticket: "legal:court order 12/2026"}).Normalize(); err != nil {
		t.Fatal(err)
	}
}

func TestAnEntryIsReadFromItsEvent(t *testing.T) {
	at := time.Date(2026, 11, 7, 10, 0, 0, 0, time.UTC)
	request := Request{Tenant: "acme", Kind: Manual, What: "docker exec", Purpose: "export \"fails\"", Ticket: "ZD-1", AccessedAt: at.Add(-time.Hour)}
	entry, ok := FromEvent(store.Event{ID: 7, At: at, Actor: "jan via root", Action: request.Kind.Action(), Tenant: "acme", Detail: request.Detail()})
	if !ok || entry.ID != 7 || entry.Kind != Manual || entry.What != "docker exec" || entry.Purpose != `export "fails"` || entry.Ticket != "ZD-1" ||
		!entry.AccessedAt.Equal(at.Add(-time.Hour)) || !entry.When().Equal(at.Add(-time.Hour)) || entry.Actor != "jan via root" {
		t.Fatalf("entry: %+v", entry)
	}
	if !strings.Contains(entry.Said(), `manual docker exec, for "export \"fails\"" (ticket ZD-1), accessed at 2026-11-07T09:00:00Z`) {
		t.Fatalf("said: %s", entry.Said())
	}
	if _, ok := FromEvent(store.Event{Action: "tenant.created"}); ok {
		t.Fatal("another event read as an access")
	}
	// A detail no release wrote is kept, not lost.
	if odd, ok := FromEvent(store.Event{Action: "access.logs", Detail: "by hand"}); !ok || odd.What != "by hand" {
		t.Fatalf("an odd detail: %+v", odd)
	}
}

func TestAGrantServesTheSameReadBySameActorForAWhile(t *testing.T) {
	at := time.Date(2026, 11, 7, 10, 0, 0, 0, time.UTC)
	entry := Entry{ID: 3, At: at, Actor: "jan via root", Tenant: "acme", Kind: Secret, What: "DB", Purpose: "x", Ticket: "ZD-1"}
	if err := entry.Grants(Secret, "acme", "DB", "jan via root", at.Add(59*time.Minute)); err != nil {
		t.Fatal(err)
	}
	for name, err := range map[string]error{
		"another kind":     entry.Grants(Logs, "acme", "", "jan via root", at),
		"another tenant":   entry.Grants(Secret, "globex", "DB", "jan via root", at),
		"another secret":   entry.Grants(Secret, "acme", "OTHER", "jan via root", at),
		"another person":   entry.Grants(Secret, "acme", "DB", "anna via root", at),
		"too late":         entry.Grants(Secret, "acme", "DB", "jan via root", at.Add(GrantFor+time.Second)),
		"far before":       entry.Grants(Secret, "acme", "DB", "jan via root", at.Add(-2*time.Minute)),
		"after the fact":   Entry{ID: 4, At: at, Actor: "jan via root", Tenant: "acme", Kind: Logs, AccessedAt: at.Add(-time.Hour)}.Grants(Logs, "acme", "", "jan via root", at),
		"recorded offline": Entry{ID: 5, At: at, Actor: "jan via root", Tenant: "acme", Kind: Logs, Offline: "X"}.Grants(Logs, "acme", "", "jan via root", at),
	} {
		if !errors.Is(err, ErrNoGrant) {
			t.Errorf("%s: %v", name, err)
		}
	}
	restore := Entry{ID: 6, At: at, Actor: "root", Tenant: "acme", Kind: BackupRestore}
	if err := restore.Grants(BackupRestore, "acme", "", "root", at.Add(20*time.Hour)); err != nil {
		t.Fatalf("a restore that took hours: %v", err)
	}
}

func TestAProofIsAPurposeAndTicketOrAGrant(t *testing.T) {
	values := url.Values{}
	Proof{Purpose: "export fails", Ticket: "ZD-1"}.AddTo(values)
	if proof, err := ProofFrom(values); err != nil || proof.Purpose != "export fails" || proof.Ticket != "ZD-1" || proof.Grant != 0 {
		t.Fatalf("%+v %v", proof, err)
	}
	values = url.Values{}
	Proof{Grant: 12}.AddTo(values)
	if proof, err := ProofFrom(values); err != nil || proof.Grant != 12 || !proof.Given() {
		t.Fatalf("%+v %v", proof, err)
	}
	for _, query := range []string{"access=0", "access=x", "access=3&purpose=x", "access=3&ticket=y"} {
		values, _ := url.ParseQuery(query)
		if _, err := ProofFrom(values); err == nil {
			t.Errorf("%s taken", query)
		}
	}
	if proof, _ := ProofFrom(url.Values{}); proof.Given() {
		t.Fatal("nothing given, yet given")
	}
}

// Accesses written offline wait in their file, readable by root alone,
// until a sync records them; one written during a sync is kept.
func TestOfflineRecordsWaitForASync(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lib", "access-offline.jsonl")
	if records, _, err := ReadOffline(path); err != nil || records != nil {
		t.Fatalf("no file: %v %v", records, err)
	}
	if _, err := WriteOffline(path, Request{Tenant: "acme", Kind: BackupVerify}, "root", "", time.Now()); err == nil {
		t.Fatal("written without a purpose")
	}
	now := time.Date(2026, 11, 7, 10, 0, 0, 0, time.UTC)
	first, err := WriteOffline(path, Request{Kind: State, Purpose: "the cluster is lost", Ticket: "incident"}, "jan via root", "jan", now)
	if err != nil {
		t.Fatal(err)
	}
	second, err := WriteOffline(path, Request{Tenant: "acme", Kind: BackupVerify, Purpose: "drill", Ticket: "incident"}, "root", "", now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("the file: %v %v", info.Mode(), err)
	}
	records, _, err := ReadOffline(path)
	if err != nil || len(records) != 2 || records[0].ID != first.ID || records[0].Operator != "jan" || records[1].Tenant != "acme" {
		t.Fatalf("read: %+v %v", records, err)
	}
	synced := records[0].Synced()
	if synced.Offline != first.ID || !synced.AccessedAt.Equal(now) || synced.Kind != State || synced.Normalize() != nil {
		t.Fatalf("synced as: %+v", synced)
	}
	third, err := WriteOffline(path, Request{Tenant: "acme", Kind: BackupVerify, Purpose: "again", Ticket: "incident"}, "root", "", now)
	if err != nil {
		t.Fatal(err)
	}
	if err := KeepOffline(path, map[string]bool{first.ID: true}); err != nil {
		t.Fatal(err)
	}
	if records, _, _ := ReadOffline(path); len(records) != 2 || records[0].ID != second.ID || records[1].ID != third.ID {
		t.Fatalf("kept: %+v", records)
	}
	if err := KeepOffline(path, map[string]bool{second.ID: true, third.ID: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("the file stays when all are synced: %v", err)
	}
}

// A write cut short (a full disk) leaves a line unended: the next record
// starts on a line of its own and syncs, and the damaged one stays as it
// is, never dropped.
func TestADamagedOfflineLineStaysAndBlocksNothing(t *testing.T) {
	path := filepath.Join(t.TempDir(), "access-offline.jsonl")
	torn := `{"id":"TORN","at":"2026-11-07T10:00:00Z","actor":"root","tenant":"acme","kind":"logs","purp`
	if err := os.WriteFile(path, []byte(torn), 0o600); err != nil {
		t.Fatal(err)
	}
	record, err := WriteOffline(path, Request{Tenant: "acme", Kind: BackupVerify, Purpose: "drill", Ticket: "incident"}, "root", "", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	records, damaged, err := ReadOffline(path)
	if err != nil || len(records) != 1 || records[0].ID != record.ID || len(damaged) != 1 || string(damaged[0]) != torn {
		t.Fatalf("read: %+v %q %v", records, damaged, err)
	}
	if err := KeepOffline(path, map[string]bool{record.ID: true}); err != nil {
		t.Fatal(err)
	}
	if records, damaged, _ := ReadOffline(path); len(records) != 0 || len(damaged) != 1 || string(damaged[0]) != torn {
		t.Fatalf("kept: %+v %q", records, damaged)
	}
	// A line that parses but is no access to record is damaged too.
	if err := os.WriteFile(path, []byte(`{"id":"X","kind":"logs","tenant":"acme"}`+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if records, damaged, _ := ReadOffline(path); len(records) != 0 || len(damaged) != 1 {
		t.Fatalf("a record without a purpose: %+v %q", records, damaged)
	}
}
