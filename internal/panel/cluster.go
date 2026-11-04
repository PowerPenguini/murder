package panel

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/clusterops"
	"github.com/PowerPenguini/murder/internal/store"
	"github.com/PowerPenguini/murder/internal/upgrade"
)

// ClusterAgent is what the cluster page asks of the agent, for the person
// signed in.
type ClusterAgent interface {
	clusterops.Client
	Nodes(ctx context.Context) (api.Nodes, error)
	RemoveNode(ctx context.Context, node string) error
	FailOver(ctx context.Context, node string) (store.FailedOver, error)
	RunBackups(ctx context.Context) ([]api.BackupResult, error)
	// ThawIf takes changes again only if they are still frozen for reason.
	ThawIf(ctx context.Context, reason string) error
}

// Job is work that outlasts a request, such as an upgrade: what it says,
// as it goes. Jobs are kept by the panel that began them, on its node.
type Job struct {
	ID, Title, By  string
	Started, Ended time.Time
	Failed         string
	Kind           JobKind

	done chan struct{}

	mu  sync.Mutex
	out bytes.Buffer
}

func (j *Job) Write(data []byte) (int, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.out.Write(data)
}

// Output is what the job has said so far.
func (j *Job) Output() string {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.out.String()
}

// Done reports whether the job ended.
func (j *Job) Done() bool {
	j.mu.Lock()
	defer j.mu.Unlock()
	return !j.Ended.IsZero()
}

// JobKind is what a job does.
type JobKind string

const (
	JobUpgrade JobKind = "upgrade"
	JobDrain   JobKind = "drain"
	JobBackups JobKind = "backups"
	JobBalance JobKind = "balance"
)

// exclusive kinds change the cluster: one runs at a time.
func (k JobKind) exclusive() bool { return k == JobUpgrade || k == JobDrain || k == JobBalance }

// Jobs are a panel's recent jobs, newest first. The socket's panel and
// the tunnel's share them, so that one change of the cluster runs at a time
// whichever began it.
type Jobs struct {
	mu      sync.Mutex
	list    []*Job
	closed  bool
	running sync.WaitGroup
}

// jobsKept bounds the ended jobs kept; running ones are always kept.
const jobsKept = 20

// errClosed refuses a job while the panel restarts.
var errClosed = errors.New("the panel restarts: try again in a moment")

// jobLimit bounds a job, as a safety net only: an upgrade waits for each
// node in turn, and a drain moves every tenant of a node.
const jobLimit = 24 * time.Hour

// Start runs work in the background, whatever becomes of the request that
// began it. An exclusive job, one that changes the cluster, is refused
// while another runs.
func (j *Jobs) Start(kind JobKind, title, by string, now func() time.Time, log *slog.Logger, work func(ctx context.Context, out io.Writer) error) (*Job, error) {
	id := make([]byte, 8)
	_, _ = rand.Read(id)
	job := &Job{ID: hex.EncodeToString(id), Title: title, By: by, Started: now(), Kind: kind, done: make(chan struct{})}
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return nil, errClosed
	}
	if kind.exclusive() {
		if busy := j.busyLocked(); busy != nil {
			return nil, fmt.Errorf("%q runs now, begun by %s: one change of the cluster at a time", busy.Title, busy.By)
		}
	}
	kept := []*Job{job}
	ended := 0
	for _, old := range j.list {
		if old.Done() {
			if ended == jobsKept {
				continue
			}
			ended++
		}
		kept = append(kept, old)
	}
	j.list = kept
	j.running.Add(1)
	go func() {
		defer j.running.Done()
		ctx, cancel := context.WithTimeout(context.Background(), jobLimit)
		defer cancel()
		err := work(ctx, job)
		job.mu.Lock()
		defer job.mu.Unlock()
		defer close(job.done)
		job.Ended = now()
		if err != nil {
			job.Failed = err.Error()
			fmt.Fprintf(&job.out, "\nfailed: %v\n", err)
		}
		log.Info("a panel job ended", "job", title, "by", by, "failed", job.Failed)
	}()
	return job, nil
}

// busyLocked is the exclusive job that runs, if one does.
func (j *Jobs) busyLocked() *Job {
	for _, job := range j.list {
		if job.Kind.exclusive() && !job.Done() {
			return job
		}
	}
	return nil
}

// Busy is the job that changes the cluster now, if one does.
func (j *Jobs) Busy() *Job {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.busyLocked()
}

// CloseIdle takes no more jobs, if none runs, and reports whether it
// closed: a panel replaced by an upgrade restarts only then, and no job
// begins as it does.
func (j *Jobs) CloseIdle() bool {
	j.mu.Lock()
	defer j.mu.Unlock()
	for _, job := range j.list {
		if !job.Done() {
			return false
		}
	}
	j.closed = true
	return true
}

// Close takes no more jobs; Wait waits for those that run.
func (j *Jobs) Close() {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.closed = true
}

// WaitBusy waits until no job changes the cluster; with the jobs closed,
// none begins meanwhile.
func (j *Jobs) WaitBusy() {
	for busy := j.Busy(); busy != nil; busy = j.Busy() {
		<-busy.done
	}
}

// Wait waits until no job runs.
func (j *Jobs) Wait() { j.running.Wait() }

// Running counts the jobs that run.
func (j *Jobs) Running() int {
	j.mu.Lock()
	defer j.mu.Unlock()
	n := 0
	for _, job := range j.list {
		if !job.Done() {
			n++
		}
	}
	return n
}

func (j *Jobs) find(id string) *Job {
	j.mu.Lock()
	defer j.mu.Unlock()
	for _, job := range j.list {
		if job.ID == id {
			return job
		}
	}
	return nil
}

func (j *Jobs) all() []*Job {
	j.mu.Lock()
	defer j.mu.Unlock()
	return slices.Clone(j.list)
}

type nodeRow struct {
	api.NodeState
	// Targets are the nodes it could be drained to: they answer.
	Targets []string
	State   tag
	Leader  bool
	Version string
	Schema  int
	Started time.Time
}

type clusterData struct {
	// Release says what the nodes run; Mixed that they do not all run one.
	Release string
	Mixed   bool
	Nodes   []nodeRow
	Frozen  string
	// Rotating says a master key rotation holds the freeze.
	Rotating bool
	// Busy is the job that changes the cluster now, if one does;
	// Upgrading the upgrade among them, which takes changes again.
	Busy, Upgrading *Job
	Jobs            []*Job
	Self            string
	// Balance are the moves that would even the nodes' tenants.
	Balance []store.Move
	// Backups are each node's backups, in the nodes' order; Reported says
	// any node reported backups at all. Failing are the tenants whose
	// backups fail or are late.
	Backups  []nodeBackupRow
	Reported bool
	Failing  []tenantBackup
	// Tenant is a tenant to show the restore of, the first of this node.
	Tenant string
}

// nodeBackupRow is a node's line in the backups' table.
type nodeBackupRow struct {
	Node string
	// Backups is nil when the node did not report any: not set up, or it
	// did not answer (Answers).
	Backups *nodeBackups
	Answers bool
}

// tenantBackup is a tenant whose backups need a look.
type tenantBackup struct {
	Tenant string
	backupView
}

func (s *Server) cluster(w http.ResponseWriter, r *http.Request) {
	agent := s.Cluster(s.actor(userOf(r)))
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()
	nodes, err := agent.Nodes(ctx)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	data := clusterData{Frozen: nodes.Frozen, Self: nodes.Self, Rotating: strings.HasPrefix(nodes.Frozen, store.RotationFreeze)}
	if moves, err := agent.BalanceMoves(ctx); err == nil {
		data.Balance = moves
	}
	data.Nodes = make([]nodeRow, len(nodes.Nodes))
	var wg sync.WaitGroup
	for i, node := range nodes.Nodes {
		data.Nodes[i] = nodeRow{NodeState: node, State: nodeState(node, s.now()), Leader: node.Name == nodes.Leader}
		for _, other := range nodes.Nodes {
			if other.Name != node.Name && other.Answers && !other.Failed() {
				data.Nodes[i].Targets = append(data.Nodes[i].Targets, other.Name)
			}
		}
		if !node.Answers {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			ask, cancel := context.WithTimeout(ctx, 3*time.Second)
			defer cancel()
			name := node.Name
			if name == nodes.Self {
				name = "-"
			}
			if info, err := agent.NodeInfo(ask, name); err == nil {
				data.Nodes[i].Version, data.Nodes[i].Schema, data.Nodes[i].Started = shortImage("sha256:"+info.Version.Binary), info.Version.Schema, info.Started
			}
		}()
	}
	wg.Wait()
	data.Release, data.Mixed = releaseOf(data.Nodes)
	archived := map[string]bool{}
	if tenants, err := s.backend.List(ctx); err == nil {
		for _, t := range tenants {
			archived[t.Spec.Name] = t.Archived()
		}
	}
	use := s.usageNow(ctx, archived)
	for _, n := range nodes.Nodes {
		row := nodeBackupRow{Node: n.Name, Answers: n.Answers && !slices.Contains(use.Missing, n.Name)}
		if b, ok := use.NodeBackups[n.Name]; ok {
			row.Backups = &b
			data.Reported = true
		}
		data.Backups = append(data.Backups, row)
	}
	for _, name := range slices.Sorted(maps.Keys(use.Backups)) {
		b := use.Backups[name]
		if b.Bad() {
			data.Failing = append(data.Failing, tenantBackup{Tenant: name, backupView: b})
		}
		if data.Tenant == "" && use.Backups[name].Node == nodes.Self {
			data.Tenant = name
		}
	}
	if data.Tenant == "" {
		data.Tenant = "TENANT"
	}
	data.Jobs, data.Busy = s.Jobs.all(), s.Jobs.Busy()
	if data.Busy != nil && data.Busy.Kind == JobUpgrade {
		data.Upgrading = data.Busy
	}
	s.render(w, r, "cluster", "Cluster", "cluster", data)
}

func (s *Server) clusterAction(w http.ResponseWriter, r *http.Request) {
	// Forms of a few fields; an upgrade posts to its own address.
	r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
	user := s.actor(userOf(r))
	agent := s.Cluster(user)
	node := r.FormValue("node")
	confirmed := strings.TrimSpace(r.FormValue("confirm"))
	back := func(kind, message string) {
		flash(w, r, kind, message)
		to := nodeBack(r, node)
		if r.FormValue("do") == "remove" && kind == "done" {
			to = "/cluster"
		}
		http.Redirect(w, r, to, http.StatusSeeOther)
	}
	toJob := func(job *Job, err error) {
		if err != nil {
			back("problem", err.Error())
			return
		}
		http.Redirect(w, r, "/jobs/"+url.PathEscape(job.ID), http.StatusSeeOther)
	}
	ctx, cancel := context.WithTimeout(r.Context(), time.Minute)
	defer cancel()
	switch r.FormValue("do") {
	case "balance":
		toJob(s.Jobs.Start(JobBalance, "balance the tenants", user, s.now, s.log, func(ctx context.Context, out io.Writer) error {
			return clusterops.Balance(ctx, agent, false, out)
		}))
	case "drain":
		to := r.FormValue("to")
		if to == "" || to == node {
			back("problem", "pick the node to move the tenants to")
			return
		}
		toJob(s.Jobs.Start(JobDrain, "drain "+node+" to "+to, user, s.now, s.log, func(ctx context.Context, out io.Writer) error {
			return clusterops.Drain(ctx, agent, node, to, out)
		}))
	case "remove":
		if confirmed != node {
			back("problem", "type the node's name to remove it")
			return
		}
		if err := agent.RemoveNode(ctx, node); err != nil {
			back("problem", err.Error())
			return
		}
		back("done", "node "+node+" left the cluster: switch it off")
	case "failover":
		if confirmed != node {
			back("problem", "type the node's name to say it is powered off")
			return
		}
		outcome, err := agent.FailOver(ctx, node)
		if err != nil {
			back("problem", err.Error())
			return
		}
		back("done", fmt.Sprintf("%d tenants of %s run from their replicas; %d had none and stay", len(outcome.Promoted), node, len(outcome.Stranded)))
	case "thaw":
		if confirmed != "thaw" {
			back("problem", "type thaw to take changes again")
			return
		}
		// The freeze the page showed, and that one alone: one taken since,
		// a rotation's or another upgrade's, stays (checked as the thaw is
		// written).
		frozen := r.FormValue("frozen")
		if frozen == "" {
			back("problem", "the form does not say which freeze to end: look at the page again")
			return
		}
		if strings.HasPrefix(frozen, store.RotationFreeze) {
			back("problem", "a master key rotation holds the freeze: it takes changes again once it ends (sudo murder key rotate goes on with one that stopped)")
			return
		}
		if busy := s.Jobs.Busy(); busy != nil && busy.Kind == JobUpgrade {
			back("problem", fmt.Sprintf("%q runs now: it takes changes again once it ends", busy.Title))
			return
		}
		if err := agent.ThawIf(ctx, frozen); err != nil {
			back("problem", err.Error())
			return
		}
		back("done", "changes are taken again")
	case "backups":
		toJob(s.Jobs.Start(JobBackups, "back up the tenants of "+s.nodeName(ctx, agent), user, s.now, s.log, func(ctx context.Context, out io.Writer) error {
			results, err := agent.RunBackups(ctx)
			failed := 0
			for _, result := range results {
				if result.Error != "" {
					failed++
					fmt.Fprintf(out, "%s: failed: %s\n", result.Label(), result.Error)
				} else {
					fmt.Fprintf(out, "%s: %s\n", result.Label(), result.Key)
				}
			}
			if err == nil && failed > 0 {
				err = fmt.Errorf("%d of %d backups failed", failed, len(results))
			}
			return err
		}))
	default:
		back("problem", "unknown action")
	}
}

func (s *Server) nodeName(ctx context.Context, agent ClusterAgent) string {
	if nodes, err := agent.Nodes(ctx); err == nil {
		return nodes.Self
	}
	return "this node"
}

// upgradeLimit bounds an uploaded binary; a variable for tests.
var upgradeLimit int64 = 256 << 20

// upgradeAction takes a new murder binary and upgrades the cluster with
// it, as a job.
func (s *Server) upgradeAction(w http.ResponseWriter, r *http.Request) {
	user := s.actor(userOf(r))
	r.Body = http.MaxBytesReader(w, r.Body, upgradeLimit+1<<20)
	back := func(message string) {
		// The rest of the upload is read first: a browser shown a reply
		// before it sent its file reports a broken connection instead.
		_, _ = io.Copy(io.Discard, r.Body)
		flash(w, r, "problem", message)
		http.Redirect(w, r, "/cluster", http.StatusSeeOther)
	}
	if busy := s.Jobs.Busy(); busy != nil {
		back(fmt.Sprintf("%q runs now, begun by %s: one change of the cluster at a time", busy.Title, busy.By))
		return
	}
	agent := s.Cluster(user)
	// Jobs are this panel's own: an upgrade begun from another node's
	// panel, or a command line, shows only as its freeze. The panel
	// begins one only with changes taken, and never resumes or takes over
	// another; "murder upgrade" on a node does.
	ask, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	nodes, err := agent.Nodes(ask)
	cancel()
	switch {
	case err != nil:
		back(err.Error())
		return
	case strings.HasPrefix(nodes.Frozen, store.UpgradeFreeze):
		back("changes are frozen for an upgrade (" + nodes.Frozen + "), which may still run from another node: the panel begins no upgrade until it ends; to finish one that stopped, run murder upgrade again on the node it began on")
		return
	case nodes.Frozen != "":
		back("changes are frozen (" + nodes.Frozen + "): the panel begins no upgrade until they are taken again")
		return
	}
	path, err := s.receiveBinary(r)
	if err != nil {
		back(err.Error())
		return
	}
	probe, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	_, err = upgrade.Probe(probe, path)
	cancel()
	if err != nil {
		os.Remove(path)
		back("the file did not tell its murder version, so it is not used: " + err.Error())
		return
	}
	job, err := s.Jobs.Start(JobUpgrade, "upgrade the cluster", user, s.now, s.log, func(ctx context.Context, out io.Writer) error {
		defer os.Remove(path)
		return clusterops.Upgrade(ctx, agent, path, 5*time.Minute, out)
	})
	if err != nil {
		os.Remove(path)
		back(err.Error())
		return
	}
	http.Redirect(w, r, "/jobs/"+url.PathEscape(job.ID), http.StatusSeeOther)
}

// receiveBinary keeps an uploaded binary in a file only root reads, in
// the panel's upload directory. The form says "upgrade" before the file,
// so a file without it is never kept.
func (s *Server) receiveBinary(r *http.Request) (string, error) {
	reader, err := r.MultipartReader()
	if err != nil {
		return "", errors.New("choose the new murder binary to upload")
	}
	confirmed := false
	for {
		part, err := reader.NextPart()
		if errors.Is(err, io.EOF) {
			return "", errors.New("choose the new murder binary to upload")
		}
		if err != nil {
			return "", uploadError(err)
		}
		switch part.FormName() {
		case "confirm":
			value, _ := io.ReadAll(io.LimitReader(part, 64))
			confirmed = strings.TrimSpace(string(value)) == "upgrade"
		case "binary":
			if !confirmed {
				return "", errors.New("type upgrade, before the file: the binary runs as root on every node")
			}
			if part.FileName() == "" {
				return "", errors.New("choose the new murder binary to upload")
			}
			return s.keepBinary(part)
		}
	}
}

func (s *Server) keepBinary(data io.Reader) (string, error) {
	dir := s.UploadDir
	if dir == "" {
		dir = os.TempDir()
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", err
	}
	file, err := os.CreateTemp(dir, "murder-upgrade-*")
	if err != nil {
		return "", err
	}
	n, err := io.Copy(file, io.LimitReader(data, upgradeLimit+1))
	switch {
	case err != nil:
		err = uploadError(err)
	case n == 0:
		err = errors.New("the file is empty: choose the new murder binary")
	case n > upgradeLimit:
		err = uploadError(&http.MaxBytesError{Limit: upgradeLimit})
	}
	err = errors.Join(err, file.Chmod(0o700), file.Close())
	if err != nil {
		os.Remove(file.Name())
		return "", err
	}
	return file.Name(), nil
}

func uploadError(err error) error {
	if errors.As(err, new(*http.MaxBytesError)) {
		return fmt.Errorf("the binary is larger than %d MB", upgradeLimit>>20)
	}
	return fmt.Errorf("the upload broke off: %w", err)
}

type jobData struct{ Job *Job }

func (s *Server) jobPage(w http.ResponseWriter, r *http.Request) {
	job := s.Jobs.find(r.PathValue("id"))
	if job == nil {
		s.fail(w, r, &api.Error{Status: http.StatusNotFound, Message: "no such job on this node's panel: jobs are kept by the panel that began them, until it restarts"})
		return
	}
	s.render(w, r, "job", job.Title, "cluster", jobData{Job: job})
}

// releaseOf says which release the nodes that answered run: one, or that
// they differ, and which runs what, as while an upgrade goes on.
func releaseOf(nodes []nodeRow) (string, bool) {
	runs := map[string][]string{}
	var order []string
	for _, n := range nodes {
		if n.Version == "" {
			continue
		}
		release := n.Version + " (schema " + strconv.Itoa(n.Schema) + ")"
		if _, ok := runs[release]; !ok {
			order = append(order, release)
		}
		runs[release] = append(runs[release], n.Name)
	}
	switch len(order) {
	case 0:
		return "", false
	case 1:
		if len(runs[order[0]]) == 1 {
			return "It runs release " + order[0] + ".", false
		}
		return "Every node that answers runs release " + order[0] + ".", false
	}
	var parts []string
	for _, release := range order {
		parts = append(parts, strings.Join(runs[release], ", ")+" run "+release)
	}
	return "The nodes run different releases: " + strings.Join(parts, "; ") + ". An upgrade that stopped leaves them so: run it again to finish it.", true
}
