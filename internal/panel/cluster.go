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
	"net/http"
	"net/url"
	"os"
	"slices"
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
}

// Job is work that outlasts a request, such as an upgrade: what it says,
// as it goes. Jobs are kept by the panel that began them, on its node.
type Job struct {
	ID, Title, By  string
	Started, Ended time.Time
	Failed         string
	// exclusive jobs change the cluster: one runs at a time.
	exclusive bool

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
func (j *Jobs) Start(title, by string, exclusive bool, now func() time.Time, log *slog.Logger, work func(ctx context.Context, out io.Writer) error) (*Job, error) {
	id := make([]byte, 8)
	_, _ = rand.Read(id)
	job := &Job{ID: hex.EncodeToString(id), Title: title, By: by, Started: now(), exclusive: exclusive}
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return nil, errClosed
	}
	if exclusive {
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
		if job.exclusive && !job.Done() {
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
	Nodes  []nodeRow
	Frozen string
	// Busy is the job that changes the cluster now, if one does.
	Busy *Job
	Jobs []*Job
	Self string
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
	data := clusterData{Frozen: nodes.Frozen, Self: nodes.Self}
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
	data.Jobs, data.Busy = s.Jobs.all(), s.Jobs.Busy()
	s.render(w, r, "cluster", "Cluster", "cluster", data)
}

func (s *Server) clusterAction(w http.ResponseWriter, r *http.Request) {
	user := s.actor(userOf(r))
	agent := s.Cluster(user)
	node := r.FormValue("node")
	confirmed := strings.TrimSpace(r.FormValue("confirm"))
	back := func(kind, message string) {
		flash(w, r, kind, message)
		http.Redirect(w, r, "/cluster", http.StatusSeeOther)
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
	case "drain":
		to := r.FormValue("to")
		if to == "" || to == node {
			back("problem", "pick the node to move the tenants to")
			return
		}
		toJob(s.Jobs.Start("drain "+node+" to "+to, user, true, s.now, s.log, func(ctx context.Context, out io.Writer) error {
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
		if busy := s.Jobs.Busy(); busy != nil {
			back("problem", fmt.Sprintf("%q runs now: it takes changes again once it ends", busy.Title))
			return
		}
		if err := agent.Thaw(ctx); err != nil {
			back("problem", err.Error())
			return
		}
		back("done", "changes are taken again")
	case "backups":
		toJob(s.Jobs.Start("back up the tenants of "+s.nodeName(ctx, agent), user, false, s.now, s.log, func(ctx context.Context, out io.Writer) error {
			results, err := agent.RunBackups(ctx)
			for _, result := range results {
				if result.Error != "" {
					fmt.Fprintf(out, "%s: failed: %s\n", result.Tenant, result.Error)
				} else {
					fmt.Fprintf(out, "%s: %s\n", result.Tenant, result.Key)
				}
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
	back := func(message string) {
		flash(w, r, "problem", message)
		http.Redirect(w, r, "/cluster", http.StatusSeeOther)
	}
	if busy := s.Jobs.Busy(); busy != nil {
		back(fmt.Sprintf("%q runs now, begun by %s: one change of the cluster at a time", busy.Title, busy.By))
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, upgradeLimit+1<<20)
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
	agent := s.Cluster(user)
	job, err := s.Jobs.Start("upgrade the cluster", user, true, s.now, s.log, func(ctx context.Context, out io.Writer) error {
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
				return "", errors.New("type upgrade: the binary runs as root on every node")
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
