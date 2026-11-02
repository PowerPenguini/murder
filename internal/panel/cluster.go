package panel

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
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

// jobs are the panel's recent jobs, newest first.
type jobs struct {
	mu   sync.Mutex
	list []*Job
}

const jobsKept = 20

// startJob runs work in the background, for up to an hour, whatever
// becomes of the request that began it.
func (s *Server) startJob(title, by string, work func(ctx context.Context, out io.Writer) error) *Job {
	id := make([]byte, 8)
	_, _ = rand.Read(id)
	job := &Job{ID: hex.EncodeToString(id), Title: title, By: by, Started: s.now()}
	s.jobs.mu.Lock()
	s.jobs.list = append([]*Job{job}, s.jobs.list...)
	if len(s.jobs.list) > jobsKept {
		s.jobs.list = s.jobs.list[:jobsKept]
	}
	s.jobs.mu.Unlock()
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
		defer cancel()
		err := work(ctx, job)
		job.mu.Lock()
		defer job.mu.Unlock()
		job.Ended = s.now()
		if err != nil {
			job.Failed = err.Error()
			fmt.Fprintf(&job.out, "\nfailed: %v\n", err)
		}
		s.log.Info("a panel job ended", "job", title, "by", by, "failed", job.Failed)
	}()
	return job
}

// Working reports whether a job of the panel is under way: an upgrade
// replaces the panel's own binary, and must still thaw changes after.
func (s *Server) Working() bool {
	s.jobs.mu.Lock()
	defer s.jobs.mu.Unlock()
	for _, job := range s.jobs.list {
		if !job.Done() {
			return true
		}
	}
	return false
}

func (s *Server) job(id string) *Job {
	s.jobs.mu.Lock()
	defer s.jobs.mu.Unlock()
	for _, job := range s.jobs.list {
		if job.ID == id {
			return job
		}
	}
	return nil
}

type nodeRow struct {
	api.NodeState
	State   tag
	Leader  bool
	Version string
	Schema  int
	Started time.Time
}

type clusterData struct {
	Nodes  []nodeRow
	Frozen string
	Jobs   []*Job
	Self   string
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
	s.jobs.mu.Lock()
	data.Jobs = slices.Clone(s.jobs.list)
	s.jobs.mu.Unlock()
	s.render(w, r, "cluster", "Cluster", "cluster", data)
}

// upgradeLimit bounds an uploaded binary.
const upgradeLimit = 256 << 20

func (s *Server) clusterAction(w http.ResponseWriter, r *http.Request) {
	user := s.actor(userOf(r))
	agent := s.Cluster(user)
	r.Body = http.MaxBytesReader(w, r.Body, upgradeLimit+1<<20)
	node := r.FormValue("node")
	confirmed := strings.TrimSpace(r.FormValue("confirm"))
	back := func(kind, message string) {
		flash(w, r, kind, message)
		http.Redirect(w, r, "/cluster", http.StatusSeeOther)
	}
	toJob := func(job *Job) { http.Redirect(w, r, "/jobs/"+url.PathEscape(job.ID), http.StatusSeeOther) }
	ctx, cancel := context.WithTimeout(r.Context(), time.Minute)
	defer cancel()
	switch r.FormValue("do") {
	case "drain":
		to := r.FormValue("to")
		if to == "" || to == node {
			back("problem", "pick the node to move the tenants to")
			return
		}
		toJob(s.startJob("drain "+node+" to "+to, user, func(ctx context.Context, out io.Writer) error {
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
		if err := agent.Thaw(ctx); err != nil {
			back("problem", err.Error())
			return
		}
		back("done", "changes are taken again")
	case "backups":
		toJob(s.startJob("back up the tenants of "+s.nodeName(ctx, agent), user, func(ctx context.Context, out io.Writer) error {
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
	case "upgrade":
		if confirmed != "upgrade" {
			back("problem", "type upgrade: the binary runs as root on every node")
			return
		}
		path, err := s.saveBinary(r)
		if err != nil {
			back("problem", err.Error())
			return
		}
		toJob(s.startJob("upgrade the cluster", user, func(ctx context.Context, out io.Writer) error {
			defer os.Remove(path)
			return clusterops.Upgrade(ctx, agent, path, 5*time.Minute, out)
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

// saveBinary keeps an uploaded binary in a file only root reads, for the
// upgrade, and checks that it is one: it says its version.
func (s *Server) saveBinary(r *http.Request) (string, error) {
	file, header, err := r.FormFile("binary")
	if err != nil {
		return "", errors.New("choose the new murder binary to upload")
	}
	defer file.Close()
	if header.Size > upgradeLimit {
		return "", fmt.Errorf("the binary is %d MB: at most %d MB", header.Size>>20, upgradeLimit>>20)
	}
	temp, err := os.CreateTemp("", "murder-upgrade-*")
	if err != nil {
		return "", err
	}
	_, err = io.Copy(temp, file)
	err = errors.Join(err, temp.Chmod(0o700), temp.Close())
	if err == nil {
		probe, cancel := context.WithTimeout(r.Context(), 30*time.Second)
		defer cancel()
		_, err = upgrade.Probe(probe, temp.Name())
	}
	if err != nil {
		os.Remove(temp.Name())
		return "", fmt.Errorf("not a murder binary: %w", err)
	}
	return temp.Name(), nil
}

type jobData struct{ Job *Job }

func (s *Server) jobPage(w http.ResponseWriter, r *http.Request) {
	job := s.job(r.PathValue("id"))
	if job == nil {
		s.fail(w, r, &api.Error{Status: http.StatusNotFound, Message: "no such job on this node's panel: jobs are kept by the panel that began them, until it restarts"})
		return
	}
	s.render(w, r, "job", job.Title, "cluster", jobData{Job: job})
}
