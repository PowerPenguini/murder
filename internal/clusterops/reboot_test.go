package clusterops

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/cluster"
	"github.com/PowerPenguini/murder/internal/reboot"
	"github.com/PowerPenguini/murder/internal/spec"
	"github.com/PowerPenguini/murder/internal/store"
	"github.com/PowerPenguini/murder/internal/upgrade"
)

// rebootAgent is fakeAgent with nodes that answer or not, and reboot.
type rebootAgent struct {
	*fakeAgent
	// silent nodes do not answer; failed ones were failed over; away ones
	// restart until the time given.
	silent map[string]bool
	failed map[string]bool
	away   map[string]time.Time
	// notReady nodes refuse to reboot, saying why; old ones run a release
	// that takes no reboots.
	notReady map[string]string
	old      map[string]bool
	// started is when each node's agent started; a reboot starts it anew.
	// downFor is how many answers a rebooted node fails before it is
	// back; neverBack nodes do not come back.
	started   map[string]time.Time
	downFor   map[string]int
	neverBack map[string]bool
	// onWait runs as the reboot waits for a node, once.
	onWait func()
	// events are what changed, in order: "move a n3", "reboot n2".
	events   []string
	checked  []string
	awayFor  time.Duration
	rebooted bool
}

func newRebootAgent(members ...string) *rebootAgent {
	running := map[string]string{}
	for _, member := range members {
		running[member] = "v1"
	}
	a := &rebootAgent{fakeAgent: newFakeAgent(running), started: map[string]time.Time{}, downFor: map[string]int{}}
	for _, member := range members {
		a.started[member] = time.Now().Add(-time.Hour)
	}
	return a
}

func (a *rebootAgent) ClusterStatus(ctx context.Context) (cluster.Status, error) {
	status, err := a.fakeAgent.ClusterStatus(ctx)
	for i := range status.Members {
		status.Members[i].Suffrage = "Voter"
	}
	return status, err
}

func (a *rebootAgent) Nodes(ctx context.Context) (api.Nodes, error) {
	view, err := a.fakeAgent.Nodes(ctx)
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, name := range a.members {
		state := api.NodeState{Node: store.Node{Name: name, AwayUntil: a.away[name]}, Answers: !a.silent[name] && !a.failed[name]}
		if a.failed[name] {
			state.FailedAt = time.Now().Add(-time.Minute)
		}
		view.Nodes = append(view.Nodes, state)
	}
	return view, err
}

func (a *rebootAgent) NodeInfo(_ context.Context, name string) (upgrade.Info, error) {
	a.mu.Lock()
	node := a.node(name)
	wait := a.rebooted && a.onWait != nil
	onWait := a.onWait
	if wait {
		a.onWait = nil
	}
	a.mu.Unlock()
	if wait {
		onWait()
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.downFor[node] > 0 || a.neverBack[node] && a.rebooted {
		a.downFor[node]--
		return upgrade.Info{}, &api.Error{Status: http.StatusBadGateway, Message: "node " + node + " does not answer"}
	}
	started := a.started[node]
	return upgrade.Info{Node: node, Started: started, Applied: a.applied, LastPass: started.Add(time.Second)}, nil
}

func (a *rebootAgent) Reboot(_ context.Context, name string, request reboot.Request) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	node := a.node(name)
	if a.old[node] {
		return &api.Error{Status: http.StatusNotFound, Message: "404 page not found"}
	}
	if problem, ok := a.notReady[node]; ok {
		return &api.Error{Status: http.StatusUnprocessableEntity, Message: (&reboot.NotReadyError{Problems: []string{problem}}).Error()}
	}
	if request.Check {
		a.checked = append(a.checked, node)
		return nil
	}
	a.events = append(a.events, "reboot "+node)
	a.awayFor, a.rebooted = request.Away, true
	// It comes back a moment later, an agent started anew.
	a.started[node] = time.Now()
	a.downFor[node] += 2
	return nil
}

func (a *rebootAgent) Move(ctx context.Context, tenant, to string) (api.Tenant, error) {
	moved, err := a.fakeAgent.Move(ctx, tenant, to)
	if err == nil {
		a.mu.Lock()
		a.events = append(a.events, "move "+tenant+" "+to)
		a.mu.Unlock()
	}
	return moved, err
}

func runningOn(name, node string) api.Tenant {
	return api.Tenant{Spec: spec.Tenant{Name: name}, Node: node}
}

// The tenants move away, the node reboots, and once it is back and ready
// they move back to it: each, by default, to the node keeping its copy, or
// to the one running the fewest.
func TestARebootMovesTheTenantsAwayAndBack(t *testing.T) {
	agent := newRebootAgent("n1", "n2", "n3")
	copied := runningOn("a", "n2")
	copied.Replica, copied.ReplicaReady = "n1", true
	archived := runningOn("c", "n2")
	archived.ArchivedAt = time.Now()
	agent.tenants = []api.Tenant{copied, runningOn("b", "n2"), archived, runningOn("d", "n1"), runningOn("e", "n1")}
	var out bytes.Buffer
	if err := Reboot(context.Background(), agent, "n2", RebootOptions{Wait: 20 * time.Second}, &out); err != nil {
		t.Fatalf("%v\n%s", err, out.String())
	}
	want := []string{"move a n1", "move b n3", "move c n3", "reboot n2", "move a n2", "move b n2", "move c n2"}
	if !slices.Equal(agent.events, want) {
		t.Fatalf("events %v, want %v", agent.events, want)
	}
	// Checked before anything moved; marked away for as long as the wait.
	if !slices.Equal(agent.checked, []string{"n2"}) || agent.awayFor != 20*time.Second {
		t.Fatalf("checked %v, away for %v", agent.checked, agent.awayFor)
	}
	for _, tenant := range agent.tenants[:3] {
		if tenant.Node != "n2" {
			t.Fatalf("%s runs on %s after the reboot", tenant.Spec.Name, tenant.Node)
		}
	}
	for _, line := range []string{"a moved to n1", "node n2 reboots; waiting", "node n2 is back", "b moved back to n2", "node n2 rebooted and runs its tenants again (3 moved back)"} {
		if !strings.Contains(out.String(), line) {
			t.Fatalf("output lacks %q:\n%s", line, out.String())
		}
	}
}

func TestARebootMovesEveryTenantToTheNodeNamed(t *testing.T) {
	agent := newRebootAgent("n1", "n2", "n3")
	copied := runningOn("a", "n2")
	copied.Replica, copied.ReplicaReady = "n1", true
	agent.tenants = []api.Tenant{copied, runningOn("b", "n2")}
	if err := Reboot(context.Background(), agent, "n2", RebootOptions{To: "n3", Stay: true, Wait: 20 * time.Second}, io.Discard); err != nil {
		t.Fatal(err)
	}
	// -stay leaves them there once the node is back.
	if want := []string{"move a n3", "move b n3", "reboot n2"}; !slices.Equal(agent.events, want) {
		t.Fatalf("events %v, want %v", agent.events, want)
	}
}

// Run on the node it reboots, the command cannot wait for it: it needs
// -stay, and returns once the node reboots.
func TestTheNodeThatRunsTheCommandRebootsOnlyWithStay(t *testing.T) {
	agent := newRebootAgent("n1", "n2", "n3")
	agent.tenants = []api.Tenant{runningOn("a", "n1")}
	err := Reboot(context.Background(), agent, "", RebootOptions{}, io.Discard)
	if err == nil || !strings.Contains(err.Error(), `run "murder node reboot n1" on another node`) || len(agent.events) != 0 {
		t.Fatalf("reboot: %v, events %v", err, agent.events)
	}
	var out bytes.Buffer
	if err := Reboot(context.Background(), agent, "-", RebootOptions{Stay: true}, &out); err != nil {
		t.Fatal(err)
	}
	if want := []string{"move a n2", "reboot n1"}; !slices.Equal(agent.events, want) {
		t.Fatalf("events %v, want %v", agent.events, want)
	}
	if !strings.Contains(out.String(), "node n1 reboots now; the 1 tenants moved from n1 run a on n2") {
		t.Fatalf("output:\n%s", out.String())
	}
}

// What cannot go through is refused before anything changes.
func TestARebootThatCannotGoThroughChangesNothing(t *testing.T) {
	for _, c := range []struct {
		name  string
		set   func(*rebootAgent)
		opts  RebootOptions
		wants string
	}{
		{"frozen", func(a *rebootAgent) { a.frozen = store.UpgradeFreeze + "abc" }, RebootOptions{}, "changes are frozen (upgrade to abc)"},
		{"silent node", func(a *rebootAgent) { a.silent = map[string]bool{"n2": true} }, RebootOptions{}, "node n2 does not answer"},
		{"no majority left", func(a *rebootAgent) { a.silent = map[string]bool{"n3": true} }, RebootOptions{}, "1 of the cluster's 3 voters answer, not a majority"},
		{"units off", func(a *rebootAgent) { a.notReady = map[string]string{"n2": "murder-edge.service is disabled"} }, RebootOptions{}, "node n2 refuses to reboot: the node would not come back as it runs now, so it does not reboot: murder-edge.service is disabled"},
		{"old release", func(a *rebootAgent) { a.old = map[string]bool{"n2": true} }, RebootOptions{}, "upgrade it first"},
		{"target is the node", func(*rebootAgent) {}, RebootOptions{To: "n2"}, "cannot move to itself"},
		{"target silent", func(a *rebootAgent) { a.silent = map[string]bool{"n4": true} }, RebootOptions{To: "n4"}, "node n4 cannot take the tenants: it does not answer"},
		{"target failed", func(a *rebootAgent) { a.failed = map[string]bool{"n4": true} }, RebootOptions{To: "n4"}, "it was failed over"},
		{"target away", func(a *rebootAgent) { a.away = map[string]time.Time{"n4": time.Now().Add(time.Hour)} }, RebootOptions{To: "n4"}, "it restarts"},
		{"target unknown", func(*rebootAgent) {}, RebootOptions{To: "n9"}, "it is no member"},
		{"no target", func(a *rebootAgent) {
			a.away = map[string]time.Time{}
			for _, node := range []string{"n1", "n3", "n4", "n5"} {
				a.away[node] = time.Now().Add(time.Hour)
			}
		}, RebootOptions{}, "no other node can take the tenants of node n2"},
		{"no such node", func(a *rebootAgent) { a.members = []string{"n1", "n3", "n4", "n5"} }, RebootOptions{}, "node n2 is no member"},
	} {
		t.Run(c.name, func(t *testing.T) {
			agent := newRebootAgent("n1", "n2", "n3", "n4", "n5")
			if c.name == "no majority left" {
				agent = newRebootAgent("n1", "n2", "n3")
			}
			agent.tenants = []api.Tenant{runningOn("a", "n2")}
			c.set(agent)
			c.opts.Wait = time.Second
			err := Reboot(context.Background(), agent, "n2", c.opts, io.Discard)
			if err == nil || !strings.Contains(err.Error(), c.wants) {
				t.Fatalf("reboot: %v, want %q", err, c.wants)
			}
			if len(agent.events) != 0 {
				t.Fatalf("changed %v all the same", agent.events)
			}
		})
	}
}

// A cluster of two loses its majority while one reboots: only -force
// reboots it, saying what that does.
func TestAClusterOfTwoRebootsOnlyWithForce(t *testing.T) {
	agent := newRebootAgent("n1", "n2")
	agent.tenants = []api.Tenant{runningOn("a", "n2")}
	err := Reboot(context.Background(), agent, "n2", RebootOptions{Wait: time.Second}, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "add -force") || len(agent.events) != 0 {
		t.Fatalf("reboot: %v, events %v", err, agent.events)
	}
	var out bytes.Buffer
	if err := Reboot(context.Background(), agent, "n2", RebootOptions{Force: true, Wait: 20 * time.Second}, &out); err != nil {
		t.Fatal(err)
	}
	if want := []string{"move a n1", "reboot n2", "move a n2"}; !slices.Equal(agent.events, want) {
		t.Fatalf("events %v, want %v", agent.events, want)
	}
	if !strings.Contains(out.String(), "rebooting all the same (-force)") {
		t.Fatalf("output:\n%s", out.String())
	}
}

func TestANodeAloneIsForTheCaller(t *testing.T) {
	agent := newRebootAgent("solo")
	agent.self, agent.alone = "solo", true
	if err := Reboot(context.Background(), agent, "", RebootOptions{}, io.Discard); !errors.Is(err, ErrAlone) {
		t.Fatalf("outside a cluster: %v", err)
	}
	agent.alone = false
	if err := Reboot(context.Background(), agent, "", RebootOptions{Stay: true}, io.Discard); !errors.Is(err, ErrAlone) {
		t.Fatalf("in a cluster of one: %v", err)
	}
	if len(agent.events) != 0 {
		t.Fatalf("changed %v", agent.events)
	}
}

// A tenant that does not move stops the reboot there: the node is not
// rebooted, and the error says where the moved ones run.
func TestARebootStopsAtATenantThatDoesNotMove(t *testing.T) {
	agent := newRebootAgent("n1", "n2", "n3")
	agent.tenants = []api.Tenant{runningOn("a", "n2"), runningOn("b", "n2")}
	agent.failMove = map[string]bool{"b": true}
	err := Reboot(context.Background(), agent, "n2", RebootOptions{To: "n3", Wait: time.Second}, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "move b to n3: the copy did not arrive") || !strings.Contains(err.Error(), "node n2 was not rebooted; the 1 tenants moved from n2 run a on n3") {
		t.Fatalf("reboot: %v", err)
	}
	if slices.Contains(agent.events, "reboot n2") {
		t.Fatalf("rebooted all the same: %v", agent.events)
	}
}

// A node that does not come back leaves its tenants where they moved, and
// says how to move them back.
func TestANodeThatDoesNotComeBackKeepsItsTenantsAway(t *testing.T) {
	agent := newRebootAgent("n1", "n2", "n3")
	agent.tenants = []api.Tenant{runningOn("a", "n2")}
	agent.neverBack = map[string]bool{"n2": true}
	err := Reboot(context.Background(), agent, "n2", RebootOptions{To: "n3", Wait: 2 * time.Second}, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "node n2 did not come back ready within 2s") || !strings.Contains(err.Error(), "a on n3") || !strings.Contains(err.Error(), `murder tenant move NAME -to n2`) {
		t.Fatalf("reboot: %v", err)
	}
	if want := []string{"move a n3", "reboot n2"}; !slices.Equal(agent.events, want) {
		t.Fatalf("events %v, want %v", agent.events, want)
	}
}

// Back means an agent started since the reboot, caught up and reconciled:
// a node that answers as before, from before it rebooted, is not back.
func TestTheRebootWaitsForTheAgentStartedAnew(t *testing.T) {
	agent := newRebootAgent("n1", "n2", "n3")
	agent.tenants = []api.Tenant{runningOn("a", "n2")}
	before := agent.started["n2"]
	// The node still answers as it ran before, until the wait has looked
	// twice.
	looks := 0
	client := lookingRebootAgent{agent, func() {
		agent.mu.Lock()
		defer agent.mu.Unlock()
		if !agent.rebooted {
			return
		}
		looks++
		if looks == 1 {
			agent.downFor["n2"] = 0
			agent.started["n2"], before = before, agent.started["n2"]
		}
		if looks == 3 {
			agent.started["n2"] = before
		}
	}}
	if err := Reboot(context.Background(), client, "n2", RebootOptions{Wait: 20 * time.Second}, io.Discard); err != nil {
		t.Fatal(err)
	}
	if looks < 3 {
		t.Fatalf("back after %d looks, while it still ran as before", looks)
	}
}

// lookingRebootAgent runs look before each NodeInfo.
type lookingRebootAgent struct {
	*rebootAgent
	look func()
}

func (a lookingRebootAgent) NodeInfo(ctx context.Context, node string) (upgrade.Info, error) {
	a.look()
	return a.rebootAgent.NodeInfo(ctx, node)
}

// A tenant moved on while the node rebooted stays where it is now.
func TestATenantMovedOnMeanwhileStays(t *testing.T) {
	agent := newRebootAgent("n1", "n2", "n3")
	agent.tenants = []api.Tenant{runningOn("a", "n2"), runningOn("b", "n2")}
	agent.onWait = func() {
		agent.mu.Lock()
		defer agent.mu.Unlock()
		agent.tenants[0].Node = "n1"
	}
	var out bytes.Buffer
	if err := Reboot(context.Background(), agent, "n2", RebootOptions{To: "n3", Wait: 20 * time.Second}, &out); err != nil {
		t.Fatal(err)
	}
	if want := []string{"move a n3", "move b n3", "reboot n2", "move b n2"}; !slices.Equal(agent.events, want) {
		t.Fatalf("events %v, want %v", agent.events, want)
	}
	if !strings.Contains(out.String(), "a stays: it is no longer on n3") || !strings.Contains(out.String(), "(1 moved back)") {
		t.Fatalf("output:\n%s", out.String())
	}
}

// A tenant that does not move back stops the return there, saying where
// the rest run.
func TestAReturnStopsAtATenantThatDoesNotMoveBack(t *testing.T) {
	agent := newRebootAgent("n1", "n2", "n3")
	agent.tenants = []api.Tenant{runningOn("a", "n2"), runningOn("b", "n2")}
	agent.onWait = func() {
		agent.mu.Lock()
		defer agent.mu.Unlock()
		agent.failMove = map[string]bool{"a": true}
	}
	err := Reboot(context.Background(), agent, "n2", RebootOptions{To: "n3", Wait: 20 * time.Second}, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "move a back to n2") || !strings.Contains(err.Error(), "the 2 tenants moved from n2 run a, b on n3") {
		t.Fatalf("reboot: %v", err)
	}
}

// Each tenant goes to the node keeping its whole copy, if that can take
// it, else to the one running the fewest as the moves go.
func TestADrainPlanSpreadsTheTenants(t *testing.T) {
	ready := func(name, replica string) api.Tenant {
		tenant := runningOn(name, "n1")
		tenant.Replica, tenant.ReplicaReady = replica, true
		return tenant
	}
	suspended := runningOn("s", "n1")
	suspended.Spec.Suspended = true
	unready := runningOn("u", "n1")
	unready.Replica = "n2"
	theirs := []api.Tenant{ready("a", "n4"), runningOn("b", "n1"), runningOn("c", "n1"), unready, suspended, ready("d", "n9")}
	all := append(slices.Clone(theirs), runningOn("x", "n2"), runningOn("y", "n2"), runningOn("z", "n3"))
	plan := planDrain(theirs, all, []string{"n2", "n3", "n4"}, "")
	var got []string
	for _, move := range plan {
		got = append(got, fmt.Sprintf("%s>%s", move.tenant, move.to))
	}
	// n2 runs 2, n3 1, n4 0: a goes to its copy on n4 (1), b to n3 or n4
	// (first by name, n3: 2), c to n4 (2), u to n2 (3: its copy is not
	// whole), s, suspended, counts nowhere, d's copy is on no target.
	want := []string{"a>n4", "b>n3", "c>n4", "u>n2", "s>n3", "d>n3"}
	if !slices.Equal(got, want) {
		t.Fatalf("plan %v, want %v", got, want)
	}
	if plan := planDrain(theirs[:2], all, []string{"n2", "n3"}, "n2"); plan[0].to != "n2" || plan[1].to != "n2" {
		t.Fatalf("to n2: %v", plan)
	}
}
