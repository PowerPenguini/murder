// Package replicate keeps a copy of every tenant's data on a second node,
// its replica, to run the tenant from if its own node fails. The node that
// runs a tenant snapshots its data every few seconds and sends what changed
// since the copy's newest snapshot; the replica's node takes it into a
// dataset that is never mounted, and refuses data from any node but the
// one that runs the tenant, so a node that lost its tenants never writes
// over the copy that took them over.
package replicate

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/PowerPenguini/murder/internal/metrics"
	"github.com/PowerPenguini/murder/internal/store"
	"github.com/PowerPenguini/murder/internal/volume"
)

// Volumes is what replication does with this node's data.
type Volumes interface {
	volume.Replicas
	Snapshot(ctx context.Context, tenant, reason string) (string, error)
	Prune(ctx context.Context, tenant, reason string, keep int) error
}

// Peer reaches the node that keeps a tenant's copy.
type Peer interface {
	// Snapshots lists the snapshots of the copy node keeps, oldest first.
	Snapshots(ctx context.Context, node, tenant string) ([]string, error)
	// Send streams the tenant's data to the copy node keeps: whole, or
	// what changed since the copy's newest snapshot.
	Send(ctx context.Context, node, tenant string, whole bool, data io.Reader) error
}

// Replicator sends this node's tenants to their replicas, and keeps the
// copies this node holds for other nodes as the state says.
type Replicator struct {
	Store   State
	Volumes Volumes
	Peer    Peer
	Log     *slog.Logger
	// CatchUp applies here what the cluster has applied; copies and data
	// are only dropped or put aside on a state no older than the cluster's.
	CatchUp func() error
	// Moving reports a tenant a move on this node is handling: its data is
	// the move's to remove.
	Moving func(tenant string) bool
	// Leader reports whether this node leads the cluster; the leader gives
	// tenants their replicas.
	Leader func() bool
	// Every is how often each tenant's changes are sent; 15 s if zero.
	Every time.Duration
	// Parallel bounds the tenants sent at once; 4 if zero.
	Parallel int
	// Keep is how many replica snapshots each side keeps; 3 if zero.
	Keep int
	// Stall fails a send whose stream moves nothing for this long, or whose
	// other node answers nothing for this long once it is all sent; 2
	// minutes if zero.
	Stall time.Duration
	Now   func() time.Time

	mu sync.Mutex
	// synced is when each tenant's copy last matched its data, failing
	// why its last send failed.
	synced  map[string]time.Time
	failing map[string]string
	// copyFailures counts, by tenant and node, the failed sends in a row
	// of copies not whole yet.
	copyFailures map[string]int
	// caughtUp is set, by Run only, once this node first caught up.
	caughtUp bool
	// busy holds, by tenant and node, the copies a send runs for;
	// changeSlots and wholeSlots bound the sends of copies whole and not.
	busy                    map[string]bool
	changeSlots, wholeSlots chan struct{}
	// moves counts, by tenant, the copies moving to another node now, and
	// movesSeen every start and end of one.
	moves     map[string]int
	movesSeen map[string]uint64
	// snapshotting takes each tenant's snapshots one at a time.
	snapshotting map[string]*sync.Mutex
	// running waits for every send under way.
	running sync.WaitGroup
	// sending counts each tenant's sends under way, with their reports;
	// held, the holds keeping its sends off (see Hold). ended is closed,
	// and replaced, whenever a send ends.
	sending map[string]int
	held    map[string]int
	ended   chan struct{}
}

// Hold keeps tenant's data from being sent, or reported whole, until
// release is called: for a replacement of the data, after which no copy
// matches it. It first waits, or until ctx ends, for the sends of tenant
// under way, and their reports, to end: a send begun before cannot report
// the old data whole once the replacement began; a copy moving to another
// node ends with the round of changes under way. A tenant held is left out
// of sends, without counting as a failure.
func (r *Replicator) Hold(ctx context.Context, tenant string) (release func(), err error) {
	r.mu.Lock()
	if r.held == nil {
		r.held = map[string]int{}
	}
	r.held[tenant]++
	var once sync.Once
	release = func() {
		once.Do(func() {
			r.mu.Lock()
			if r.held[tenant]--; r.held[tenant] == 0 {
				delete(r.held, tenant)
			}
			r.mu.Unlock()
		})
	}
	for r.sending[tenant] > 0 {
		if r.ended == nil {
			r.ended = make(chan struct{})
		}
		ended := r.ended
		r.mu.Unlock()
		select {
		case <-ended:
		case <-ctx.Done():
			release()
			return nil, ctx.Err()
		}
		r.mu.Lock()
	}
	r.mu.Unlock()
	return release, nil
}

// begin starts a send of tenant, unless a hold keeps it off; end ends it.
func (r *Replicator) begin(tenant string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.held[tenant] > 0 {
		return false
	}
	if r.sending == nil {
		r.sending = map[string]int{}
	}
	r.sending[tenant]++
	return true
}

func (r *Replicator) end(tenant string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.sending[tenant]--; r.sending[tenant] == 0 {
		delete(r.sending, tenant)
	}
	if r.ended != nil {
		close(r.ended)
		r.ended = nil
	}
}

// isHeld reports whether a hold keeps tenant's sends off.
func (r *Replicator) isHeld(tenant string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.held[tenant] > 0
}

// ErrHeld is the error of a send of a tenant whose data a replacement
// holds.
var ErrHeld = errors.New("its data is being replaced")

// ReplicaReason names the snapshots replication takes.
const ReplicaReason = volume.ReplicaReason

func (r *Replicator) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

func (r *Replicator) stall() time.Duration {
	if r.Stall > 0 {
		return r.Stall
	}
	return 2 * time.Minute
}

func (r *Replicator) every() time.Duration {
	if r.Every > 0 {
		return r.Every
	}
	return 15 * time.Second
}

func (r *Replicator) keep() int {
	if r.Keep > 0 {
		return r.Keep
	}
	return 3
}

// Run replicates until ctx ends, and returns once every send it began
// ended.
func (r *Replicator) Run(ctx context.Context) {
	defer r.running.Wait()
	every := r.every()
	for {
		r.step(ctx)
		select {
		case <-ctx.Done():
			return
		case <-time.After(every):
		}
	}
}

// step is one round of Run: it begins the round's sends, whose wait it
// returns, and does not wait for them. Nothing is done before this node
// has once caught up with the cluster: a node back from the dead would
// otherwise send, from its old state, tenants that have since run
// elsewhere.
func (r *Replicator) step(ctx context.Context) *sync.WaitGroup {
	if !r.caughtUp {
		if r.CatchUp != nil {
			if err := r.CatchUp(); err != nil {
				r.Log.Debug("wait for the cluster before replicating", "error", err)
				return &sync.WaitGroup{}
			}
		}
		r.caughtUp = true
	}
	// An upgrade's freeze refuses changes: placement waits for the thaw.
	if r.Leader != nil && r.Leader() && r.Store.Frozen() == "" {
		if placed, err := r.Store.PlaceReplicas("murder"); err != nil {
			r.Log.Warn("give tenants their replicas", "error", err)
		} else if placed > 0 {
			r.Log.Info("gave tenants their replicas", "tenants", placed)
		}
	}
	started := r.start(ctx)
	if err := r.Sweep(ctx); err != nil && ctx.Err() == nil {
		r.Log.Warn("tidy the copies on this node", "error", err)
	}
	return started
}

// Pass sends every tenant of this node with a replica what changed since
// its copy's newest snapshot, and waits for the sends. A tenant moving to
// its replica's node waits: the move brings that copy up to date. A copy
// moving to another node (ReplicaNext) is sent there too, and replaces the
// replica once whole.
func (r *Replicator) Pass(ctx context.Context) { r.start(ctx).Wait() }

// start begins the sends of a pass and returns what waits for them. Each
// copy, a tenant's on one node, has one send at a time: one still under way,
// as a whole copy that takes hours, is left to finish, and the copies of
// other tenants, and the tenant's replica while its copy moves, go on
// without it. Sends of copies not whole yet, a replica's first and those
// moving, take slots of their own, so they never hold up the changes of
// ready replicas.
func (r *Replicator) start(ctx context.Context) *sync.WaitGroup {
	var tenants, moving []store.Record
	current := map[string]bool{}
	for _, record := range r.Store.List() {
		if record.Node != r.Store.Node() {
			continue
		}
		if record.Replica != "" && record.Replica != record.Node && record.MoveTo != record.Replica {
			tenants = append(tenants, record)
			current[record.Spec.Name] = true
		}
		if record.ReplicaNext != "" && record.ReplicaNext != record.Node && record.MoveTo == "" {
			moving = append(moving, record)
		}
	}
	// Tenants that went, or whose copy is elsewhere's to report, leave
	// the metrics; copies no longer sent leave the failure counts.
	counted := map[string]bool{}
	for _, record := range tenants {
		if !record.ReplicaReady {
			counted[copyKey(record.Spec.Name, record.Replica)] = true
		}
	}
	for _, record := range moving {
		counted[copyKey(record.Spec.Name, record.ReplicaNext)] = true
	}
	r.mu.Lock()
	for key := range r.copyFailures {
		if !counted[key] {
			delete(r.copyFailures, key)
		}
	}
	for name := range r.synced {
		if !current[name] {
			delete(r.synced, name)
		}
	}
	for name := range r.failing {
		if !current[name] {
			delete(r.failing, name)
		}
	}
	r.mu.Unlock()
	var started sync.WaitGroup
	for _, record := range tenants {
		r.launch(ctx, &started, record.Spec.Name, record.Replica, !record.ReplicaReady, func() { r.update(ctx, record) })
	}
	for _, record := range moving {
		r.launch(ctx, &started, record.Spec.Name, record.ReplicaNext, true, func() { r.relocate(ctx, record) })
	}
	return &started
}

// launch runs job, a send of tenant's copy on node, unless one runs
// already; whole says the copy is not whole yet, so the send may take
// hours.
func (r *Replicator) launch(ctx context.Context, started *sync.WaitGroup, tenant, node string, whole bool, job func()) {
	key := copyKey(tenant, node)
	r.mu.Lock()
	if r.busy[key] {
		r.mu.Unlock()
		return
	}
	if r.busy == nil {
		r.busy = map[string]bool{}
	}
	r.busy[key] = true
	slots := r.slots(whole)
	r.mu.Unlock()
	started.Add(1)
	r.running.Add(1)
	go func() {
		defer func() {
			r.mu.Lock()
			delete(r.busy, key)
			r.mu.Unlock()
			started.Done()
			r.running.Done()
		}()
		select {
		case slots <- struct{}{}:
		case <-ctx.Done():
			return
		}
		defer func() { <-slots }()
		job()
	}()
}

// slots bounds the sends at once, of copies whole or not; the caller holds
// mu.
func (r *Replicator) slots(whole bool) chan struct{} {
	if r.changeSlots == nil {
		parallel := r.Parallel
		if parallel <= 0 {
			parallel = 4
		}
		r.changeSlots, r.wholeSlots = make(chan struct{}, parallel), make(chan struct{}, parallel)
	}
	if whole {
		return r.wholeSlots
	}
	return r.changeSlots
}

// update sends a tenant's replica what changed, or all of it while it has
// no whole copy yet.
func (r *Replicator) update(ctx context.Context, record store.Record) {
	// The send and its report, together, or neither while held.
	if !r.begin(record.Spec.Name) {
		return
	}
	defer r.end(record.Spec.Name)
	// The send may have waited for its slot while the copy moved: a node
	// no longer the replica gets nothing.
	record, ok := r.placed(record.Spec.Name, func(now store.Record) bool {
		return now.Replica == record.Replica && now.MoveTo != now.Replica
	})
	if !ok {
		return
	}
	_, err := r.replicate(ctx, record.Spec.Name, record.Replica, true)
	if !record.ReplicaReady && ctx.Err() == nil {
		r.copyFailed(record.Spec.Name, record.Replica, err)
	}
	// Once the copy holds the whole data, it may take over.
	if err == nil && !record.ReplicaReady {
		err = r.Store.ReplicaReady(record.Spec.Name, record.Replica, record.Node)
	}
	r.mu.Lock()
	if r.failing == nil {
		r.failing = map[string]string{}
	}
	if err != nil {
		r.failing[record.Spec.Name] = err.Error()
	} else {
		delete(r.failing, record.Spec.Name)
	}
	r.mu.Unlock()
	if err != nil && ctx.Err() == nil {
		r.Log.Warn("send a tenant's changes to its replica", "tenant", record.Spec.Name, "replica", record.Replica, "error", err)
	}
}

// catchUpRounds bounds the sends of changes a moving copy gets in one go
// once whole; one still behind then gets more on the passes that follow.
const catchUpRounds = 5

// relocate sends a tenant's copy to the node it moves to, whole, then the
// changes since, and makes that node the replica once its copy is about
// as current as the replica's: a whole copy can take hours, and the
// replica got every change meanwhile.
func (r *Replicator) relocate(ctx context.Context, record store.Record) {
	name, next := record.Spec.Name, record.ReplicaNext
	// The sends and the switch, together, or none while held.
	if !r.begin(name) {
		return
	}
	defer r.end(name)
	record, ok := r.placed(name, func(now store.Record) bool { return now.ReplicaNext == next && now.MoveTo == "" })
	if !ok {
		return
	}
	// The replica's changes keep going meanwhile, but prune nothing: the
	// snapshot this copy is sent from is the base of the next node's
	// first changes.
	r.relocating(name, 1)
	defer r.relocating(name, -1)
	switched, current := false, false
	var err error
	for round := range catchUpRounds {
		// A hold taken meanwhile waits for this send: it gets no more
		// rounds, nor the switch.
		if round > 0 && r.isHeld(name) {
			r.copyFailed(name, next, nil)
			return
		}
		var matched time.Time
		matched, err = r.replicate(ctx, name, next, false)
		if err != nil {
			break
		}
		if current = r.current(name, matched); current {
			break
		}
	}
	if ctx.Err() == nil {
		r.copyFailed(name, next, err)
	}
	if err == nil && current {
		switched, err = r.Store.SwitchReplica(name, next, record.Node)
	}
	switch {
	case err != nil && ctx.Err() == nil:
		r.Log.Warn("send a tenant's copy to the node it moves to", "tenant", name, "to", next, "error", err)
	case switched:
		r.Log.Info("a tenant's copy moved", "tenant", name, "from", record.Replica, "to", next)
	case err == nil && !current && ctx.Err() == nil:
		r.Log.Info("a tenant's moving copy is still behind its replica; it gets more changes next pass", "tenant", name, "to", next)
	}
}

// placed is tenant's record now, if this node still runs it and still is
// to send the copy that wants says: a send may wait long for its slot.
func (r *Replicator) placed(tenant string, wants func(store.Record) bool) (store.Record, bool) {
	record, err := r.Store.Get(tenant)
	if err != nil || record.Node != r.Store.Node() || !wants(record) {
		return store.Record{}, false
	}
	return record, true
}

// current reports whether a copy that matched tenant's data at matched is
// about as current as its replica: no more than a pass behind its last
// match.
func (r *Replicator) current(tenant string, matched time.Time) bool {
	return !matched.Before(r.Synced(tenant).Add(-r.every()))
}

// relocating counts a copy of tenant starting (1) or ending (-1) its move.
func (r *Replicator) relocating(tenant string, by int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.moves == nil {
		r.moves, r.movesSeen = map[string]int{}, map[string]uint64{}
	}
	r.moves[tenant] += by
	if r.moves[tenant] == 0 {
		delete(r.moves, tenant)
	}
	r.movesSeen[tenant]++
}

// pruneTurn is what a send of tenant's replica, begun now, gives mayPrune.
func (r *Replicator) pruneTurn(tenant string) uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.movesSeen[tenant]
}

// mayPrune reports whether a send of tenant's copy on node, begun at
// turn, may prune its snapshots: node is still its replica, and no copy
// of it moves, as the state says, or moved here since. Pruning keeps the
// newest snapshots, the replica's base among them; a moving copy's base
// may be older, and a node no longer the replica has another's base.
func (r *Replicator) mayPrune(tenant, node string, turn uint64) bool {
	if _, ok := r.placed(tenant, func(now store.Record) bool { return now.Replica == node && now.ReplicaNext == "" }); !ok {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.moves[tenant] == 0 && r.movesSeen[tenant] == turn
}

// Replicate brings the copy of tenant on node up to date with its data:
// the changes since the newest snapshot both have, or the whole data if
// they have none in common. Nothing is sent when nothing changed.
func (r *Replicator) Replicate(ctx context.Context, tenant, node string) error {
	if !r.begin(tenant) {
		return fmt.Errorf("tenant %s: %w", tenant, ErrHeld)
	}
	defer r.end(tenant)
	_, err := r.replicate(ctx, tenant, node, true)
	return err
}

// replicate is Replicate, and reports when the copy last matched the data;
// track says the copy is the replica's, whose last match the metrics
// report and whose sends prune the snapshots. A copy moving to another
// node prunes none: the replica's next sends would take its base.
func (r *Replicator) replicate(ctx context.Context, tenant, node string, track bool) (time.Time, error) {
	turn := r.pruneTurn(tenant)
	snapshots, err := r.Volumes.Snapshots(ctx, tenant, ReplicaReason)
	if err != nil {
		return time.Time{}, err
	}
	asked, cancel := context.WithTimeout(ctx, r.stall())
	theirs, err := r.Peer.Snapshots(asked, node, tenant)
	cancel()
	if err != nil {
		return time.Time{}, fmt.Errorf("ask node %s for its copy: %w", node, peerFailure{err})
	}
	base := NewestCommon(snapshots, theirs)
	if base != "" {
		checked := r.now()
		written, err := r.Volumes.Written(ctx, tenant, base)
		if err != nil {
			return time.Time{}, err
		}
		if written == 0 {
			if track {
				r.markSynced(tenant, checked)
			}
			return checked, nil
		}
	}
	taken := r.now()
	full, err := r.snapshot(ctx, tenant)
	if err != nil {
		return time.Time{}, err
	}
	_, snapshot, _ := strings.Cut(full, "@")
	if err := r.send(ctx, tenant, node, base, snapshot); err != nil {
		// The snapshot goes, so failures never push the copy's base out of
		// the snapshots kept.
		if destroyErr := r.Volumes.DestroySnapshot(context.WithoutCancel(ctx), tenant, snapshot); destroyErr != nil {
			err = errors.Join(err, destroyErr)
		}
		return time.Time{}, err
	}
	if !track {
		return taken, nil
	}
	r.markSynced(tenant, taken)
	if !r.mayPrune(tenant, node, turn) {
		return taken, nil
	}
	if err := r.Volumes.Prune(ctx, tenant, ReplicaReason, r.keep()); err != nil {
		r.Log.Warn("prune replica snapshots", "tenant", tenant, "error", err)
	}
	return taken, nil
}

// snapshot takes a replica snapshot of tenant. The replica's send and a
// moving copy's may take one at once: they take them one at a time, a
// millisecond apart, as the names are timed to the millisecond.
func (r *Replicator) snapshot(ctx context.Context, tenant string) (string, error) {
	r.mu.Lock()
	if r.snapshotting == nil {
		r.snapshotting = map[string]*sync.Mutex{}
	}
	lock := r.snapshotting[tenant]
	if lock == nil {
		lock = &sync.Mutex{}
		r.snapshotting[tenant] = lock
	}
	r.mu.Unlock()
	lock.Lock()
	defer lock.Unlock()
	defer time.Sleep(time.Millisecond)
	return r.Volumes.Snapshot(ctx, tenant, ReplicaReason)
}

// NewestCommon is the newest of the snapshots ours and theirs both list,
// oldest first; "" if none.
func NewestCommon(ours, theirs []string) string {
	for i := len(theirs) - 1; i >= 0; i-- {
		if slices.Contains(ours, theirs[i]) {
			return theirs[i]
		}
	}
	return ""
}

func (r *Replicator) send(ctx context.Context, tenant, node, base, snapshot string) error {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	reader, writer := io.Pipe()
	stream := &watched{r: reader, last: time.Now()}
	// A stream that stalls ends, so neither side waits on it for ever.
	unwatch := watch(ctx, stream, r.stall(), func(err error) {
		cancel(err)
		_ = reader.CloseWithError(err)
		_ = writer.CloseWithError(err)
	})
	sent := make(chan error, 1)
	go func() {
		err := r.Volumes.SendSnapshot(ctx, tenant, base, snapshot, writer)
		// Known before the other node can see the stream end.
		sent <- err
		_ = writer.CloseWithError(err)
	}()
	received := r.Peer.Send(ctx, node, tenant, base == "", stream)
	// Whose failure: a send that failed here first ended the stream; a
	// send that failed only once the other node gave up is its failure.
	var sentErr error
	failedHere := false
	select {
	case sentErr = <-sent:
		failedHere = sentErr != nil
	default:
		_ = reader.CloseWithError(received)
		sentErr = <-sent
	}
	_ = reader.CloseWithError(received)
	unwatch()
	var stalled stallError
	if (sentErr != nil || received != nil) && errors.As(context.Cause(ctx), &stalled) {
		if stalled.here {
			return errors.Join(stalled, sentErr)
		}
		return peerFailure{fmt.Errorf("node %s: %w", node, stalled)}
	}
	switch {
	case failedHere:
		return errors.Join(sentErr, received)
	case received != nil:
		return peerFailure{received}
	}
	return sentErr
}

// watched is a stream to another node, which notes when it last moved and
// whether the other node is reading it.
type watched struct {
	r       io.Reader
	mu      sync.Mutex
	last    time.Time
	reading bool
}

func (w *watched) Read(p []byte) (int, error) {
	w.mu.Lock()
	w.reading = true
	w.mu.Unlock()
	n, err := w.r.Read(p)
	w.mu.Lock()
	w.reading = false
	if n > 0 || err != nil {
		w.last = time.Now()
	}
	w.mu.Unlock()
	return n, err
}

// stallError is a stream that moved nothing for a while: here says this
// node had nothing to give while the other read, rather than the other
// reading nothing, or answering nothing once it had all.
type stallError struct {
	after time.Duration
	here  bool
}

func (s stallError) Error() string {
	if s.here {
		return fmt.Sprintf("this node sent nothing for %s", s.after)
	}
	return fmt.Sprintf("the stream moved nothing for %s", s.after)
}

// watch calls stop once stream has not moved for stall, until the returned
// function is called; it returns once watching ended.
func watch(ctx context.Context, stream *watched, stall time.Duration, stop func(error)) func() {
	quit, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		tick := time.NewTicker(max(stall/8, time.Millisecond))
		defer tick.Stop()
		for {
			select {
			case <-quit:
				return
			case <-ctx.Done():
				return
			case <-tick.C:
			}
			stream.mu.Lock()
			idle, here := time.Since(stream.last), stream.reading
			stream.mu.Unlock()
			if idle >= stall {
				stop(stallError{after: stall, here: here})
				return
			}
		}
	}()
	return func() {
		close(quit)
		<-done
	}
}

// peerFailure is a failure of the node a copy goes to, not of this one.
type peerFailure struct{ err error }

func (p peerFailure) Error() string { return p.err.Error() }
func (p peerFailure) Unwrap() error { return p.err }

// copyGiveUp is how many sends of a copy that is not whole yet, a
// replica's first or a copy moving to another node, may fail in a row
// for the other node before the copy is given up there: copies move one
// at a time, and only once every replica is whole, so one that never gets
// there would stop every other.
const copyGiveUp = 4

func copyKey(tenant, node string) string { return tenant + "\x00" + node }

// copyFailed counts a send of tenant's copy, not whole yet, to node; a
// success resets the count. Only the other node's failures count, and
// not while it restarts on purpose.
func (r *Replicator) copyFailed(tenant, node string, err error) {
	key := copyKey(tenant, node)
	r.mu.Lock()
	if err == nil {
		delete(r.copyFailures, key)
		r.mu.Unlock()
		return
	}
	var peer peerFailure
	if !errors.As(err, &peer) || r.away(node) {
		r.mu.Unlock()
		return
	}
	if r.copyFailures == nil {
		r.copyFailures = map[string]int{}
	}
	r.copyFailures[key]++
	failed := r.copyFailures[key]
	if failed >= copyGiveUp {
		delete(r.copyFailures, key)
	}
	r.mu.Unlock()
	if failed < copyGiveUp {
		return
	}
	why := fmt.Sprintf("%d sends in a row failed, the last: %v", failed, err)
	if changed, giveUpErr := r.Store.GiveUpCopy(tenant, node, why); giveUpErr != nil {
		r.Log.Warn("give up a tenant's copy on a node that does not take it", "tenant", tenant, "node", node, "error", giveUpErr)
	} else if changed {
		r.Log.Warn("gave up a tenant's copy on a node that does not take it; no copy goes there for a while", "tenant", tenant, "node", node, "why", why)
	}
}

// away reports whether node restarts on purpose now.
func (r *Replicator) away(node string) bool {
	now := r.now()
	for _, n := range r.Store.Nodes() {
		if n.Name == node {
			return now.Before(n.AwayUntil)
		}
	}
	return false
}

func (r *Replicator) markSynced(tenant string, at time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.synced == nil {
		r.synced = map[string]time.Time{}
	}
	if at.After(r.synced[tenant]) {
		r.synced[tenant] = at
	}
}

// Synced is when the copy of tenant last matched its data, as far as this
// node knows; zero if not since the agent started.
func (r *Replicator) Synced(tenant string) time.Time {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.synced[tenant]
}

// Sweep drops the copies this node keeps of tenants it is no longer the
// replica of, or that are gone, and puts aside the data of tenants it no
// longer runs, as after its tenants failed over to their replicas while
// it was down: that data holds the writes the failover lost, so it is kept.
// It acts only on a state caught up with the cluster's.
func (r *Replicator) Sweep(ctx context.Context) error {
	if r.CatchUp != nil {
		if err := r.CatchUp(); err != nil {
			return fmt.Errorf("the state may be behind the cluster's, so nothing is tidied: %w", err)
		}
	}
	datasets, err := r.Volumes.Datasets(ctx)
	if err != nil {
		return err
	}
	self := r.Store.Node()
	var errs []error
	for _, dataset := range datasets {
		name := dataset.Tenant
		record, err := r.Store.Get(name)
		known := err == nil
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			errs = append(errs, err)
			continue
		}
		if known && (record.Node == self || record.MoveTo == self) {
			continue // this node's data, or a copy taking over
		}
		if r.Moving != nil && r.Moving(name) {
			continue
		}
		// Data a move left unmounted here, which it removes itself if the
		// move is undone.
		if !dataset.Replica && !dataset.Mounted {
			continue
		}
		if dataset.Replica {
			if known && (record.Replica == self || record.ReplicaNext == self) {
				continue
			}
			if err := r.Volumes.DropReplica(ctx, name); err != nil {
				errs = append(errs, err)
				continue
			}
			r.Log.Info("dropped a copy this node no longer keeps", "tenant", name)
			continue
		}
		why := "elsewhere"
		if !known {
			why = "purged"
		}
		aside, err := r.Volumes.PutAside(ctx, name, why)
		if err != nil {
			errs = append(errs, fmt.Errorf("put aside the data of tenant %s, which runs elsewhere or is gone: %w", name, err))
			continue
		}
		r.Log.Warn("put aside the data of a tenant this node no longer runs; destroy it once it is not needed", "tenant", name, "dataset", aside)
	}
	return errors.Join(errs...)
}

// WriteMetrics writes how far behind each tenant's copy is.
func (r *Replicator) WriteMetrics(out *metrics.Writer) {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.now()
	out.Family("murder_tenant_replica_lag_seconds", "gauge", "How long ago the tenant's copy on its replica last matched its data.")
	for _, name := range slices.Sorted(maps.Keys(r.synced)) {
		out.Sample("murder_tenant_replica_lag_seconds", now.Sub(r.synced[name]).Seconds(), "tenant", name)
	}
	out.Family("murder_tenant_replica_failing", "gauge", "1 if the last send of the tenant's changes to its replica failed.")
	names := slices.Sorted(maps.Keys(r.failing))
	for _, name := range slices.Sorted(maps.Keys(r.synced)) {
		if !slices.Contains(names, name) {
			out.Sample("murder_tenant_replica_failing", 0, "tenant", name)
		}
	}
	for _, name := range names {
		out.Sample("murder_tenant_replica_failing", 1, "tenant", name)
	}
}
