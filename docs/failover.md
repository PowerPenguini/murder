# Replication and automatic failover

How Murder keeps a second copy of every tenant's data on another node and
runs the tenant from that copy when its node dies. Murder already moves
tenants between nodes, holding requests meanwhile (`tenant move`,
`cluster drain`/`replace`). This document covers what happens when a node
dies without warning.

## Goals and limits

- **Recovery point (RPO):** the writes a tenant took in the last 15 s
  before its node stopped being reachable, plus those it took until it was
  powered off. Replication sends what changed every 15 s, and the node keeps
  taking writes over its public edge until the power-off, about 30–45 s
  later. So up to about 60 s of writes in the worst case. A node that just
  crashed takes none after its death.
- **Recovery time (RTO):** about 70–90 s from the node's death until its
  tenants serve again from their replicas:
  - 30 s until the node counts as dead;
  - the provider's power-off, confirmed;
  - at least 60 s since the node was last seen;
  - the start of the containers.
- **No split brain:** a tenant never runs on two nodes that both take
  writes. A replica is promoted only once the old node is **confirmed powered
  off** by the provider's API, or an operator states that it is. Without that,
  nothing is promoted and Murder only reports it.
- **Scope:** ZFS nodes only (the dir driver has no snapshots). One node fails
  at a time: three nodes keep a majority with one down.
- **Out of scope here:**
  - **DNS:** the wildcard name keeps pointing at the dead node's address too,
    so new connections to that address fail until a client tries another one.
    A load balancer, or DNS records updated on failover, is a later step.
  - **Restarting a dead node:** that is the operator's call.

## Change from docs/design.md: no self-stop after 20 s

The design had every node stop its own tenants after 20 s without contact.
That is dropped:

- **Promotion already needs a confirmed power-off.** A node that is powered
  off serves nothing, so stopping itself first adds no safety.
- **Self-stopping costs availability.** Suppose the private network fails
  for everyone at once, a known Hetzner incident type. With self-stop, every
  node would stop every tenant after 20 s, though no promotion can happen:
  promotion needs a majority, and no node would have one.
- **Without self-stop, the same incident keeps all tenants running.** The
  edges keep serving them over the public network.

The 5 s lease writes go too. The leader probes nodes directly, so health
checking adds no Raft writes.

## Model

Each tenant record gets two new fields:

- **`Replica`:** the node that keeps a copy of the tenant's data, never the
  node that runs it.
- **`ReplicaReady`:** set by the owner once a send to the copy succeeds.
  Only a ready copy takes over. It is cleared:
  - whenever `Replica` changes;
  - when a move onto the replica's node starts, because the move writes
    over the copy (a failed move rolls the copy back to its newest replica
    snapshot, and the next send makes it ready again);
  - when any move finishes, because the new node's first send has to reach
    the copy first. A moved tenant cannot fail over until then.

These changes go together in one schema version:

- **Placement:** the leader gives every tenant without a replica, or with a
  replica on a node that left the cluster, the member node (other than its
  own) holding the fewest replicas.
- **Rebalancing:** once every tenant whose node is not failed has a ready
  copy and none moves, the leader moves one copy at a time from the node
  keeping the most to one keeping at least two fewer, as after a node
  joins or comes back from a failure (schema 10: `tenants.replica_next`).
  The owner sends the new node a whole copy while still sending the
  replica its changes, and only once that copy is whole does it become the
  replica, ready (`SwitchReplica`); the old node then drops its copy. So a
  tenant never lacks a whole copy while its copy moves. A failover, a move
  of the tenant, or the new node failing or leaving stops the move of the
  copy; a failover promotes the old, whole copy.
- **`FinishMove`:** it clears `Replica` when the tenant moves onto its replica
  node, so placement picks another.
- **`FailOver(node)`:** in one batch, every tenant of the dead node with a
  ready replica gets `Node = Replica` and `Replica = ""`, and any move it was
  in is cancelled. The audit log records each one.

## Replication (owner → replica)

The owner node's agent runs a replication loop. Every 15 s, for each tenant
it owns that has a replica, it does the following. A tenant moving onto its
replica's node is skipped, because the move updates that copy. Tenants
moving elsewhere keep being replicated.

1. It skips the tenant if nothing was written since the last replica
   snapshot (ZFS `written@<snapshot>` is 0).
2. It takes the snapshot `@murder-replica-<time>`.
3. It asks the replica node for its snapshots and sends the changes since
   the newest one both have (`zfs send -i`). If they have none in common,
   it sends the whole dataset.
4. The replica node receives with `zfs receive -u -o canmount=noauto
   -o io.murder:role=replica -o mountpoint=<tenants>/<name>`:
   - `canmount=noauto` and the role property: a replica is never mounted,
     not at boot either, and is never taken for live data.
   - Changes are received with `-F`, which rolls back anything written to
     the copy since its newest snapshot.
   - A whole dataset is received beside the old copy
     (`murder-incoming-<name>`) and renamed over it once it is all there,
     so the node keeps a copy throughout.
   - Receiving and promotion share a per-dataset lock.
5. The replica node prunes its received snapshots to the newest 3. The owner
   prunes its own `replica` snapshots to the newest 3.

Up to 4 tenants are sent at once. Archived tenants are replicated too: their
data rarely changes, so step 1 skips them.

**What the replica node refuses**, checked against its own view of the
state:

- data for a tenant it runs itself;
- data from a node that does not run the tenant (the sender is known from
  its cluster certificate);
- data for a tenant whose replica it is not;
- data for a tenant that is moving to this node.

So an old owner coming back can never overwrite the copy that took over.

**Cleanup:** a node destroys its replica copy of a tenant when the tenant is
purged, or when its replica moves to another node. That is always a copy; the
owner holds the data. A move onto the replica node sends only the changes
since the newest snapshot it shares with the copy, so the copy is never
dropped.

**Safeguards in the reconciler:**

- A tenant that has run before (its data has an image) is not started on
  new, empty data when its data is missing. It reports that the data is
  missing instead.
- A copy found for a tenant that never ran belongs to an earlier tenant of
  the same name that was purged. It is dropped, never adopted.

**Measured:**

- On the owner: `murder_tenant_replica_lag_seconds{tenant}`, the age of the
  newest snapshot the replica has, and `murder_tenant_replica_failing`.
- `tenant show`: the replica node, whether its copy is ready, and when it
  last matched the data.

## Failure detection and fencing (leader)

The Raft leader probes every other node's cluster port every 2 s, with a 2 s
timeout. A node that has not answered for `failover.after` (30 s) counts as
dead. Then the leader:

1. **Checks there is something to run elsewhere.** If none of the node's
   tenants has a ready copy on a node the leader hears, the node is not
   powered off. After 5 minutes of silence it is marked failed: the copies
   it keeps go to other nodes, and its tenants stay where they are. A
   shorter pause keeps its copies.
2. **Powers the node off** through the provider's API:
   - On Hetzner Cloud, it finds the server by the node's name (or a
     configured server name) and checks that the server has the node's
     private address. A wrong name never powers off, or confirms off,
     another machine.
   - It then calls `POST /servers/{id}/actions/poweroff`, and reads
     `GET /servers/{id}` until the status is `off`. That read is the
     confirmation. It is tried for up to 3 minutes, and a failed read is
     simply tried again.
3. **Waits** until at least 60 s have passed since the node last answered.
4. **Checks again**, and stops if the node answered meanwhile, or if the
   provider no longer reports it off.
5. **Writes `FailOver(node)`.** The replica nodes' reconcilers see tenants of
   their own, mount the datasets (`canmount=on`) and start the containers. The
   edges route to them as soon as the state changes.

**What the leader never does:**

- **Act while it hears fewer than a majority of the nodes, itself
  included.** A leader whose own probes fail may be the node that is cut
  off. Without this rule, it would power off the others one after another.
- **Act on a node that is away** (`awayUntil`). An agent that stops cleanly,
  as for a reboot, marks its node away for 10 minutes, and clears the mark
  once it is back and caught up. An agent that crashes leaves no mark.
- **Act while changes are frozen for an upgrade.** `murder upgrade`
  restarts agents on purpose.
- **Act without a configured provider.** The operator then runs
  `murder cluster failover NODE -powered-off` after checking the node is
  off.
- **Fence two nodes at once, or the node it runs on.**

A new leader starts counting from zero. So if the leader itself died, its
tenants fail over up to one election plus 30 s later.

**When no power-off can be confirmed** (API down, wrong token, server not
found), nothing is promoted. The leader:

- logs an error, tries again every minute, and sets
  `murder_failover_blocked{node} 1`;
- leaves the operator to run `murder cluster failover NODE -powered-off`.

**Tenants without a ready replica** (one node left, or replication never
caught up) are not promoted, and get no new replica while their node is
failed. The audit log names them.

**Limits:**

- **Two nodes:** losing either leaves no majority, so no failover can be
  written, automatic or manual.
- **A freeze after the power-off:** if a freeze begins after the leader
  powered the node off, its tenants wait until the thaw.
- **A dead agent on a live host:** a host whose agent stays down for 30 s
  without having stopped cleanly is powered off, even though its tenants
  were still running.

## A dead node that comes back

The operator powers the node on again. To keep it from serving stale data:

- **Containers use Docker's restart policy `no`.** The engine starts none
  by itself, at boot either.
  - `on-failure` would not do: after a power-off, Docker finds the
    containers that were running without a process, gives them exit code
    255, and so starts them all at boot, on the stale data.
  - The reconciler starts a container whose app exited at its next pass,
    and counts and reports the exit as the engine did.
  - The reconciler moves existing containers to `no` in place
    (`POST /containers/{id}/update`), with no restart.
- **In a cluster, the reconciler's first pass waits for the agent to catch
  up with the cluster's log.** Until then it starts nothing. A node that
  cannot reach a majority starts nothing either.
- **Datasets of tenants the node no longer runs are renamed aside**, to
  `murder-failed-<tenant>-<time>`. They are unmounted and never destroyed
  automatically, because they hold the seconds of writes the failover lost.
  The node then gets a fresh copy if it becomes a replica again.

## Configuration

```yaml
cluster:
  failover:
    after: 30s                       # unreachable this long counts as dead
    hetzner:
      tokenFile: /etc/murder/hcloud.token   # read/write token of the project
      # servers: {n1: murder-n1}     # Hetzner server names, if not the node names
```

- **Without `failover`:** replication still runs, and failover is manual
  (`cluster failover -powered-off`).
- **The token can power off and delete every server of the project.** It
  belongs on the nodes as a `0600` file, in a project that holds only the
  cluster.

## Tests

- **Unit tests:**
  - placement;
  - `FailOver` preconditions;
  - the replica checks;
  - the snapshot-base choice (incremental vs whole);
  - pruning;
  - the leader's state machine, with a fake clock, fake prober and fake
    provider: power-off timing, a blocked confirmation, frozen changes, a
    leader change;
  - the restart-policy update;
  - the first pass waiting for catch-up.
- **On 3 Hetzner servers**, with a client writing a counter to a tenant and
  recording what was acknowledged:
  1. Steady state: replica lag stays under 30 s under write load.
  2. Hard power-off of a node from outside: detection, Murder's own power-off
     call, promotion. Measured: time until the node's tenants serve again, and
     acknowledged writes lost.
  3. Private-network partition of a node that keeps running: Murder powers
     it off and promotes. Powered on again, the node serves nothing stale,
     puts its old datasets aside, rejoins and receives fresh replicas.
  4. Failure of the node that leads the cluster.
  5. A move onto the replica node; replication continues afterwards.
  6. No provider token: no promotion, the blocked metric, then a manual
     `cluster failover -powered-off`.
- **Afterwards**, every test server is deleted.

### Measured on Hetzner, 2 November 2026

Three cpx02 nodes (1 vCPU, 1 GB) in fsn1, nbg1 and hel1 and a client in nbg1; the counter
writes about 5 times a second, alternating between the nodes' edges.

| Scenario | Clients cut off | Acknowledged writes lost |
| --- | --- | --- |
| Steady state | none; replica lag 1–13 s | none |
| Hard power-off of a node | 56.7 s | 31 (about 6 s) |
| Partition of a node that keeps serving | 34.9 s | 39 (acknowledged by the cut-off node until it was off) |
| Power-off of the node running every tenant | 55.8 s | 3 |
| Power-off of the leader | 64.1 s (new leader after 4 s) | 3 |
| Partition of the leader | 27.1 s | 48 (as above) |
| Move onto the replica's node | none | none |
| No provider, then `cluster failover -powered-off` | until the operator acts | none |

In every power-off the leader called Hetzner's API about 32 s after the
node's last answer, had the power-off confirmed within 10 s, and promoted
60 s after the last answer. A cut-off leader powered off no one. Nodes
powered on again put their old data aside (it held the writes acknowledged
in isolation), served nothing stale, and rejoined. Rolling upgrades with
failover enabled powered off no one.

The tests found, and these PRs fix: sends to an unreachable replica that
held up all replication and placement for over two minutes (connections
between nodes now time out); a node back from the dead that sent tenants
from its old state before catching up; a replica refusing a copy it had
just been given, before applying that; and the blocked-failover alert
staying on after an operator's failover.

## Delivery

1. **Replication:**
   - the `Replica` field and placement;
   - the replication loop and the replica endpoints with their checks;
   - cleanup;
   - moves;
   - metrics and `tenant show`;
   - the restart policy `no` and the first pass waiting for catch-up;
   - renaming stale datasets aside.
2. **Failover:**
   - `FailOver`;
   - the leader's monitor;
   - the Hetzner fencing client;
   - `cluster failover -powered-off`;
   - configuration, metrics and the README.
3. **Hetzner harness:**
   - the failover config on the test nodes;
   - a counter-writing client;
   - the scenarios above, with their evidence in the PR.
