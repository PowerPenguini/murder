# Replication and automatic failover

How Murder keeps a second copy of every tenant's data on another node and
runs the tenant from that copy when its node dies. Murder already moves
tenants between nodes, holding requests meanwhile (`tenant move`,
`cluster drain`/`replace`). This document covers what happens when a node
dies without warning.

## Goals and limits

- **Recovery point (RPO):** at most ~30 s of a tenant's writes are lost when
  its node dies: replication sends what changed every 15 s.
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

Each tenant record gets one new field:

- **`Replica`:** the node that keeps a copy of the tenant's data, never the
  node that runs it.

These changes go together in one schema version:

- **Placement:** the leader gives every tenant without a replica, or with a
  replica on a node that left the cluster, the member node (other than its
  own) holding the fewest replicas. It reassigns as nodes join and leave.
- **`FinishMove`:** it clears `Replica` when the tenant moves onto its replica
  node, so placement picks another.
- **`FailOver(node)`:** in one batch, every tenant of the dead node that has a
  replica gets `Node = Replica` and `Replica = ""`, and any move it was in is
  cancelled. The audit log records each one.

## Replication (owner → replica)

The owner node's agent runs a replication loop. Every 15 s, for each tenant
it owns that has a replica and is not moving, it does the following:

1. It skips the tenant if nothing was written since the last replica
   snapshot (ZFS `written@<snapshot>` is 0).
2. It takes the snapshot `@murder-replica-<time>`.
3. It asks the replica node for its newest snapshot. If the owner still has
   that snapshot, it sends the changes since then (`zfs send -i`). Otherwise
   it sends the whole dataset, and the replica first drops what it had.
4. The replica node receives with `zfs receive -u -F -o canmount=noauto
   -o mountpoint=<tenants>/<name>`:
   - `canmount=noauto`: a replica is never mounted, not at boot either.
   - `-F`: it rolls back anything written to the copy since the last
     snapshot.
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
- data for a tenant that is moving.

So an old owner coming back can never overwrite the copy that took over.

**Cleanup:** a node destroys its replica copy of a tenant when the tenant is
purged, or when its replica moves to another node. That is always a copy; the
owner holds the data. A move onto the replica node discards the replica
before the move's whole send.

**Measured:**

- On the owner: `murder_tenant_replica_lag_seconds{tenant}`, the age of the
  newest snapshot the replica has.
- On the replica: `murder_tenant_replica_received_timestamp_seconds{tenant}`.
- `tenant show`: the replica node and its lag.

## Failure detection and fencing (leader)

The Raft leader probes every other node's cluster port every 2 s, with a 2 s
timeout. A node that has not answered for `failover.after` (30 s) counts as
dead. Then the leader:

1. **Powers the node off** through the provider's API. On Hetzner Cloud that
   means finding the server by the node's name (or a configured server name),
   calling `POST /servers/{id}/actions/poweroff`, then reading
   `GET /servers/{id}` until its status is `off`. That read is the
   confirmation; it is tried for 2 minutes.
2. **Waits** until at least 60 s have passed since the node last answered.
3. **Writes `FailOver(node)`.** The replica nodes' reconcilers see tenants of
   their own, mount the datasets (`canmount=on`) and start the containers. The
   edges route to them as soon as the state changes.

**What the leader never does:**

- act while changes are frozen for an upgrade (`murder upgrade` restarts
  agents on purpose);
- act without a configured provider; then the operator runs
  `murder cluster failover NODE -powered-off` after checking the node is off;
- fence two nodes at once;
- fence the node it runs on.

A new leader starts counting from zero. So if the leader itself died, its
tenants fail over up to one election plus 30 s later.

**When no power-off can be confirmed** (API down, wrong token, server not
found), nothing is promoted. The leader:

- logs an error every minute;
- sets `murder_failover_blocked{node} 1`;
- leaves the operator to run `murder cluster failover NODE -powered-off`.

**Tenants without a replica** (one node left, or replication never caught
up) are not promoted. The audit log and metrics name them.

## A dead node that comes back

The operator powers the node on again. To keep it from serving stale data:

- **Containers use Docker's `on-failure` restart policy, not
  `unless-stopped`.**
  - Docker still restarts a container that crashes, as now.
  - It no longer starts containers when the server boots. The agent starts
    them once it knows they are its own.
  - The reconciler moves existing containers to the new policy in place
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

## Delivery

1. **Replication:**
   - the `Replica` field and placement;
   - the replication loop and the replica endpoints with their checks;
   - cleanup;
   - moves;
   - metrics and `tenant show`;
   - the `on-failure` restart policy and the first pass waiting for catch-up;
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
