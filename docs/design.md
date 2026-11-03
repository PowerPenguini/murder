# Design

Murder is a self-built orchestrator for single-tenant web apps: one app
container per tenant, its state in one local data directory. It deliberately
avoids Kubernetes and Nomad: the hard part, moving a tenant's local data
between servers, is not something they solve.

## Model

- **Tenant**: one customer workspace. It gets its own container, data
  directory, network and subdomain (`<tenant>.<base domain>`). The app inside
  is single-tenant; Murder never looks into its data.
- **Spec**: what a tenant runs — image (pinned by digest), container port,
  health path, data mount path and owner, environment, generated secrets and
  resource limits. An app is configuration, not code in Murder.
- **Secrets**: generated per tenant, stored encrypted with a master key that
  lives in a `0600` file on each node and offline in a password manager.

## Topology

- Start with three small cloud servers in three locations on one private
  network; each runs tenants, the Murder agent, the edge proxy and a Raft voter.
  Grow by resizing, then by adding dedicated servers and draining old ones.
- Capacity is planned N+1: the tenants of a failed server must fit on the rest.

## Data

- One ZFS dataset per tenant on local disk, snapshotted and sent every ~15 s
  to a replica in another location (RPO ≤ 30 s). A snapshot is taken before
  every image change.
- Hourly logical backups plus data files go off-site; a weekly job restores a
  random tenant.
- The app's database must recover every acknowledged write from a crash and
  from a point-in-time snapshot; that is verified per app before onboarding.

## Failover

- The cluster's leader probes every node every 2 s. After 30 s without an
  answer it powers the server off through the provider API, and only after a
  confirmed power-off and 60 s since the node last answered does it start the
  tenants from their replicas. No confirmed power-off means no promotion, only
  an alert. Nodes do not stop their own tenants when cut off: see
  [failover.md](failover.md).
- A planned move (`drain`) stops the tenant, sends a final incremental
  snapshot and starts it on the replica: no data loss, 10–30 s downtime.

## Traffic

- The wildcard DNS name points at every server; the edge proxy on each routes
  by `Host` to wherever the tenant runs, over the private network. A wildcard
  certificate comes from ACME DNS-01.
- Custom tenant domains come later.

## Delivery

- CI builds images once and pushes them to a registry; nodes pull by digest.
- Rollouts go to a random canary group (with an exclusion list) and then to
  the rest, promoting automatically after a quiet period with a manual brake;
  a failed start or migration restores the pre-upgrade snapshot.

## Build stages

1. Verify the first app's crash and snapshot recovery (done in the app repo).
2. Single server: agent, Docker runtime, per-tenant volumes, edge proxy,
   tenant CLI, encrypted secrets. State in SQLite: apps, tenants (an app plus
   overrides), sealed secrets and an audit log, behind the API a panel will
   use.
3. Three servers: Raft replicates the same store changes to every node's
   SQLite; ZFS replication; `drain`.
4. Automatic failover: leases, power-off fencing, promotion, alerts.
5. Wave rollouts with canaries and rollback. Started: apps follow an image
   tag, a newer image is offered, and every change is a revision to go back
   to; waves and canaries are next.
6. Shared services: provider proxy with per-tenant metering, catalog
   publishing.

Stages 1 to 4 are done, as is the administration panel.
