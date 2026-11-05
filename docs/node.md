# What a node runs

Every server runs the same processes: `murder agent`, `murder edge` and, if wanted, `murder panel`. This is what each part does.

`murder agent` runs on every server:

- **Reconciler.** Keeps Docker in line with the desired state. Each tenant gets
  its own container, network and data volume. A container is replaced when its
  spec changes (requests for the tenant wait meanwhile, on every edge, until
  the new container answers, instead of failing), and the volume is
  snapshotted first if the image changed: once while the old container
  runs (a failed snapshot leaves it running, unchanged), and again once it
  has stopped, so the snapshot to go back to holds every write; it
  replaces the first. A tenant keeps the last five of those snapshots. Each pass removes registry
  images no tenant needs: a tenant needs the image it runs, the one its data
  has and the one before, so a rollback by one release needs no registry.
  Images are compared by ID, one a container still uses is tried again an
  hour later, and local images stay (they could not be pulled again). Containers of archived or unknown tenants are
  removed. Tenants keep running
  while the agent is down; a restarted agent adopts them without restarting
  them. Images are pulled in the background: a tenant waiting for one shows
  `pulling` while its old container keeps serving, and a failed pull is
  retried after 10 s, doubling up to 10 minutes (`murder reconcile` retries
  at once, say after fixing credentials, and waits for the pulls to report
  how they went). Health checks run side by side. A tenant whose app exits
  is started again at the next pass and shows `restarting`; `tenant list`
  counts its restarts and `tenant show` says when it last exited and why,
  such as going over its memory limit (containers get no swap beyond it;
  ones created before this release from their next replacement). Stopping
  the agent lets a container replacement under way finish first, also in a
  pass `murder reconcile` started, so no tenant is left without one.
- **Edge proxy.** `murder edge`, a process of its own, routes
  `<tenant>.<baseDomain>` to the tenant's container. It
  streams responses (server-sent events) and replaces any client-sent
  `X-Forwarded-*` headers. Over HTTPS it adds `Strict-Transport-Security` (one
  year) unless the app sets its own. TLS is `off`, `internal` (a local CA, for testing) or
  `acme`, which gets a wildcard certificate through DNS-01 from Hetzner Cloud DNS
  or Namecheap. It holds no state: the agent hands it the routes as soon as
  they change, and it keeps the last ones (also on disk, `edge.routes`), so
  it serves while the agent restarts or is down. An edge started anew, as
  after a reboot, holds requests for tenants until the agent confirms
  routes, for at most `edge.routesHold` (15 s; 0 holds none): the saved
  routes are those of before it stopped, and a tenant may have failed over
  or moved since. In a cluster the agent names routes only once its node
  has caught up with the cluster. If the agent does not answer in time,
  the saved routes serve, and from then on every request at once. An edge
  that takes over from another, in an upgrade, holds nothing. A request costs it little:
  routes are one lookup in memory, its reverse proxies are shared and lend
  copy buffers from a pool, connections to tenants and to other nodes' edges
  stay open (one HTTP/2 connection to each other edge is kept warm and
  carries its requests side by side; WebSockets get HTTP/1.1 connections of
  their own), it neither compresses nor decompresses bodies, flushes only
  streams as they come, speaks HTTP/2 to browsers over HTTPS, and counts
  requests without a lock. On the host, `deploy/murder-sysctl.conf` keeps a
  connection's sending rate across a pause between requests instead of
  starting it over (a 60 KB response after a click takes one round trip, not
  three); the edge warns at start when it is not set. A client must keep
  sending a request's body once its headers came: one that sends nothing
  of it for 30 seconds, counted from when the edge starts reading it or
  answering (after any wait for a moving tenant), is given up on, whatever
  its method or headers say (a slow upload that keeps moving is not, nor is
  a response that streams). The edge tunnels nothing: a `CONNECT` is
  answered 501. The edge holds at most 4096
  requests at once before passing them on, waiting for a moving tenant or
  for a small body to keep for another node's edge; past that, a request
  for a moving tenant is answered 503 at once, and a body streams through.
  `murder_edge_routes_timestamp_seconds`
  in its metrics (`edge.metrics`) says when the agent last answered.
- **Local API.** A unix socket the `murder` CLI (and, later, the panel) talks to.
- **Database.** One SQLite file holds the node's apps, tenants, sealed secrets
  and an audit log of every change: who (the socket user, plus the person a
  panel acts for), what and when. In a cluster every node holds the same.
- **Volumes.** The `zfs` driver gives each tenant a dataset, limited to its
  disk limit (`refquota`, so snapshots do not count). A limit below what a
  tenant already stores is not applied; `tenant list` shows the tenant with
  a warning and `tenant show` says why. The `dir` driver uses
  plain directories, cannot limit disk use and is for development.
- **Firewall.** An nftables table (`inet murder`) keeps tenants away from
  the host and private networks; see the [security model](security.md).
- **Metrics.** With `metrics:` set (say `127.0.0.1:9464`), the agent's
  `/metrics` serves, in the Prometheus text format: each live tenant of the
  node's phase, health, restarts and warning; when the last whole reconcile
  pass ended and its error count; when the firewall was last loaded and a
  backup last sent (since the agent started). With `edge.metrics` set (say
  `127.0.0.1:9465`), the edge's serves per tenant its responses by status
  class, how long their headers took and requests it could not pass on, and
  when the certificate expires and the agent last confirmed the routes.
  Enough to alert on a tenant down or crashing, a stuck agent, a missing
  backup or a certificate not renewed.
- **Use.** Every 15 seconds the agent measures each running tenant: CPU,
  memory against its limit and disk I/O from its container's cgroup (v2;
  on a cgroup v1 host containers are not measured), its traffic from its
  network namespace, its data's size from ZFS every 5 minutes, and the
  host's CPU, memory and load. The edge counts the requests it passes to
  its own node's containers (each request once, however many edges it went
  through, and once while an edge hands over to the next, as both tell
  their own counts, or after it could not tell for a while) and tells the agent every 15 seconds: requests, errors (5xx and
  requests it could not pass on) and the 95th percentile of the time to
  the response headers. A day is kept a minute at a time, in memory: a
  restarted agent starts the day again. `GET /v1/usage` is the cluster's
  use now, `GET /v1/tenants/NAME/usage?step=300` a tenant's day; the panel
  draws both. `/metrics` has `murder_tenant_cpu_seconds_total`,
  `murder_tenant_memory_bytes` and `_limit_bytes`, network and disk byte
  counters and `murder_tenant_data_bytes`, for a Prometheus to keep longer.
  Memory leaves out the files a container no longer uses (as `docker
  stats` does). On the zfs driver the cgroup counts few of a container's
  disk writes, as ZFS writes them later on its own: the disk chart reads
  low there; the data's size is exact.
- **Secrets.** Generated per tenant and sealed with the node's master key
  (AES-256-GCM, bound to tenant and name). The database never holds them in
  the clear.
