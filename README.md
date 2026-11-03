# Murder

Murder runs one container per tenant across a few servers and, from stage 4,
moves a tenant to another server when its server fails. It is built for small
fleets of single-tenant web apps that keep their state in a local data
directory. An app is configuration (an image and a spec), not code in Murder.
The name is the word for a flock of crows: a murder of crows.

See [docs/design.md](docs/design.md) for the architecture and build stages.
Stage 2 — a single server — is what this code does, and the start of stage 3:
the state replicated between nodes.

## What one node does

`murder agent` runs on every server:

- **Reconciler.** Keeps Docker in line with the desired state. Each tenant gets
  its own container, network and data volume. A container is replaced when its
  spec changes (requests for the tenant wait meanwhile, on every edge, until
  the new container answers, instead of failing), and the volume is
  snapshotted first if the image changed; a
  tenant keeps the last five of those snapshots. Each pass removes registry
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
  it serves while the agent restarts or is down. A request costs it little:
  routes are one lookup in memory, its reverse proxies are shared and lend
  copy buffers from a pool, connections to tenants and to other nodes' edges
  stay open (one HTTP/2 connection to each other edge is kept warm and
  carries its requests side by side; WebSockets get HTTP/1.1 connections of
  their own), it neither compresses nor decompresses bodies, flushes only
  streams as they come, speaks HTTP/2 to browsers over HTTPS, and counts
  requests without a lock. On the host, `deploy/murder-sysctl.conf` keeps a
  connection's sending rate across a pause between requests instead of
  starting it over (a 60 KB response after a click takes one round trip, not
  three); the edge warns at start when it is not set. `murder_edge_routes_timestamp_seconds`
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
  the host and private networks; see the security model.
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
  through) and tells the agent every 15 seconds: requests, errors (5xx and
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

## Install on a node

```bash
sudo apt install nftables               # the agent loads the tenant firewall with nft
sudo install -m 0755 murder /usr/local/bin/murder
sudo install -m 0644 deploy/murder-sysctl.conf /etc/sysctl.d/90-murder.conf
sudo sysctl -p /etc/sysctl.d/90-murder.conf
sudo install -d -m 0755 /etc/murder
sudo install -m 0644 deploy/agent.example.yaml /etc/murder/agent.yaml   # then edit it
sudo murder init                       # creates the master key (store a copy offline) and an empty database
sudo install -m 0644 deploy/murder-firewall.service deploy/murder-agent.service deploy/murder-edge.service \
  deploy/murder-backup.service deploy/murder-backup.timer /etc/systemd/system/
sudo systemctl enable --now murder-firewall murder-agent murder-edge murder-backup.timer
```

### Upgrade Murder

An upgrade stops no tenant and drops no request. On any node, with the new
binary:

```bash
sudo murder upgrade ./murder          # every node of the cluster, one after the other, this one last
```

It freezes changes (the panel and CLI only read meanwhile, so nodes of two
releases exchange none), then, node by node: the node checks the binary
(its hash, that it runs, no older schema), keeps the binary it runs as
`murder.prev` and a copy of its database, hands its edge over to the new
binary and restarts its agent into it (`murder-agent.service` restarts it:
keep `Restart=always`); the next node follows once this one runs the new
binary, has caught up with the cluster and reconciled. Then changes are
taken again. A node that refuses the binary keeps its old one, edge
included; if it was the first, changes are taken again at once. A node that
does not come back stops the upgrade with changes still frozen: on that
node, with its agent stopped, `murder.prev upgrade rollback` (the old
binary, beside the new one, so it works even if the new one does not run)
puts the old binary back (and the database from before the upgrade, if the
new release changed its schema); it undoes that node's last install, nothing older, and only
while an upgrade is under way (`-force` otherwise: the database would go back
to before it). Running the same upgrade again resumes it where it stopped,
and an upgrade with a fixed binary takes over; `murder upgrade -thaw` takes
changes again, safe only once every node runs one release. Before freezing
anything, the upgrade checks that every node answers. Only root
upgrades, and each install is in the audit log (`node.upgraded`). By hand,
on one node, the same is:

```bash
sudo install -m 0755 murder /usr/local/bin/murder
sudo systemctl reload murder-edge     # the new edge takes the listeners over; the old one finishes its requests
sudo systemctl restart murder-agent   # tenants keep running, the edge keeps serving
```

From a release in which the agent served the edge itself, the first
upgrade has a short gap, once: install `murder-edge.service`, install the
binary, `systemctl restart murder-agent` (which frees ports 80 and 443) and
`systemctl enable --now murder-edge`. The edge's metrics (requests per
tenant, certificate expiry) are served from then on at `edge.metrics`.

The reload starts the new binary beside the old one and hands it the open
listeners; only once it serves does the old one stop taking connections,
finish the requests it has, WebSockets included (up to `edge.drain`, 30 s;
what still runs then is closed, and reconnects) and exit. A new edge that does not start leaves the old
one serving. Restarting the agent leaves containers alone and the edge routing.

A new release replaces no container either: a container is replaced only
when what it is made from changes (the tenant's spec, limits, secrets,
address or data path), not when Murder makes containers differently. To
apply such a change, roll the tenants, one at a time, each healthy before
the next; it stops at the first that is not:

```bash
sudo murder tenant roll -all          # this node's tenants; or: murder tenant roll NAME
```

With the `zfs` driver, create the parent dataset first, for example
`zfs create -p tank/murder/tenants`, and make Docker start after ZFS has mounted
everything: install `deploy/docker-after-zfs.conf` as
`/etc/systemd/system/docker.service.d/murder.conf`. If a dataset still mounts
late, the agent sees in the container's mount table that it runs on the empty
directory underneath and replaces it.

The agent refuses to start without its database. A new node gets one from
`murder init`; on an existing node a missing file means a broken mount or a
lost disk, and starting from an empty database would remove every tenant.

### Back up and restore

`murder backup <directory>` asks the agent for a copy of the database as it
is at one moment (a plain copy of a live database can be torn), checks it and
keeps it as `murder-<UTC time>.db`, removing all but the newest 48 (`-keep`).
`murder-backup.timer` runs it every hour into `/var/backups/murder`; copy that
directory off the server as well. The database holds secrets only sealed: keep
the master key apart from it, offline.

To restore, stop the agent and put a backup in place:

```bash
sudo systemctl stop murder-agent
sudo murder restore /var/backups/murder/murder-20261029T150000Z.db
sudo systemctl start murder-agent
```

`murder restore` refuses while the agent runs, refuses a damaged backup and
one whose secrets the node's master key cannot open, and keeps the database
it replaces beside it (`murder.db.before-restore-<time>`). The agent then
makes the tenants match the backup; while the agent still runs, `murder
events` shows what changed since the backup was taken:

- a tenant created since loses its container; its data stays on disk, and
  its name is free again only once purged;
- a tenant archived since runs again, and one purged since comes back with
  an empty data directory;
- secrets rotated since go back to their old values.

### Back tenants up to a bucket

With a `backups:` section, every node backs its tenants up to an S3 bucket
(Hetzner Object Storage, say) every hour: each backup is the **whole** data
of a tenant (database, receipts, everything) from a ZFS snapshot, so of one
moment with nothing stopped, as `tar`, compressed with zstd and encrypted
with [age](https://age-encryption.org) for a public key; the private key
stays offline. Any one backup restores alone, even without Murder:
`age -d -i key.txt < backup.tar.zst.age | zstd -d | tar x`. The cluster's
leader also keeps copies of the cluster's state there.

Backups are thinned out as they age: every one for 48 hours, the first of
each day for 30 days, the first of each month for a year (`keepHourly`,
`keepDaily`, `keepMonthly`). The newest backup of a tenant always stays,
however old, so backups that keep failing never cost the last one that
worked. A purged tenant's backups are thinned out the same way, down to that
newest one; `murder backups forget TENANT -yes` deletes the rest (in
production, once their locks end). Use one bucket per cluster, or per node
without a cluster: backups are kept by tenant name.

```yaml
backups:
  mode: production                 # or test
  endpoint: https://hel1.your-objectstorage.com
  region: hel1
  bucket: acme-murder-backups
  accessKeyFile: /etc/murder/s3.access
  secretKeyFile: /etc/murder/s3.secret
  recipient: age1...               # the public key; age-keygen makes the pair
```

- **`mode: production`** locks backups under S3 object lock (compliance):
  no one, not even someone holding the bucket's keys, deletes one or
  shortens its lock before it ends. The first backup of each day is locked
  for `lockFor` (30 days), the others for as long as they are kept. The
  bucket must be made with object lock: `murder backups setup` does.
- **`mode: test`** locks nothing: `murder backups destroy -yes` deletes every
  backup and the bucket at once. It refuses a production config, and a
  locked bucket.

```bash
sudo murder backups setup                       # make the bucket, or check it fits the mode
sudo murder backups now                         # this node's tenants, now
sudo murder backups list acme
sudo murder backups verify acme -identity key.txt   # read it through, compare every file with its snapshot
sudo murder tenant archive acme
sudo murder backups restore acme -at 2026-11-01T18:50:55Z -identity key.txt
sudo murder tenant restore acme
```

A restore unpacks the backup into a new dataset, apart from the tenant's
data. The agent then puts it in the tenant's place, checking once more that
the tenant is still archived on this node, with no backup, reconcile pass or
purge running beside it. What the tenant had stays beside it, unmounted
(`murder-replaced-…`). A restore that fails, or is stopped, midway is undone
and leaves the tenant's data where it was. A restore needs the tenant in the
cluster's state: after losing a node and its state, restore the state first
(`murder backups state`, then `murder restore`). Murder never deletes
either kind of dataset by itself: once the restored data is checked, remove
what was put aside with `zfs destroy -r tank/murder/murder-replaced-…`. While
it exists, Murder makes no new empty dataset for that tenant, so the old
data is not hidden by mistake. A `murder-copy-…` dataset is a restore that
was interrupted and can be removed the same way.

With `identityFile` set on a node, it verifies its tenants' backups weekly
by itself. An agent that restarts makes no new backup of a tenant backed up
less than a run ago. `/metrics` has
`murder_tenant_backup_last_success_timestamp_seconds` and
`murder_tenant_backup_failing` for each tenant; `murder backups now` exits
with an error when any backup failed. An S3 key file missing or unreadable does not stop
the agent: it runs without backups, logs why every minute and sets
`murder_backups_ready` to 0 until the file is fixed. A missing
`identityFile` skips only the verifications. A recipient that is no age key
stops the agent, as any other mistake in the config does.

Hetzner Object Storage drops a lock given with a multipart
upload: Murder locks each backup after it is stored and reads the lock
back, failing the backup if it is not there.

Limits to know:

- Monthly backups are kept for a year but **locked for only `lockFor`**
  (30 days); after that, someone with the bucket's keys could delete them.
  Raise `lockFor` to lock longer, which also costs longer.
- One backup holds at most about 156 GiB compressed (10,000 parts of 16 MiB,
  the S3 limit).

### Join nodes into a cluster

A cluster keeps the same state (apps, tenants, sealed secrets, the audit
log and its nodes) on every node, replicated with Raft: a change made on any
node is applied on every node, in the same order, and a node that was down
catches up when it returns. With three nodes the cluster takes changes while
any one of them is down; a node cut off from the other two takes none.
Tenants and the edge keep running either way.

Every node runs tenants: each tenant runs on one node, which keeps its
data, and `tenant list` shows which. A tenant runs on the node it was
created on (run `murder tenant create` there). Every node's edge serves
every tenant: a request for a tenant of another node goes on, over TLS on
the private network, to that node's edge (`edge.relayPort`, 7402, the same
on every node), so DNS may point at any node. If the tenant moved while the
request waited for it there, the request follows it, once. A node switched
off is noticed within seconds (HTTP/2 pings), and its tenants' requests fail
(502) instead of waiting.

Only members reach a cluster: Raft connections and requests between nodes
carry the cluster's name, so nodes of another cluster with the same master
key, such as the one a backup was restored from, are refused, as is a node
removed from it. A node brought back with a new database rejoins only once
removed (`murder cluster remove`).

### Move tenants, replace a server

`murder tenant move NAME -to NODE` moves a tenant with its data: everything
is copied while it runs (a ZFS stream, or a tar of a directory), then it
stops, what changed meanwhile follows, and the other node starts it. It is
away for seconds; requests for it wait meanwhile, on every edge, instead of
failing, until it is healthy on the new node (a minute at most). Its data is
removed from the old node once it is healthy on the new one. A move that
fails before the handover leaves it where it was, running; one cut short,
say by an agent restart, is undone by that node's agent within a minute.

To swap a server for a bigger or smaller one, join the new server, then:

```bash
sudo murder cluster replace old-node new-node   # moves every tenant, one after the other, then old-node leaves
```

Point the DNS records at the new server once it has joined (until then its
edge answers 503, "starting"; the old one's serves every tenant it knows),
and switch the old one off. `murder cluster drain NODE
-to TARGET` moves the tenants without taking the node out.

### A copy of every tenant on a second node

In a cluster of ZFS nodes, every tenant has a **replica**: another node
that keeps a copy of its data, never mounted, to run it from if its node
fails. The cluster's leader picks the replicas and spreads them over the
nodes; when a node joins or comes back, it moves copies to it one at a
time (`replicaNext`), each tenant keeping its old copy until the new one
is whole. Every 15 s, the node that runs a tenant snapshots its data and sends
the replica what changed since the copy's newest snapshot; when nothing
changed, nothing is sent. So the copy is at most about 15 s behind, plus
the time a send takes. A whole copy goes in beside the old one, so the node
keeps a copy throughout, and only a copy that holds the whole data may take
over (`replicaReady`). A move onto the replica's node sends only what
changed since that copy. A node
takes a copy only from the node that runs the tenant, so a node that lost
its tenants never writes over the copies that took over.

- **`tenant show`:** shows the replica, whether its copy holds the whole
  data and may take over (`replicaReady`), and when it last matched the
  data (`replicaSyncedAt`), when asked on the node that runs the tenant.
- **Connections between nodes** dial within 10 s; one waiting on a peer
  that vanished ends after about 30 s (TCP keepalive), and a send whose
  peer answered no ping for 30 s is cancelled. So a replica that vanishes
  holds up no other tenant's sends, while one that is merely slow to take
  a large copy is not cut off.
- **Metrics:** `/metrics` has `murder_tenant_replica_lag_seconds` and
  `murder_tenant_replica_failing` for each tenant.
- **Cleanup:** a copy a node no longer keeps, of a tenant purged or whose
  replica moved, is dropped.
- **A node that comes back with data of tenants it no longer runs:** it
  puts that data aside, unmounted, as `murder-failed-<tenant>-…`, because
  it may hold writes the cluster lost. Murder never destroys such data;
  check it, then run `zfs destroy -r`.

**Containers no longer start with the server.** The engine starts no
tenant container by itself (restart policy `no`). With `on-failure` it
would, at boot, every container a power-off cut off. The agent starts a
container whose app exited at its next pass (`reconcileInterval`), and at
boot only once it has applied what the cluster decided while the node was
down. A node that cannot reach a majority starts none. The agent changes
the policy of every container made by an older release in place, without
a restart, when it starts. This applies to a node outside a cluster too:
its tenants start with its agent after a reboot.

### When a node dies

The cluster's leader probes every node every 2 s. With `cluster.failover`
set, it handles a node that has not answered for 30 s as follows:

1. It powers the node off through the Hetzner Cloud API.
2. It waits until Hetzner confirms the server is off, and until at least
   60 s have passed since the node last answered.
3. It runs the node's tenants from their replicas: each replica's node
   mounts its copy and starts the tenant, and every edge routes to it.

The tenants are away for about 70–90 s. They lose the writes of their last
~15 s of replication, plus any taken until the power-off: up to about 60 s
in the worst case.

```yaml
cluster:
  address: 10.0.0.2:7400
  failover:
    hetzner:
      tokenFile: /etc/murder/hcloud.token   # read/write token of the project
```

The leader never runs a tenant elsewhere unless the old node is confirmed
off, so no tenant ever runs twice. When the power-off cannot be confirmed
(Hetzner's API down, a wrong token, no server of the node's name), or
without `cluster.failover`:

- nothing happens except an error in the leader's log and
  `murder_failover_blocked{node} 1`;
- once you have made sure the node is off, run:

```bash
sudo murder cluster failover n2 -powered-off   # refused while n2 answers
```

Other rules:

- **Majority:** the leader acts only while it hears a majority of the
  nodes, itself included, so a leader that is itself cut off powers no one
  off.
- **Reboots:** an agent that stops cleanly, as for a reboot, marks its node
  away for 10 minutes, and its node is not powered off meanwhile.
- **Wrong server:** the Hetzner server must have the node's private
  address. A wrong name never powers off, or confirms off, another machine.
- **Last-moment check:** if the node answers again before its tenants move,
  or is no longer off, they stay.
- **Upgrades:** nothing fails over while changes are frozen for an
  upgrade, which restarts agents on purpose.
- **One at a time:** one node fails over at a time; three nodes keep a
  majority with one down.
- **No replica:** a tenant without a copy that holds its whole data stays
  where it is. A dead node with no such tenant is not powered off; it is
  only marked failed, so the copies it kept move elsewhere.
- **Two nodes:** losing either leaves no majority, so nothing can fail over.
- **A node that fails over:** it keeps no copies until it answers again.
  Powered on again, it serves nothing it no longer runs and puts that data
  aside; then copies move back to it, one at a time, until no node keeps
  two more than another. Each tenant keeps its whole copy on the old node
  until the new one is whole.
- **Clients:** with `cluster.dns` (below), the leader takes a node that
  does not answer out of the wildcard DNS records after 30 s. Without it,
  the records keep naming the dead node, and browsers that try its address
  first wait for it before trying another.
- **The token:** it can power off and delete every server of its project,
  so keep the cluster in a project of its own.

The design, and why nodes do not stop their own tenants when cut off, is
in [docs/failover.md](docs/failover.md).

### DNS records that follow the nodes

The leader can keep the base domain's wildcard records (`*.<baseDomain>`,
A and AAAA) naming the public addresses of the nodes that serve:

```yaml
cluster:
  publicAddresses: [203.0.113.10, "2001:db8::10"]   # this node's
  dns:
    provider: hetzner                 # Hetzner Cloud DNS, or namecheap
    tokenFile: /etc/murder/dns.token  # namecheap: "user:apikey"
    zone: example.com                 # the base domain's zone; default: baseDomain
    ttl: 60s                          # the default, and the least allowed
```

- **Leaving:** a node that has not answered for 30 s leaves the records,
  unless its agent stopped cleanly (a reboot) or an upgrade freezes
  changes: its edge keeps serving meanwhile.
- **Coming back:** a node is named again once it has answered for 30 s
  without a break.
- **When unsure:** a leader that hears too few nodes writes nothing, a new
  leader writes nothing for its first 30 s, and a family (A or AAAA) is
  never left empty.
- **Every node:** give every node the same `cluster.dns` and its own
  `publicAddresses`. Only the leader writes, and leadership moves; a
  leader without `cluster.dns` writes nothing and says so, and while a
  node that serves has no public addresses nothing is written.
- **One write:** Hetzner replaces a name's records in one action, so the
  name never resolves to nothing.
- **Namecheap:** not yet tried on a real domain. Its API sets a domain's
  whole host list in one call, which Murder does only when the records
  differ; try it on a spare domain first, as the libdns provider may not
  keep every host setting (such as mail) as it was. Namecheap answers only
  allowed addresses: allow every node's public address for its API.
- **Drift:** the records are written again every 5 minutes, so a change by
  hand is undone. Leave the wildcard records to Murder.
- **Metrics:** `murder_dns_records_failing` and
  `murder_dns_records_addresses`, on the leader.

Resolvers keep a removed address for up to the TTL, and some clients
longer: a minute or two after a node dies, new clients no longer try it.

Nodes talk over one TLS port on the private network. A node proves it
belongs to the cluster with a certificate of an authority derived from the
master key, so every node needs the same master key and nothing else is
shared. Give each node a `cluster:` section:

```yaml
cluster:
  address: 10.0.0.2:7400   # this node on the private network; the others reach it there
  dir: /var/lib/murder/cluster
```

On the first node (with its state, if it already runs tenants):

```bash
sudo murder cluster bootstrap
```

On every other node, copy `/etc/murder/master.key` from the first node
before `murder init` (which keeps a key that is there), start the agent and:

```bash
sudo murder cluster join 10.0.0.2:7400   # any node of the cluster
sudo murder cluster status
```

Only a node with a new database joins: it takes the cluster's state. Each
cluster has a name, given at bootstrap, so a node holding the state of
another cluster, or of this one before it was rebuilt, is refused; so is a
node with the name of a member at another address. An agent whose config
lost its `cluster:` section refuses to start on a database of a cluster.
`murder cluster remove <node>` takes a node out; to bring it back, give it a
new database and an empty cluster directory. A node is removed only once it
runs no tenant, archived ones included. A backup is restored into a cluster
by rebuilding it: move the cluster directory aside on every node, restore on
one node, bootstrap there and join the others with new databases.

## The administration panel

`murder panel` serves a web panel over the agent's API: the cluster's nodes
(up, down, failed, the leader), every tenant with its state and its copy, a
tenant's runtime, spec, limits and environment, the audit log, and who may
sign in. Its pages follow the cluster every few seconds. On a tenant's page
you suspend or resume it, roll it (a new container), move it to another
node, archive, restore or purge it (these last ones only with its name
typed), and read its logs; the overview creates tenants. It is built in
stages ([docs/panel.md](docs/panel.md)). Run it on every node
(deploy/murder-panel.service).

It is never on the internet. Every node serves it inside a WireGuard tunnel
that runs in the panel process itself, so nothing changes on the host's
network and tenants cannot reach it; only devices you add reach it:

```yaml
cluster:
  publicAddresses: [203.0.113.10]   # the tunnel's endpoint
panel:
  tunnel:
    address: 10.77.0.1              # this node's, different on every node, below .100
    # network: 10.77.0.0/24         # the same on every node; devices get .100 and up
    # port: 51820                   # UDP, open it in the provider's firewall
  # name: panel.example.com         # panel.<baseDomain> by default
```

The panel needs `edge.tls` (internal or acme): it serves HTTPS with the
wildcard certificate, as `panel.<baseDomain>` (so no tenant may be named
`panel`; rename one from before, or set `panel.name`). The nodes must be a
cluster: the devices are kept in its state. Then, on any node:

```bash
sudo murder admin device add laptop          # a QR code for the WireGuard app
sudo murder admin device add laptop -out laptop.conf
sudo murder admin device list
sudo murder admin device remove laptop       # shut out within seconds, during an upgrade too
```

The device's private key is in the QR code and the configuration, shown
once and kept nowhere (`-out` only ever makes a new file, readable by you).
A node that cannot read the devices as the cluster has them, cut off from
it, lets no device in. Each node is a peer of its own, so the panel stays
reachable through any node that is up. The panel's name must lead to the
nodes' tunnel addresses: with `cluster.dns`, the leader keeps its records
(of the nodes that serve), otherwise add them, or a hosts file entry. Some
home routers drop DNS answers with private addresses; a hosts file entry
works around that.

People sign in with passkeys only: no passwords. Every person has the same
rights, and the audit log names who did what (`jan (panel)`). The first
person is invited from a node:

```bash
sudo murder admin invite jan        # a link, valid 24 hours, once
sudo murder admin user list
sudo murder admin user remove jan
```

Opened on a device in the tunnel, the link adds a passkey (on the device, or
a phone near it) and shows ten recovery codes, once: each adds one passkey,
should every passkey be lost, at the same page (`/enroll`, "an invitation,
or a lost passkey?"). Signed in, the Access page invites others, removes
people and passkeys, makes new recovery codes, and adds or removes devices
(a QR code, shown once). The Cluster page shows each node's release,
drains, removes and fails over nodes, backs up this node's tenants, takes
changes again after a stopped upgrade, and upgrades the cluster with an
uploaded `murder` binary; the slow ones run as jobs, with a page that
follows them. The Apps page creates apps and edits their specs as YAML,
as `murder app apply` reads them (keep the files in git in step); a
tenant's settings page changes its app, image, env, own secrets and
limits, and gives a secret a new value. Secrets' values are never shown.
Sessions last 12 hours; signing in and out is in
the audit log. Should everyone lose everything, `murder admin invite` on a
node starts over.

On a computer without the WireGuard app:

```bash
murder admin connect -device laptop.conf     # then open https://panel.example.com:8443
```

It listens on 127.0.0.1:8443 and passes connections through the tunnel; add
`127.0.0.1 panel.example.com` to the hosts file. As a last resort, root on a
node reaches the panel on its unix socket (`panel.socket`) through SSH:
`ssh -L 7480:/run/murder/panel.sock root@node`, then http://localhost:7480.

To try the pages without a cluster: `go run ./internal/panel/demo`.

## Run tenants

A tenant runs an **app**: a spec shared by every tenant of it, kept in git and
applied from CI or by hand. The tenant adds only what is its own: a name and a
few overrides. Changing an app rolls the change out to all its tenants.

An app spec needs only an image and a user; everything else has a default.
The app is named after its file:

```yaml
# apps/web.yaml
image: ghcr.io/example/web:1.4  # a tag; the agent pins it to a digest when applying
user: "10001:10001"             # numeric uid:gid; never root; also owns the data
healthPath: /api/health         # default "/"
env:
  PUBLIC_URL: "{{url}}"
  TRUSTED_PROXIES: "{{network}}"
secrets: [SESSION_SECRET]       # 32 random bytes, hex; or a mapping such as ADMIN_PASSWORD: password
```

```bash
murder app apply apps/web.yaml -wait 3m          # store the app; roll it out to its tenants
murder tenant create acme -app web -wait 3m
murder tenant create beta -app web -env TZ=Europe/Berlin -memory 768
murder tenant set gamma -image ghcr.io/example/web:1.5   # a canary
murder tenant set gamma -app-image                       # back to the app's image
```

`app apply` reads and checks every file before it changes anything and
reports each app as created, updated or unchanged, with the tenants the
change rolls out to. A change that would make a live tenant invalid is
refused as a whole, before the image is pulled. Archived tenants run the
app's current spec when restored; one that no longer fits is refused then
and can be fixed with `tenant set` while archived. `-wait` is
one budget for all the apps' tenants, watched side by side, so a retry after
a failed rollout fails until they are healthy.

**Overrides** are the tenant's own `-image`, `-env`, `-secret` and limits
(`-memory`, `-cpus`, `-pids`, `-disk`). `tenant set` changes only the flags it is
given, and refuses to overwrite a change someone made since it read the
tenant; `-unset-env`, `-unset-secret` and `-app-image` drop an override, and a
limit of 0 goes back to the app's.

**Secrets** get a value once per tenant, stored sealed. A value is kept even
if the secret leaves the spec and comes back, so a mistaken edit cannot lose
it; `murder tenant rotate` gives it a new value.

**Defaults:**
- `port` 8080, `dataPath` /data, `healthPath` /;
- a read-only root with a 256 MB `/tmp` tmpfs (`writableRoot: true` and `tmpfs` change that);
- the agent's default limits.

**Env placeholders:** values may use `{{name}}`, `{{host}}`, `{{url}}` and
`{{network}}`. `{{network}}` is the tenant network's CIDR; the edge proxy
connects from it, so trust it for proxy headers.

**Images:**
- Tags are pinned to the digest the registry names for them when stored, so
  every node runs the same bytes. An app keeps the image its tag was pinned
  to: applying the same tag again, say with another env, takes no newer
  image by the way, and says when the registry holds one. `murder app update
  web` (or `app apply -update`, for CI that pushes a moving tag) takes the
  tag's newest image and rolls it out; it changes nothing if the registry
  does not answer. Another tag in the file is pinned anew.
- The cluster's leader asks the registry every `imageChecks` (5m; 0 never)
  which image each app's tag names now, without pulling it; a tag on Docker
  Hub at most hourly, as Hub counts each question as a pull. A newer image
  is only recorded: `app list` and the panel show it, the audit log says
  `app.image-found`, and `murder_app_newer_image` is 1 until someone takes
  it. Nothing rolls out by itself. `murder app check` asks at once.
  `murder_app_image_check_failing` is 1 for an app whose registry did not
  answer at the leader's last check. Tenants with an image of their own
  (`tenant set -image`) keep it; their tags are not followed.
- Every change of an app is a revision; the last 20 are kept. `murder app
  history web` lists them and `murder app rollback web` stores the one
  before the current again (`-to N` another), with its image and tag. An
  app stored by an older release gets its first revision at its next change.
- A tag that only exists locally is pinned to the local image.
- Private registries need credentials in `registryAuthFile`, a Docker
  `config.json` with an `auths` entry per registry. Credential helpers are not
  supported. The Docker engine never reads what `docker login` saved; the
  client has to send it.

Day-to-day operations:

```bash
murder app list                         # apps, their tenants, images and newer images found
murder tenant list                      # app, state, health, image, URL; archived tenants too
murder tenant show acme
murder tenant secret acme ADMIN_PASSWORD
murder tenant rotate acme ADMIN_PASSWORD -wait 3m   # new value; the container restarts
murder tenant logs acme -tail 100
murder tenant suspend acme              # stop it, keep its address (visitors get 503)
murder tenant resume acme -wait 3m
murder tenant archive acme              # take it out of service; data, secrets and overrides are kept
murder tenant restore acme -wait 3m     # back with its data, secrets and overrides, on the app's current spec
murder tenant purge acme -yes           # delete an archived tenant and its data for good
murder app check                        # ask the registries now for newer images of the apps' tags
murder app update web -wait 3m          # take the newest image of web's tag and roll it out
murder app history web                  # web's recent revisions
murder app rollback web -wait 3m        # back to the revision before the current one
murder app delete web                   # once no tenant (live or archived) runs it
murder events -tenant acme              # the audit log: who changed what, and when
```

An archive is one change: the edge stops routing to the tenant at once, and
the reconcile pass it starts removes the container and network (a pass that
cannot, retries). A new tenant cannot take an archived tenant's name until
that tenant is restored or purged, and it keeps its network range (and so
its address) until it is purged: no other tenant can be routed to a
container of it that is still running.

## Security model

- **The CLI is root-equivalent.** Anyone who can use the API socket (root, or
  members of `socketGroup`) controls every tenant on the node, including its
  secrets and data.
- **Tenants never run as root.** Specs that set user 0 are refused, and
  images must not default to root.
- **Every container is locked down.** It drops all capabilities, runs with
  `no-new-privileges` and has limits on memory, CPU, processes and (with
  ZFS, once `defaults.diskGB` or the tenant's `diskGB` is set) disk.
- **Tenants cannot reach each other.** Each has its own network (a /28 with
  inter-container traffic off, and Docker isolates networks from each
  other). Nothing is published on the host: the edge proxy, on the host,
  connects to the tenant's fixed address in its network, which other
  tenants cannot reach. An integration test checks this on Docker.
- **Tenants cannot reach the host or private networks.** An nftables table
  (`inet murder`) lets the tenant networks reach only the internet, the
  edge ports (a tenant's own public address resolves to this host), the
  host's DNS servers on port 53 (Docker's embedded DNS forwards to them from
  the tenant's network) and replies to connections the host opened (the
  edge and health checks). The host's other services (SSH, monitoring),
  other servers on a private network and the cloud metadata service
  (`169.254.169.254`) are out of reach.
  - `murder-firewall.service` loads it at boot before Docker, which starts
    the tenants (`systemctl restart murder-firewall` reloads it without
    touching Docker). The agent loads it again at start, refusing to start
    without it, and every 15 seconds, which undoes a flushed ruleset and
    follows changed DNS servers.
  - The rules are IPv4: tenant networks are made without IPv6, and one with
    IPv6 (say from Docker's `default-network-opts`) is refused.
  - They cover `networkPool` and every tenant's subnet, read from the
    database, so tenants left on an earlier pool stay guarded.
  - `firewall: off` turns it off, for development only. Two tests check it:
    `go test -tags netns ./internal/firewall` loads it in network
    namespaces of its own (no root needed) and probes from a tenant
    network, and a test in the test VM probes from a real container.
- **Tenant data is out of reach of host users.** Data directories sit below a
  root-only (`0700`) directory, and ZFS datasets are created with `setuid=off`
  and `devices=off`. A setuid file a tenant writes there is useless to host
  users.
- **Docker's `userns-remap` is not supported.** The data owner is the
  container user's numeric id on the host, which remapping would change.

## Develop

```bash
go test ./...                                   # unit tests
go test -tags docker -count=1 ./integration     # full tenant lifecycle on the local Docker engine
test/vm/go-test.sh ./internal/volume            # ZFS driver tests inside a throwaway VM
make -C test/hetzner up deploy cluster smoke TOKEN_FILE=path   # a real cluster on Hetzner Cloud
make -C test/hetzner down                       # delete it (servers bill by the hour)
```

**The Hetzner cluster:** three nodes in three locations (fsn1, nbg1,
hel1) on one private network, each with a volume for its ZFS pool, and the
test app as tenant `t-<node>` on each. `smoke` reads every tenant through
every edge; `nodes.sh load SECONDS` keeps reading while you run `nodes.sh
upgrade`, `make spare` (a fourth, bigger node) and `murder cluster
replace`, or switch a node off. The token file holds a Hetzner Cloud
project token with read and write access.

**What the Docker test does:** it builds a tiny scratch image and runs a tenant
through Murder's real components:
- create, then route through the edge;
- write data, then change the image and keep the data;
- restart the agent and adopt the container;
- suspend and resume;
- archive, restore with the data, purge.

## Known limits

- **Tenants stay on their node until moved.** A tenant's data lives on the
  node that runs it, with a copy on its replica (ZFS only). When a node
  dies, its tenants fail with 502 on every other edge until they run from
  their replicas, 70–90 s later with `cluster.failover`, or when an
  operator fails the node over. Moving a tenant with the `dir` driver
  copies all its data twice, the second time while it is stopped; ZFS
  sends only what changed.
- **Upgrades.** A node applies changes made by its own release and older
  ones, but stops on a change made by a newer release (and restarts into
  the same stop) until it is upgraded too. `murder upgrade` freezes changes
  until every node runs the new release; upgrading by hand, make no changes
  until every node is upgraded.
- **Registry access during apply.** Tags are pinned when an app is applied
  (or a tenant's image is set), which needs registry access for tags that are
  not yet local.
