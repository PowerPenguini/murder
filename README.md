# Murder

Murder runs one container per tenant across a few servers and, from stage 4,
moves a tenant to another server when its server fails. It is built for small
fleets of single-tenant web apps that keep their state in a local data
directory. An app is configuration (an image and a spec), not code in Murder.
The name is the word for a flock of crows: a murder of crows.

See [docs/design.md](docs/design.md) for the architecture and build stages.
Stage 2 — a single server — is what this code does today.

## What one node does

`murder agent` runs on every server:

- **Reconciler.** Keeps Docker in line with the desired state. Each tenant gets
  its own container, network and data volume. A container is replaced when its
  spec changes, and the volume is snapshotted first if the image changed; a
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
  how they went). Health checks run side by side. A tenant whose app keeps
  exiting shows `restarting` while the engine restarts it; `tenant list`
  counts its restarts and `tenant show` says when it last exited and why,
  such as going over its memory limit (containers get no swap beyond it;
  ones created before this release from their next replacement). Stopping
  the agent lets a container replacement under way finish first, also in a
  pass `murder reconcile` started, so no tenant is left without one.
- **Edge proxy.** Routes `<tenant>.<baseDomain>` to the tenant's container. It
  streams responses (server-sent events) and replaces any client-sent
  `X-Forwarded-*` headers. Over HTTPS it adds `Strict-Transport-Security` (one
  year) unless the app sets its own. TLS is `off`, `internal` (a local CA, for testing) or
  `acme`, which gets a wildcard certificate through DNS-01 from Hetzner Cloud DNS
  or Namecheap.
- **Local API.** A unix socket the `murder` CLI (and, later, the panel) talks to.
- **Database.** One SQLite file holds the node's apps, tenants, sealed secrets
  and an audit log of every change: who (the socket user, plus the person a
  panel acts for), what and when.
- **Volumes.** The `zfs` driver gives each tenant a dataset, limited to its
  disk limit (`refquota`, so snapshots do not count). A limit below what a
  tenant already stores is not applied; `tenant list` shows the tenant with
  a warning and `tenant show` says why. The `dir` driver uses
  plain directories, cannot limit disk use and is for development.
- **Firewall.** An nftables table (`inet murder`) keeps tenants away from
  the host and private networks; see the security model.
- **Secrets.** Generated per tenant and sealed with the node's master key
  (AES-256-GCM, bound to tenant and name). The database never holds them in
  the clear.

## Install on a node

```bash
sudo apt install nftables               # the agent loads the tenant firewall with nft
sudo install -m 0755 murder /usr/local/bin/murder
sudo install -d -m 0755 /etc/murder
sudo install -m 0644 deploy/agent.example.yaml /etc/murder/agent.yaml   # then edit it
sudo murder init                       # creates the master key (store a copy offline) and an empty database
sudo install -m 0644 deploy/murder-firewall.service deploy/murder-agent.service \
  deploy/murder-backup.service deploy/murder-backup.timer /etc/systemd/system/
sudo systemctl enable --now murder-firewall murder-agent murder-backup.timer
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
it replaces beside it (`murder.db.before-restore-<time>`, with its log). The
agent then makes the tenants match the restored state: a tenant created after
the backup is removed with its containers, though its data stays on disk.

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
- Tags are pinned to the registry digest when applied, so tenants keep the
  same bytes until the next `app apply` (or `tenant set -image`) of a tag
  that has moved.
- A tag that only exists locally is pinned to the local image.
- Private registries need credentials in `registryAuthFile`, a Docker
  `config.json` with an `auths` entry per registry. Credential helpers are not
  supported. The Docker engine never reads what `docker login` saved; the
  client has to send it.

Day-to-day operations:

```bash
murder app list                         # apps, their tenants and images
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
```

**What the Docker test does:** it builds a tiny scratch image and runs a tenant
through Murder's real components:
- create, then route through the edge;
- write data, then change the image and keep the data;
- restart the agent and adopt the container;
- suspend and resume;
- archive, restore with the data, purge.

## Known limits (stage 2)

- **Edge inside the agent.** The edge proxy runs in the agent process, so
  public traffic pauses for the few seconds an agent restart takes. Tenant
  containers keep running.
- **Single node.** State is one local SQLite database; stage 3 replicates
  it to other nodes.
- **Registry access during apply.** Tags are pinned when an app is applied
  (or a tenant's image is set), which needs registry access for tags that are
  not yet local.
