# Corral

Corral runs one container per tenant across a few servers and, from stage 4,
moves a tenant to another server when its server fails. It is built for small
fleets of single-tenant web apps that keep their state in a local data
directory. An app is configuration (an image and a spec), not code in Corral.

See [docs/design.md](docs/design.md) for the architecture and build stages.
Stage 2 — a single server — is what this code does today.

## What one node does

`corral agent` runs on every server:

- **Reconciler.** Keeps Docker in line with the desired state. Each tenant gets
  its own container, network and data volume. A container is replaced when its
  spec changes, and the volume is snapshotted first if the image changed.
  Containers of archived or unknown tenants are removed. Tenants keep running
  while the agent is down; a restarted agent adopts them without restarting
  them.
- **Edge proxy.** Routes `<tenant>.<baseDomain>` to the tenant's container. It
  streams responses (server-sent events) and replaces any client-sent
  `X-Forwarded-*` headers. TLS is `off`, `internal` (a local CA, for testing) or
  `acme`, which gets a wildcard certificate through DNS-01 from Hetzner Cloud DNS
  or Namecheap.
- **Local API.** A unix socket the `corral` CLI (and, later, the panel) talks to.
- **Database.** One SQLite file holds the node's apps, tenants, sealed secrets
  and an audit log of every change: who (the socket user, plus the person a
  panel acts for), what and when.
- **Volumes.** The `zfs` driver gives each tenant a dataset. The `dir` driver
  uses plain directories and is for development.
- **Secrets.** Generated per tenant and sealed with the node's master key
  (AES-256-GCM, bound to tenant and name). The database never holds them in
  the clear.

## Install on a node

```bash
sudo install -m 0755 corral /usr/local/bin/corral
sudo install -d -m 0755 /etc/corral
sudo install -m 0644 deploy/agent.example.yaml /etc/corral/agent.yaml   # then edit it
sudo corral init                       # creates the master key (store a copy offline) and an empty database
sudo install -m 0644 deploy/corral-agent.service /etc/systemd/system/
sudo systemctl enable --now corral-agent
```

With the `zfs` driver, create the parent dataset first, for example
`zfs create -p tank/corral/tenants`, and make Docker start after ZFS has mounted
everything: install `deploy/docker-after-zfs.conf` as
`/etc/systemd/system/docker.service.d/corral.conf`. If a dataset still mounts
late, the agent sees in the container's mount table that it runs on the empty
directory underneath and replaces it.

The agent refuses to start without its database. A new node gets one from
`corral init`; on an existing node a missing file means a broken mount or a
lost disk, and starting from an empty database would remove every tenant.
Back it up with `sqlite3 /var/lib/corral/corral.db ".backup /path/corral.db"`
(a plain copy of a live database can be torn), together with the master key.

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
corral app apply apps/web.yaml -wait 3m          # store the app; roll it out to its tenants
corral tenant create acme -app web -wait 3m
corral tenant create beta -app web -env TZ=Europe/Berlin -memory 768
corral tenant set gamma -image ghcr.io/example/web:1.5   # a canary
corral tenant set gamma -app-image                       # back to the app's image
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
(`-memory`, `-cpus`, `-pids`). `tenant set` changes only the flags it is
given, and refuses to overwrite a change someone made since it read the
tenant; `-unset-env`, `-unset-secret` and `-app-image` drop an override, and a
limit of 0 goes back to the app's.

**Secrets** get a value once per tenant, stored sealed. A value is kept even
if the secret leaves the spec and comes back, so a mistaken edit cannot lose
it; `corral tenant rotate` gives it a new value.

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
corral app list                         # apps, their tenants and images
corral tenant list                      # app, state, health, image, URL
corral tenant show acme
corral tenant secret acme ADMIN_PASSWORD
corral tenant rotate acme ADMIN_PASSWORD -wait 3m   # new value; the container restarts
corral tenant logs acme -tail 100
corral tenant suspend acme              # stop it, keep its address (visitors get 503)
corral tenant resume acme -wait 3m
corral tenant archive acme              # take it out of service; data, secrets and overrides are kept
corral tenant list -archived
corral tenant restore acme -wait 3m     # back with its data, secrets and overrides, on the app's current spec
corral tenant purge acme -yes           # delete an archived tenant and its data for good
corral app delete web                   # once no tenant (live or archived) runs it
corral events -tenant acme              # the audit log: who changed what, and when
```

An archive is one change: the edge stops routing to the tenant at once, and
the reconcile pass it starts removes the container and network (a pass that
cannot, retries). A new tenant cannot take an archived tenant's name until
that tenant is restored or purged.

## Security model

- **The CLI is root-equivalent.** Anyone who can use the API socket (root, or
  members of `socketGroup`) controls every tenant on the node, including its
  secrets and data.
- **Tenants never run as root.** Specs that set user 0 are refused, and
  images must not default to root.
- **Every container is locked down.** It drops all capabilities, runs with
  `no-new-privileges` and has resource limits.
- **Tenants cannot reach each other.** Each has its own network (a /28 with
  inter-container traffic off, and Docker isolates networks from each
  other). Nothing is published on the host: the edge proxy, on the host,
  connects to the tenant's fixed address in its network, which other
  tenants cannot reach. An integration test checks this on Docker.
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
through Corral's real components:
- create, then route through the edge;
- write data, then change the image and keep the data;
- restart the agent and adopt the container;
- suspend and resume;
- archive, restore with the data, purge.

## Known limits (stage 2)

- **Edge inside the agent.** The edge proxy runs in the agent process, so
  public traffic pauses for the few seconds an agent restart takes. Tenant
  containers keep running.
- **Single node.** State is one local file; stage 3 replaces it with a
  replicated log.
- **Failing pulls.** A pull that keeps failing is retried on every pass,
  without backoff.
- **Registry access during apply.** Tags are pinned when an app is applied
  (or a tenant's image is set), which needs registry access for tags that are
  not yet local.
