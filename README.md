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
  Containers of deleted tenants are removed. Tenants keep running while the
  agent is down; a restarted agent adopts them without restarting them.
- **Edge proxy.** Routes `<tenant>.<baseDomain>` to the tenant's container. It
  streams responses (server-sent events) and replaces any client-sent
  `X-Forwarded-*` headers. TLS is `off`, `internal` (a local CA, for testing) or
  `acme`, which gets a wildcard certificate through DNS-01 from Hetzner Cloud DNS
  or Namecheap.
- **Local API.** A unix socket the `corral` CLI talks to.
- **Volumes.** The `zfs` driver gives each tenant a dataset. The `dir` driver
  uses plain directories and is for development.
- **Secrets.** Generated per tenant and sealed with the node's master key
  (AES-256-GCM, bound to tenant and name). The state file never holds them in
  the clear.

## Install on a node

```bash
sudo install -m 0755 corral /usr/local/bin/corral
sudo install -d -m 0755 /etc/corral
sudo install -m 0644 deploy/agent.example.yaml /etc/corral/agent.yaml   # then edit it
sudo corral init                       # creates the master key (store a copy offline) and an empty state
sudo install -m 0644 deploy/corral-agent.service /etc/systemd/system/
sudo systemctl enable --now corral-agent
```

With the `zfs` driver, create the parent dataset first, for example
`zfs create -p tank/corral/tenants`, and make Docker start after ZFS has mounted
everything: install `deploy/docker-after-zfs.conf` as
`/etc/systemd/system/docker.service.d/corral.conf`. If a dataset still mounts
late, the agent sees in the container's mount table that it runs on the empty
directory underneath and replaces it.

The agent refuses to start without its state file. A new node gets one from
`corral init`; on an existing node a missing file means a broken mount or a
lost disk, and starting from an empty state would remove every tenant.

## Run tenants

```bash
corral tenant create acme -file examples/app.example.yaml -wait 3m
corral tenant list
corral tenant secret acme ADMIN_PASSWORD
corral tenant set-image acme ghcr.io/example/app@sha256:<new digest> -wait 3m
corral tenant logs acme -tail 100
corral tenant suspend acme && corral tenant resume acme -wait 3m
corral tenant delete acme -yes            # keeps the data; add -purge to delete it
corral tenant purge-data acme -yes        # delete data a deleted tenant left behind
```

A new tenant with the name of a deleted one whose data was kept is refused
until that data is purged or deliberately taken over with `create -adopt-data`.
A delete that fails leaves the tenant suspended; run it again.

**Images:**
- Images must be pinned by digest, or be a local `sha256:` image ID.
- Private registries need credentials in `registryAuthFile`, a Docker
  `config.json` with an `auths` entry per registry. Credential helpers are not
  supported. The Docker engine never reads what `docker login` saved; the
  client has to send it.

**Env placeholders:** values may use `{{name}}`, `{{host}}`, `{{url}}` and
`{{network}}`. `{{network}}` is the tenant network's CIDR; the edge proxy
connects from it, so trust it for proxy headers.

## Security model

- **The CLI is root-equivalent.** Anyone who can use the API socket (root, or
  members of `socketGroup`) controls every tenant on the node, including its
  secrets and data.
- **Tenants never run as root.** Specs that set user or data owner 0 are
  refused, and images must not default to root.
- **Every container is locked down.** It drops all capabilities, runs with
  `no-new-privileges`, has resource limits and publishes its port on loopback
  only.
- **Tenant data is out of reach of host users.** Data directories sit below a
  root-only (`0700`) directory, and ZFS datasets are created with `setuid=off`
  and `devices=off`. A setuid file a tenant writes there is useless to host
  users.
- **Optional extra isolation:** enabling Docker's `userns-remap` also maps
  container users away from host users.

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
- delete with purge.

## Known limits (stage 2)

- **Edge inside the agent.** The edge proxy runs in the agent process, so
  public traffic pauses for the few seconds an agent restart takes. Tenant
  containers keep running.
- **Single node.** State is one local file; stage 3 replaces it with a
  replicated log.
- **Failing pulls.** A pull that keeps failing is retried on every pass,
  without backoff.
