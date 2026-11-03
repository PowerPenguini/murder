<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/assets/murder-dark.svg">
    <img src="docs/assets/murder-light.svg" alt="Murder's mark: three crows' heads, one behind the other" width="180">
  </picture>
</p>

# Murder

Murder runs single-tenant web apps on a few servers: one container per
tenant, each with its own data directory, network and subdomain. It keeps a
copy of every tenant's data on a second server and moves the tenant there
when its server dies. The name is the word for a flock of crows: a murder of
crows.

It is built for small fleets, a handful of servers and tens of tenants, of
apps that keep their state in a local directory (say SQLite and uploaded
files). Kubernetes and Nomad leave the hard part, moving that local data
between servers, to you; Murder does it with ZFS. An app is configuration
(an image and a short spec), not code in Murder.

## What it does

- **Tenants.** Each gets a container, an isolated network, a ZFS dataset
  with a disk limit, generated secrets sealed with the node's master key,
  and `<tenant>.<your domain>`. Apps are specs shared by their tenants;
  a tenant adds only its own overrides. [Apps and tenants](docs/apps.md)
- **Edge proxy on every node.** Routes by host name to wherever the tenant
  runs, with wildcard TLS through ACME DNS-01 (Hetzner DNS or Namecheap).
  It keeps serving while the agent restarts. [What a node runs](docs/node.md)
- **A cluster of equal nodes.** The state (apps, tenants, secrets, audit
  log) is SQLite on every node, kept the same by Raft. Every node runs
  tenants. [Running a cluster](docs/cluster.md)
- **A copy of every tenant on a second node.** ZFS sends what changed every
  15 seconds. Moving a tenant or draining a server loses no data.
- **Automatic failover.** A node that stops answering is powered off
  through the Hetzner Cloud API, then its tenants start from their copies:
  about a minute of downtime, at most 30 seconds of writes lost. DNS records
  follow the nodes that serve. [Failover](docs/failover.md)
- **Upgrades that stop nothing.** Murder upgrades itself node by node
  without restarting a tenant or dropping a request. Apps follow an image
  tag in their registry (GHCR or any other): a newer image is offered, an
  update rolls it out, and every change of an app is a revision to go back
  to. [Operating Murder](docs/operations.md)
- **Backups.** Every tenant's whole data, compressed and encrypted with
  `age`, to an S3 bucket, with object lock in production.
- **An administration panel.** Signed in with passkeys, reached through a
  WireGuard tunnel built into Murder, never public. [The panel](docs/panel.md)
- **Isolation.** Tenants never run as root, drop every capability, cannot
  reach each other, the host or private networks (nftables).
  [Security model](docs/security.md)
- **Metrics** in the Prometheus format, and an audit log of who changed what.

## Status

Murder is young. It was built for one company's apps and is tested on real
three-node clusters on Hetzner Cloud, including power-offs, network splits
and upgrades under load, but it has no production users yet. The
database upgrades itself in place; the command line and the API may still
change.

## Requirements

- Linux with Docker 25 or newer and nftables.
- ZFS for disk limits, copies and failover; the `dir` driver runs on any
  file system, for development.
- Hetzner Cloud for automatic failover, which needs to power a dead server
  off; elsewhere a node is failed over by hand.
- Go 1.26 to build.

## Quick start

On a server with Docker, nftables and a ZFS pool (setup says what is
missing, and how to get it):

```bash
go build -o murder ./cmd/murder
sudo ./murder setup -domain example.com
```

`setup` installs Murder and its services, writes a short
`/etc/murder/agent.yaml`, makes the master key and the database, starts
everything and, on a private network, starts a cluster of this node. It
asks only for what it cannot find out, such as a DNS token for Let's
Encrypt; every answer is also an option, for scripts (`murder setup -h`).

More nodes join with an invitation, and take the master key, the config
and its tokens from the cluster:

```bash
sudo murder cluster invite                      # on a node: prints the command for the next one
sudo ./murder setup -join 10.0.0.2:7400/…       # on the new server
```

Then describe an app and run a tenant of it:

```yaml
# web.yaml: the app is named after its file
image: ghcr.io/example/web:1.4   # pinned to a digest when stored
user: "10001:10001"              # never root; owns the data
healthPath: /health
env:
  PUBLIC_URL: "{{url}}"
secrets: [SESSION_SECRET]        # generated per tenant
```

```bash
murder app apply web.yaml
murder tenant create acme -app web -wait 3m    # https://acme.<baseDomain>
murder tenant list
```

What setup does, and how to do it by hand, is in
[Operating Murder](docs/operations.md); clusters are in
[Running a cluster](docs/cluster.md).

## Documentation

| | |
| --- | --- |
| [docs/apps.md](docs/apps.md) | apps, tenants, overrides, secrets, image updates and revisions |
| [docs/node.md](docs/node.md) | what the agent, the edge and the volumes do on a node |
| [docs/operations.md](docs/operations.md) | install, upgrade Murder, back up and restore |
| [docs/cluster.md](docs/cluster.md) | join nodes, move tenants, copies, a dead node, DNS |
| [docs/failover.md](docs/failover.md) | how failover decides, and what was tested |
| [docs/panel.md](docs/panel.md) | the administration panel |
| [docs/security.md](docs/security.md) | what tenants can and cannot reach |
| [docs/design.md](docs/design.md) | the design and why |
| [docs/develop.md](docs/develop.md) | tests, the test VM and a test cluster on Hetzner Cloud |

## Known limits

- A tenant's data lives on its node and its copy (ZFS only). When a node
  dies, its tenants are down until they start from their copies, 70 to 90
  seconds later with automatic failover.
- During an upgrade by hand, make no changes until every node runs the new
  release; `murder upgrade` takes care of that.
- One container per tenant, one app per tenant, HTTP only.

## License

Apache License 2.0; see [LICENSE](LICENSE).
