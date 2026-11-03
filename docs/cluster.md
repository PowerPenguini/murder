# Running a cluster

Nodes that share one replicated state, keep a copy of each other's tenants, take over when one dies and keep DNS pointed at the living ones.

## Join nodes into a cluster

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

## Move tenants, replace a server

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

## A copy of every tenant on a second node

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

## When a node dies

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
in [docs/failover.md](failover.md).

## DNS records that follow the nodes

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
