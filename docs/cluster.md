# Running a cluster

Nodes that share one replicated state, keep a copy of each other's tenants, take over when one dies and keep DNS pointed at the living ones.

## Join nodes into a cluster

On a node of the cluster, make an invitation; on the new server, with
Docker, nftables and a ZFS pool, set up with it:

```bash
sudo murder cluster invite                 # valid an hour, once (-valid up to 24h)
sudo murder setup -join 10.0.0.2:7400/fsn-1/…    # on the new server
```

An invitation names the node that made it: its address, its name and the
public half of its identity key. The new server reaches that node on the
private network and talks to it only if the node proves it holds that
key. Anyone else at the address is refused before the secret goes out,
even someone with the cluster's master key. The server then makes its own
identity key (see [Node identity](#node-identity)) and hands over the
invitation's secret with the key's public half. The cluster admits that
key for the server's name for an hour. In return the server takes, over
the same connection, the master key, the node's config, the tokens the
config names (files in `/etc/murder` only) and the members' identity
keys. Invitations of a release before node identity keys
(`address/secret.key`, which name no node) are refused: make a new one. It makes the
config its own: its name (the host name, or `-node`), its address on the
private network (the one on the node's network, or `-cluster-address`),
its public addresses, its ZFS pool and its address in the panel's tunnel.
Then it installs and starts everything as `murder setup` does, and joins.
An invitation is used once, and only when the server is ready for it:
setup checks the server first. It hands the master key to whoever brings
it, so only root makes one, and each one made and used is in the audit
log. A restarted agent forgets the invitations it made. On a host other
people log in to, `murder setup -join -` reads the invitation from
standard input, so it never shows in the process list. A host that has a
master key or a database already, its own or a cluster's it left, is
refused: move them aside first. The node takes at most 8 hosts with
invitations at once, each for 30 seconds.

To join by hand instead, as below: copy the master key, give the node a
`cluster:` section, then join with an invitation.

A cluster keeps the same state (apps, tenants, sealed secrets, the audit
log and its nodes) on every node, replicated with Raft: a change made on any
node is applied on every node, in the same order, and a node that was down
catches up when it returns. With three nodes the cluster takes changes while
any one of them is down; a node cut off from the other two takes none.
Tenants and the edge keep running either way.

Every node runs tenants: each tenant runs on one node, which keeps its
data, and `tenant list` shows which. `murder tenant create` puts a new
tenant on the node running the fewest tenants (live, not suspended; this
node among equals; never a failed one), or on the one `-node NAME` names.
The node it goes to creates it, refusing it if it holds data of an
earlier tenant of that name (a copy, or what a failure left). Creates at
once through one node are placed one after the other, so they spread. Every node's edge serves
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
removed (`murder cluster remove`), with a new invitation.

### Node identity

The master key is shared by every node, so it cannot tell nodes apart:
whoever holds it can make a certificate of the cluster's authority naming
any node. So each node also has an identity key of its own, an ed25519 key
in its cluster directory (`node.key`, readable by root only). The agent
makes it when it first starts, or `murder setup -join` makes it before it
uses the invitation. The node's cluster certificate is issued over that
key, and the cluster's state pins the key's public half to the node's
name. A peer is a member only if the state names it and its certificate
carries that key. This is checked for every request between nodes, every
Raft connection, and every request a node sends, including on connections
it keeps open. The key never leaves its node, and a master key rotation
leaves it as it is. Members are trusted with the state; the keys keep out
those who only have the master key (see [Security](security.md)).

- **Joining** takes an invitation. Redeeming it has the leader admit the
  host's name with its identity key for an hour (`node.admitted` in the
  audit log). The name of a current voter is never admitted. Only an
  admitted host may fetch the state and be added, with that key, and the
  join uses up the admission. A host with the master key alone is
  refused. While it joins, the host trusts only the members the
  invitation named, with their keys.
- **Removing** a node checks everything that could refuse it first: the
  freeze, and tenants on the node or moving to it. Then the node's row
  goes from the state, and the node goes from the voters. A removal that
  stops between the two leaves a voter that Raft takes only by the key it
  had, never by its name alone. A node that had registered no key yet, in
  a cluster from before keys, is taken by its name, as before, until it
  is out of the voters. Running `murder cluster remove NAME` again
  finishes the removal, on any node of the cluster or on the node being
  removed, which is served for its own removal and nothing else. The
  state keeps the departed key only while the node votes: it goes with
  the removal, or with the leader's upkeep once the node is out of the
  voters.
  A leader asked to remove itself hands leadership over first, and the
  next leader removes it. Every request the removed node makes afterwards
  is refused, and its Raft connections are closed within seconds. The
  members log each refusal as a warning, at most once a minute for each
  reason and node. A removed node's agent stops at once: it does not
  mark itself away (see Reboots below) in a cluster it no longer belongs
  to.
- **A cluster from before identity keys** keeps working through a rolling
  upgrade. A node the state holds no key for is taken by its name and the
  master key, as before. Each node on a release with keys registers its
  own key once every node runs such a release and changes are not frozen
  (`murder upgrade` thaws when its last node is done). It does this
  within seconds, through the leader, which takes the key from the node
  whose certificate carries it; `node.key-registered` shows in the audit
  log. From then on the key is required. Until every node shows a key in
  `murder cluster status`, someone with the master key can still pose as
  a node without one, so finish the upgrade promptly. A node of the
  release before cannot join a cluster of this one: set it up with this
  release.
- **A lost `node.key`** (the file deleted, or the cluster directory moved
  aside) makes the node a stranger. Its agent makes a new key as it
  starts, and the other nodes refuse it. To bring it back with its tenants
  and their data, give the cluster its new key:

  ```bash
  sudo murder node key                        # on that node: prints its new identity key
  sudo murder cluster rekey fsn-2 <key>       # on any other node of the cluster
  ```

  The leader puts the new key in place of the old one. It keeps the node's
  place, its tenants and everything else the state holds of it, and
  closes the connections of the old key. It is refused while changes are
  frozen, and is in the audit log (`node.rekeyed`). A node that runs no
  tenant may instead be removed (`murder cluster remove`) and joined
  again with an invitation and a new database. A node that runs tenants
  cannot be removed until they are gone, and cannot be drained while the
  other nodes refuse it.
- **`murder cluster status`** shows each voter's pinned key, "none yet"
  for a node of a cluster from before keys that has registered none, and
  "leaving" for a node out of the state on its way out of the voters,
  with the key Raft still takes it by.

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
-to TARGET` moves the tenants without taking the node out; to reboot a
node, see [Reboot a node](#reboot-a-node).

Tenants added one by one, a node that joined later or one drained for
maintenance leave the nodes uneven. `murder cluster balance` moves tenants,
one after the other and each with its data, from the node running the most
to the one running the fewest, until none runs two more than another (the
panel's Cluster page does the same, as a job). A tenant whose whole copy
the target keeps goes first: its move sends only what changed. Suspended
and archived tenants stay; nothing moves while a tenant moves already.
`-dry-run` prints the moves only.

## Reboot a node

A kernel or C library update waits for a reboot (`murder doctor` says so).
`murder node reboot NODE`, run on another node of the cluster, reboots it
without stopping its tenants for longer than a move:

```bash
sudo murder node reboot n2              # tenants away, reboot, tenants back
sudo murder node reboot n2 -to n3       # all of them to n3
sudo murder node reboot n2 -stay        # leave them where they moved
```

It moves the node's tenants away one after the other, as `murder cluster
drain` does: all to `-to`, or each to the node keeping its whole copy (only
what changed since goes across), else to the node running the fewest. Then
it marks the node away for as long as it waits (`-wait`, 15 minutes), so it
is not failed over while it boots, and the node runs `systemctl reboot`. The
command waits until the node's agent has started anew, caught up with the
cluster and reconciled, then moves the tenants it moved back, those still
where it put them. Moving them back is the default because the command
knows which it moved and waits for the node anyway, and a reboot should
leave the cluster as it found it; with `-stay`, or if the node does not
come back in time, they stay where they moved, and the output says where.

Run on the node it reboots, the command cannot wait for itself: it needs
`-stay`, and returns once the node reboots (`murder cluster balance` evens
the nodes out afterwards).

It changes nothing, and says why, when:

- changes are frozen (an upgrade, a key rotation): reboot once it is over;
- the node does not answer, or no other node can take its tenants (each
  must answer, and be neither failed over nor restarting);
- the node would not come back as it runs: `murder-agent`, `murder-edge`,
  `murder-firewall` (with the tenant firewall), `murder-panel` (if it runs)
  and Docker (`docker.service` or `docker.socket`) must be enabled at boot;
- fewer than a majority of the voters would answer while it reboots, as in
  a cluster of two: the cluster then takes no change until it is back (no
  tenant created, moved or failed over). `-force` reboots it all the same;
- the node runs a release before this command: upgrade it first.

When the node does not reboot after all (a move fails, or an upgrade that
started meanwhile has it refuse), the tenants moved so far move back to
it, as it still runs; if that fails too, the error says where each runs.
Nothing holds other cluster changes off meanwhile, as for `murder cluster
drain`: run one at a time.
The panel does not offer reboots yet.

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
  away for 10 minutes, and its node is not powered off meanwhile. The
  mark goes through the leader; a stop does not wait for it longer than
  5 s, so an agent cut off from the leader still stops promptly (its node
  is then not marked). After the reboot the agent names routes to its
  edge only once it has caught up with the cluster, and the edge holds
  requests until then (`edge.routesHold`, 15 s): its saved routes are
  from before the node went down, and may lead to a tenant that failed
  over or moved since.
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

With namecheap, `publicAddresses` must hold the node's IPv4 address:
Namecheap answers calls from addresses on the account's allow list only,
and the node says which one it calls from.

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
master key, issued over its own identity key, which the state pins to its
name ([Node identity](#node-identity)). `murder key rotate` replaces the
master key on every node without stopping one
([Rotate the master key](operations.md#rotate-the-master-key)). Give each node a `cluster:` section (a node set up alone moves its
top-level `publicAddresses` into it, as `cluster.publicAddresses`):

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
before `murder init` (which keeps a key that is there), start the agent,
make an invitation on a node of the cluster and:

```bash
sudo murder cluster invite                    # on a node of the cluster
sudo murder cluster join 10.0.0.2:7400/fsn-1/…  # on the new node; - reads it from standard input
sudo murder cluster status
```

`murder cluster join` with an address alone, as releases before identity
keys took it, is refused: the master key no longer lets a node in.

Only a node with a new database joins: it takes the cluster's state. Each
cluster has a name, given at bootstrap, so a node holding the state of
another cluster, or of this one before it was rebuilt, is refused; so is a
node with the name of a member at another address. An agent whose config
lost its `cluster:` section refuses to start on a database of a cluster.
`murder cluster remove <node>` takes a node out; to bring it back, give it a
new database and an empty cluster directory. A node is removed only once it
runs no tenant, archived ones included.

A backup is restored into a cluster by rebuilding the cluster around it:

1. On every node, stop the agent and move the cluster directory
   (`cluster.dir`, with `node.key` in it) aside.
2. On one node, `murder restore <backup>`, start the agent, then run
   `murder cluster bootstrap -restored`. This starts a cluster of that node
   alone from the restored state, under a new name, so no node of the
   cluster the backup came from is taken for one of it. It pins the
   node's new identity key in place of the one the backup held for it.
3. On each other node, give it a new database (`murder init` after moving
   the old one aside) and start its agent. Make an invitation on the first
   node and run `murder cluster join <invitation>`. The state already names
   the node, with its tenants, but it is no voter of the rebuilt cluster,
   so the invitation admits it again under its name, with its new key and
   at its address.

A plain `murder cluster bootstrap` refuses a database that names a
cluster.
