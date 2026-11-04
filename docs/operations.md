# Operating Murder

Installing a node, upgrading Murder without stopping tenants, and backups.

## Install

`murder setup` sets a host up as a node, and checks first that it has what
Murder needs: systemd, Docker 25 or newer, nftables and, unless told
`-zfs-pool none`, a ZFS pool. It never installs system packages; it names
the missing ones and the commands that install them. Then it:

- installs the binary as `/usr/local/bin/murder`, the systemd units, the
  sysctl settings and, with ZFS, the Docker drop-in, all from the binary;
- turns on Docker's `live-restore` (in `/etc/docker/daemon.json`, the
  rest kept, the old file beside it as `daemon.json.before-murder`; a
  reload, no container restarts): a restart of Docker, as its upgrade
  does, then leaves tenants running. Swarm mode forbids it; setup says so.
  The agent warns at start while it is off;
- creates the dataset `<pool>/murder/tenants`;
- writes `/etc/murder/agent.yaml` with only what differs from the defaults,
  and the tokens it was given as `0600` files beside it;
- makes the master key and the database (`murder init`);
- starts `murder-firewall`, `murder-agent`, `murder-edge`, the hourly
  backup of the database and, in a cluster with TLS and a public address,
  `murder-panel`;
- starts a cluster of the node, when it has a private address.

It finds the node's name (the host name), its ZFS pool, its private and
public addresses, and asks in a terminal for the rest: the domain, how
certificates are made (Let's Encrypt through Hetzner or Namecheap DNS, a
local CA for testing, or none), the DNS token, the e-mail for Let's
Encrypt. With a Hetzner Cloud token it also powers dead nodes off for
failover (`-hetzner-token-file`); `-manage-dns` has the cluster keep the
wildcard records. `-dry-run` shows the plan and the config; `-yes` asks
nothing. A node set up already is left alone (`-force` writes the config
anew, keeping the master key and database).

`murder setup -join <invitation>` sets a host up as a node of a cluster:
see [joining nodes](cluster.md#join-nodes-into-a-cluster).

To install by hand instead:

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

## Upgrade Murder

An upgrade stops no tenant and drops no request. On any node, with the new
binary:

```bash
sudo murder upgrade ./murder          # every node of the cluster, one after the other, this one last
```

It freezes changes (the panel and CLI only read meanwhile, so nodes of two
releases exchange none) under a freeze of its own, which names the binary,
the node that runs the upgrade and an operation id; it then reads again
what every node runs. Then, node by node: the node checks that the upgrade
still holds the freeze and the binary (its hash, that it runs, no older
schema), takes one install at a time, keeps the binary it runs as
`murder.prev` and a copy of its database, hands its edge over to the new
binary and restarts its agent into it (`murder-agent.service` restarts it:
keep `Restart=always`); the next node follows once this one runs the new
binary, has caught up with the cluster and reconciled; a node that runs
the binary already is not sent it again, but is waited for alike. Then the
upgrade takes changes again, if the freeze is still its own. A node that
refuses the binary keeps its old one, edge included; if it was the first,
and the nodes ran one release, changes are taken again at once. A node that
does not come back stops the upgrade with changes still frozen: on that
node, with its agent stopped, `murder upgrade rollback` (the new binary, in
place) puts the old binary back (and the database from before the upgrade,
if the new release changed its schema); it undoes that node's last install,
nothing older, and only while an upgrade is under way (`-force` otherwise:
the database would go back to before it). It checks first that both can go
back, and changes nothing if not. It puts the database back first, while
the new binary is still in place: a rollback that stops there is run again
the same way, and finishes. It then keeps the new binary as `murder.failed`
and puts the old one in place; should it stop after that, run
`murder.failed upgrade rollback` to finish. Run the rollback from the new
binary, or from `murder.failed`, not from the old one: the old binary of
v0.1.0 puts its binary back first and forgets the upgrade, so a rollback it
began does not resume. Only if the new binary does not run at all, run
`murder.prev upgrade rollback` (the old binary, beside it); from v0.1.0, if
it then fails to put the database back, put the copy back by hand:
`sudo murder restore /var/lib/murder/murder.db.before-upgrade`. Running
the same upgrade again, on the node it ran from, resumes it where it stopped. An upgrade never takes over the
freeze of another, which may still run on another node: once sure that one
stopped and runs nowhere, `murder upgrade -take-over ./murder` takes it
over, say with a fixed binary. An upgrade whose freeze was taken over, or
thawed, installs nothing more: the node checks the freeze again, caught up
with the cluster, once the binary has arrived and before it replaces the
one in place, and again once its edge has handed over; if it went
meanwhile, the node keeps (or puts back) its old binary, edge included,
and does not restart. `murder upgrade -thaw` (or the panel's "take
changes again") ends any freeze: it is for an upgrade that stopped, safe
only once every node runs one release, and must never be used while an
upgrade runs on another node. The panel begins no upgrade while changes are
frozen. Before freezing anything, the upgrade checks that every node
answers. Only root upgrades, and each install is in the audit log
(`node.upgraded`). By hand,
on one node, the same is:

```bash
sudo install -m 0755 murder /usr/local/bin/murder
sudo systemctl reload murder-edge     # the new edge takes the listeners over; the old one finishes its requests
sudo systemctl restart murder-agent   # tenants keep running, the edge keeps serving
```

**From a release before node identity keys,** finish the upgrade promptly.
While any node shows no key in `murder cluster status`, someone with the
master key can still pose as that node, and as any node while a leader of
the older release runs. Each node registers its key within seconds of the
upgrade thawing changes, once every node runs the new release. If a node
still shows no key a minute after the upgrade, check its agent's log
("register this node's identity key").

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
Likewise `murder init` makes no master key for a database that exists: its
secrets are sealed with the original key, which no new one opens; restore
that key from its copy offline.

## Back up and restore

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

## Rotate the master key

The master key seals every tenant's secrets and is what the cluster's
authority, which every node's certificate comes from, is derived from. If
a copy of it leaked (a backup of `/etc/murder`, a laptop), or as routine,
rotate it on every node without stopping one:

```bash
sudo murder key status    # every node's ring: the key in use, older and next ones
sudo murder key rotate    # takes a few tens of seconds
```

The master key file is a ring: one key per line, the first in use, the
others older keys that still open what they sealed and are still trusted
between nodes; `next KEY` is a key on its way in. A rotation, run from any
node:

1. makes a key and sends it to every node as its next key (sent, never
   fetched: a node that only holds the old key gets nothing by asking);
   each node, and its edge, trusts it within seconds;
2. has every node use it: their certificates come from the new authority,
   new secrets are sealed with it;
3. seals every secret again with it, in one change (no container is
   replaced: containers are told apart by digests of their secrets'
   values, not the sealed values; one made by a release before this one is
   replaced once);
4. takes the old key out of every node: from then on, a host with only the
   old key is a stranger to the cluster.

The cluster takes no change while a rotation runs (a few tens of seconds):
no node joins, no tenant is created, no app changes; tenants keep serving.
Every node must answer: bring a stopped node back, or take a failed one out
of the cluster, first. A rotation stopped part way (a node lost meanwhile,
`Ctrl-C`) goes on where it was when `murder key rotate` runs again, from
the node that made the key. Afterwards, copy the new key file to where you
keep the master key, and keep the old key with the database backups made
before: `murder restore` opens their secrets with any key in the file, so
add it as a line when restoring one. A rotation leaves each node's identity
key ([Node identity](cluster.md#node-identity)) as it is: nodes keep their
places. Whoever holds the old key and reaches the nodes' private network
cannot pass for a node whose identity key the cluster holds, during the
rotation either: they cannot read the state, write to it or take the new
key as it is sent. Until the old key is out of every node's ring, they can
still send requests to tenants through a node's edge relay
([Security](security.md)), as anyone can through the public ports. They
also keep what the old key sealed: a copy of the state they took before (a
backup, a removed node's database) opens with it, so rotate the secrets of
tenants whose values matter. Nodes without a key yet, in a cluster from
before identity keys, can still be passed for with the master key alone.
These are the nodes with no key in `murder cluster status` (on a node of
this release), or with no `node.key-registered` in the audit log. Rotate
once every node shows its key. Connections between nodes made before
the rotation stay up until they close; to end them at once, run
`systemctl restart murder-agent` and `systemctl reload murder-edge` on each
node in turn (tenants keep running through both). Invitations made before
a rotation no longer work; make new ones.

## Back tenants up to a bucket

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
sudo murder backups verify acme -identity key.txt   # read it through as a restore would, compare it with its snapshot
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

An archived tenant is backed up too, by the next run, until its newest
backup holds its data as it was archived (nothing written since that
backup's snapshot), and then no more, on request or not: writes between the
last hourly backup and archiving reach the bucket, and a tenant made and
archived between two runs gets its backup. Data that changes after all, as
when a restore replaces it, is backed up again.

A verification reads the newest backup by the rules a restore unpacks it by
(each path once, every entry in a directory an earlier one made, never below
a symlink, only files, directories and symlinks), so a backup that verifies
is one that restores. While the snapshot it was made of is still on the
node, it then compares what a restore puts back: the same entries, of the
same types, owners, modes and times, symlinks to the same targets, files
with the same content.

With `identityFile` set on a node, it verifies its tenants' backups weekly
by itself. An agent that restarts makes no new backup of a tenant backed up
less than a run ago. `/metrics` has
`murder_tenant_backup_last_success_timestamp_seconds` and
`murder_tenant_backup_failing` for each tenant, and, on the leader,
`murder_state_backup_last_success_timestamp_seconds` and
`murder_state_backup_failing` for the cluster's state (a node that stops
leading drops them). `murder backups now` lists the state's backup beside
the tenants' on the leader, and exits with an error when any of them failed,
the state's included; the panel's backup job fails alike. An S3 key file missing or unreadable does not stop
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
