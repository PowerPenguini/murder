# Operating Murder

Installing a node, upgrading Murder without stopping tenants, and backups.

## Install

`murder setup` sets a host up as a node, and checks first that it has what
Murder needs: systemd, Docker 25 or newer, nftables and, unless told
`-zfs-pool none`, a ZFS pool. It never installs system packages; it names
the missing ones and the commands that install them. Then it:

- installs the binary as `/usr/local/bin/murder`, the systemd units, the
  sysctl settings, the kernel hardening (see [Harden the node](#harden-the-node))
  and, with ZFS, the Docker drop-in, all from the binary;
- turns on Docker's `live-restore` (in `/etc/docker/daemon.json`, the
  rest kept, the old file beside it as `daemon.json.before-murder`; a
  reload, no container restarts): a restart of Docker, as its upgrade
  does, then leaves tenants running. Swarm mode forbids it; setup says so.
  The agent warns at start while it is off;
- turns SSH's password logins off, when sshd takes passwords and the
  account running setup (the one `sudo` was run from, or root) has a key
  in its `authorized_keys`: `PasswordAuthentication no` and
  `KbdInteractiveAuthentication no` in `/etc/ssh/sshd_config.d/00-murder.conf`,
  then a reload of sshd, which keeps open sessions. sshd takes the first
  value it reads, so the file is named to come before cloud-init's
  `50-cloud-init.conf`, which often turns passwords on. Setup checks the
  result with `sshd -T` and takes the file away if sshd refuses it or a
  setting read earlier still turns passwords on. It reads sshd's settings
  for your own connection (`sshd -T -C` with the account and the address in
  `SSH_CONNECTION`), so a `Match` block that gives you another key file or
  asks for a password too counts; when it cannot tell the address and the
  config has a `Match` block, it leaves SSH as it is. Without a key, or with an
  `AuthenticationMethods` that asks for more than a key, it leaves SSH as
  it is and says so; `-keep-ssh` leaves it alone anyway;
- turns the host firewall on (`hostFirewall`, see
  [Harden the node](#harden-the-node)), with SSH's ports and what listened
  when setup ran left open;
- creates the dataset `<pool>/murder/tenants`;
- writes `/etc/murder/agent.yaml` with only what differs from the defaults,
  and the tokens it was given as `0600` files beside it;
- makes the master key and the database (`murder init`);
- starts `murder-firewall`, `murder-agent`, `murder-edge` (with
  `murder-edge.socket`, which holds its ports; see
  [Restart the edge](#restart-the-edge)), the hourly backup of the database and, in a cluster with TLS and a public address,
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
sudo install -m 0644 deploy/murder-hardening.conf /etc/sysctl.d/91-murder-hardening.conf
sudo sysctl -p /etc/sysctl.d/91-murder-hardening.conf
sudo install -d -m 0755 /etc/murder
sudo install -m 0644 deploy/agent.example.yaml /etc/murder/agent.yaml   # then edit it
sudo murder init                       # creates the master key (store a copy offline) and an empty database
sudo install -m 0644 deploy/murder-firewall.service deploy/murder-agent.service deploy/murder-edge.service \
  deploy/murder-edge.socket deploy/murder-backup.service deploy/murder-backup.timer /etc/systemd/system/
sudo systemctl enable --now murder-firewall murder-agent murder-edge murder-backup.timer
sudo murder doctor                     # what is left to harden; changes nothing
```

## Harden the node

`murder doctor` checks how safely the node is set up, and changes nothing:
it needs neither the agent nor Docker running, and runs best as root (what
it cannot see as another user it skips and says so). Each finding comes
with its fix; it exits 1 when one failed.

`murder doctor -fix` first does, on a node set up already, what setup does
on a new one where that is safe on a running node, after showing the list
and asking (`-yes` asks nothing): SSH stops taking passwords when the
account running it logs in with a key (see [Install](#install)); Docker's
`live-restore` goes on (a reload, no container stops); and on a node alone
the host firewall goes on in the config, with SSH and what listens now left
open, the old config kept beside it as `agent.yaml.before-host-firewall`,
then `murder-agent` and `murder-firewall` restart (tenants keep running).
The nodes of a cluster share a config, so there it prints the
`hostFirewall` to add to each instead. Then it checks as usual.

- **Murder's files.** The master key and every token are root's and `0600`,
  the config is writable by root only (whoever writes it runs code as
  root) and the database's directory is `0700`.
- **The firewall.** The tenant firewall is on and loaded, and the host
  firewall is on (or another firewall filters the host's ports).
- **Services and backups.** Murder's units start at boot, the database was
  copied in the last 3 hours and tenants are backed up to a bucket.
- **SSH.** From sshd's effective configuration (`sshd -T`, so `Include` and
  `Match` count): no passwords, no root login with one, and every port
  sshd listens on open in `hostFirewall.ssh` (from `sshd -T`, and from the
  sockets sshd holds, which a `ListenAddress` with its own port adds).
- **Listeners.** What listens on a public or any address besides SSH, the
  edge and the panel's tunnel, by process. The Docker API over TCP fails
  (whoever reaches it is root); a port a container publishes is flagged
  even with the host firewall, as Docker forwards it before the host's
  rules see it.
- **Docker.** `live-restore`, so a Docker upgrade does not stop tenants;
  seccomp; members of the `docker` group, who are root in all but name.
- **Updates.** On Debian and Ubuntu, that `unattended-upgrades` installs
  security updates, and whether one waits for a reboot (see [Reboot a
  node](#reboot-a-node)).
- **The edge's confinement.** Whether the running edge is confined (see
  [security](security.md)). On a node installed before, the agent of this
  release brings `murder-edge.service` up to date at start (see
  [Upgrade Murder](#upgrade-murder)); then `systemctl restart murder-edge`:
  a reload hands over to a child of the old, unconfined process, so only a
  restart confines it (open requests get `edge.drain` to finish; with
  `murder-edge.socket` holding the ports, new connections wait for the
  new edge rather than being refused). `journalctl -u murder-edge` then shows any
  call the filter refused, and one `systemctl reload murder-edge` proves
  the handover still works.
- **The edge's ports across a restart.** Whether `murder-edge.socket`
  holds them (see [Restart the edge](#restart-the-edge)); on a node
  upgraded from before it, `systemctl stop murder-edge.service && systemctl
  start murder-edge.socket murder-edge.service`, once, starts it: the edge
  lets the ports go before the socket takes them.
- **Kernel and clock.** The settings `deploy/murder-hardening.conf` sets
  (no ICMP redirects, loose reverse path filtering, SYN cookies, kernel
  addresses, log and BPF kept from users; `ip_forward` stays on for
  Docker), and that the clock is synchronised, which certificates, passkeys
  and the cluster rely on. `murder setup` installs them; on a node
  installed by hand:

```bash
sudo install -m 0644 deploy/murder-hardening.conf /etc/sysctl.d/91-murder-hardening.conf
sudo sysctl --system
```

The host firewall is off in a config without `hostFirewall`, as it closes
every port the host serves besides SSH and Murder's. `murder setup` turns
it on (`-no-host-firewall` leaves it off): `hostFirewall.ssh` gets the
ports sshd listens on (`sshd -T`, and its sockets, a socket-activated
ssh's too), and what else listened beyond loopback when setup ran goes in
`open`, or in `private` when it listened on a private address only, each
with a comment naming the process, so setup closes nothing that served.
A host joining a cluster (`setup -join`) adds its own SSH ports and
listeners to the cluster's `hostFirewall` the same way.
Take out what should not be public. Docker's published ports, the DHCP
client, mDNS and LLMNR are left out: the first pass the host's input rules
anyway, and the others need no opening. With `hostFirewall` set, the same table
drops what reaches the host from outside unless it goes to SSH (port 22,
or `hostFirewall.ssh`), the edge ports, the panel's tunnel or a port in
`hostFirewall.open`; the cluster port, the relay port and metrics not on
loopback (and `hostFirewall.private`) are open to private networks only.
Replies, loopback, ICMP and DHCP pass. It covers IPv4 and IPv6.

```yaml
hostFirewall:
  ssh: [22]
  open: ["9100/tcp"]        # to everyone
  private: ["9100/tcp"]     # to private networks only
```

Run `murder doctor` first, which fails if sshd listens on a port
`hostFirewall.ssh` leaves out, then turn it on with `systemctl restart
murder-agent murder-firewall` (tenants keep running): the agent reads the
config only at start, and loads its rules again every 15 seconds, so
restarting `murder-firewall` alone is undone within 15 seconds. SSH
connections already open survive. Locked out all the same, from the
provider's console: remove `hostFirewall` from the config and run the same
restart (`nft delete table inet murder` alone is undone by the agent as
well). Other containers on the host (outside Murder's tenant networks)
reach only the private ports.

## Reboot a node

An update of the kernel or the C library takes effect at the next reboot;
`murder doctor` warns while one waits. Reboot with `murder node reboot`,
which checks first that the node comes back as it runs: Murder's units and
Docker start at boot (`systemctl enable` the one it names otherwise),
changes are not frozen, and no other operation (a drain, a balance) holds
the cluster: the reboot holds it itself meanwhile (see
[One operation at a time](cluster.md#one-operation-at-a-time)).

In a cluster, run it on another node, naming the node to reboot: its
tenants move to other nodes, it reboots, and once it is back they move
back, each away only for the seconds of a move (see
[Reboot a node](cluster.md#reboot-a-node)).

```bash
sudo murder node reboot n2
```

On a node alone, nothing takes its tenants over: they stop, and its edge
answers no request, until it is back; Docker and Murder's units start them
again at boot. It says so and asks first; `-yes` does not ask.

```bash
sudo murder node reboot          # asks
sudo murder node reboot -yes
```

The reboot is in the audit log (`node.rebooting`).

## Upgrade Murder

An upgrade stops no tenant and drops no request. On any node, with the new
binary:

```bash
sudo murder upgrade ./murder          # every node of the cluster, one after the other, this one last
```

It freezes changes (the panel and CLI only read meanwhile, so nodes of two
releases exchange none), refused while a drain, a balance, a replace or a
reboot holds the cluster (see
[One operation at a time](cluster.md#one-operation-at-a-time)): it begins
once that one ended, under a freeze of its own, which names the binary,
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

**Units and sysctl settings follow the binary.** The agent, started from
`/usr/local/bin/murder` as root, compares the systemd units, the sysctl
files and the Docker drop-in the node has with those of the release it
runs, replaces any that differ, then runs `systemctl daemon-reload` and
applies the sysctl files; it logs what it replaced (a reload that failed is
done at the next start). A file the node does not have stays out, as one
left out on purpose cannot be told from one a later release added: `murder
doctor` names settings missing, such as the kernel hardening on a node set
up before it. The one exception is a file a release adds beside one the
node has: `murder-edge.socket` comes with `murder-edge.service`. Nothing restarts: services run under a changed unit from
their next start. When the config moves `edge.tls.storage`, `edge.routes`
or `edge.pidFile` out of `/var/lib/murder` and `/run/murder-edge`, the
agent also makes those directories and writes
`murder-edge.service.d/murder-paths.conf` with `ReadWritePaths=` for them,
so the confined edge can still write there. When the config's edge
addresses are not the socket's own (ports 80 and 443 on all addresses:
TLS off, other ports or addresses, or a cluster, whose relay the socket
holds too), it writes `murder-edge.socket.d/murder-listen.conf` with
them. Change a unit with a drop-in (`systemctl edit
murder-edge`), which stays; an edit of the unit itself is replaced. When
`murder-edge.service` changed, the agent says so: a reload hands over to a
child of the old edge, which keeps the old unit's limits, so run `sudo
systemctl restart murder-edge` (open requests get `edge.drain` to finish)
for the edge to run under the new one. With `murder-edge.socket` holding
the ports, that restart drops no connection; on the upgrade that brings
the socket, that restart is the one that starts it, and the last to
refuse connections for a moment. When the socket's addresses changed, the
agent says to stop and start both (see [Restart the edge](#restart-the-edge)).
`murder doctor` reports an edge still unconfined, and ports the socket
does not hold.

**From a release before node identity keys,** finish the upgrade promptly.
While any node shows no key in `murder cluster status`, someone with the
master key can still pose as that node, and as any node while a leader of
the older release runs. Each node registers its key within seconds of the
upgrade thawing changes, once every node runs the new release. If a node
still shows no key a minute after the upgrade, check its agent's log
("register this node's identity key").

**From a release before tenants' own domains** (schema 17), the
upgrade adds the domains' tables. The leader checks and orders nothing
for domains until every node runs schema 18 or later, so finish the
upgrade before adding domains: until then, the leader asks the nodes
which release they run at each of its passes.

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

### Restart the edge

A reload (`systemctl reload murder-edge`) is how to replace the edge: the
old and the new one overlap. A restart cannot overlap them: systemd starts
the new edge only once the old one has exited, after finishing its
requests (at least 2 s, at most `edge.drain`). So that this gap refuses no
connection, `murder-edge.socket` holds the edge's ports: systemd binds
them, passes them to every edge it starts, and keeps them open while none
runs. A connection that arrives during a restart, or in the second after a
crash before systemd starts the edge again, waits in the kernel's queue
and is served by the next edge. In a test under systemd, ten restarts 3 s
apart, with a new connection every 20 ms to each of ports 80 and 443,
refused 1121 of 2714 connections without the socket and none of 1751 with
it; the slowest was answered after 3.9 s. A crash still loses the
connections the dead edge had accepted (4 of 2333 in the same test, with
`kill -9` for a restart), not those waiting.

- `murder-edge.service` pulls the socket in (`Wants=`); it is not enabled
  on its own. If the socket cannot start, the edge binds its ports itself,
  as it does outside systemd.
- Since a connection to the socket starts the edge, stopping the edge for
  good takes `systemctl stop murder-edge.socket murder-edge.service`.
- The socket holds the addresses of `edge.http`, `edge.https` (with TLS)
  and, in a cluster, the relay; the agent writes them to
  `murder-edge.socket.d/murder-listen.conf` when they are not the unit's
  own. `edge.metrics` is not held: a scrape missed during a restart is no
  loss. A host name in an address is left to the edge to bind.
- systemd applies the socket's addresses only when the socket starts, and
  it refuses to start the socket while the edge runs. After changing them,
  `sudo systemctl stop murder-edge.socket murder-edge.service; sudo
  systemctl start murder-edge` (`systemctl restart` of both at once fails
  to order them). That closes the ports for as long as the old edge
  drains. An edge started with a socket out of date with its config says
  so in its log: it serves a socket on the same port at other addresses
  rather than fail to bind beside it, and binds an address the socket
  lacks itself.

The panel's WireGuard tunnel (`panel.tunnel.port`, UDP 51820 by default)
is the panel's, not the edge's: wireguard-go runs in `murder-panel`, and the
port closes while the panel restarts (on an upgrade, or after a crash).
No socket holds it, as `murder-edge.socket` holds the edge's ports,
because holding it would gain nothing. The tunnel's session keys and the
TCP connections inside it live in the panel process: a new panel knows
neither, so it drops the packets a device sent the old one, whether they
waited in a held socket or were refused. A device gets through again once
it makes a new handshake. It does so on its own, about 15 s after its
packets go unanswered (WireGuard's keepalive and rekey timeouts). In a
test of the tunnel's two ends, a page was reached again 15.3 s after the
panel's end restarted with no gap, and 12.2 s after it restarted 3 s
later: 15 s after it stopped either way. A browser that was open on the
panel reloads once it is through.

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

## Give a tenant its own domain

A tenant answers at `<tenant>.<baseDomain>`. It may also have host names
of its own, such as a customer's `app.customer.com`, with a certificate
the cluster orders and renews:

```bash
sudo murder tenant domain add acme app.customer.com   # prints the DNS record to create
sudo murder tenant domain list acme                   # state, DNS record, certificate, why
sudo murder tenant domain check acme app.customer.com # check its DNS now, after changing it
sudo murder tenant domain remove acme app.customer.com -yes
sudo murder tenant domain primary acme app.customer.com # redirect its other hosts there
sudo murder tenant domain primary acme -none            # stop redirecting
```

The panel's tenant page does the same (Domains). A name is kept in lower
case, as IDNA's ASCII form (`bücher.example` is `xn--bcher-kva.example`),
without a trailing dot. It is refused when it is a URL, has a port or a
wildcard (give each name of its own), is an address or a single label, is
a public suffix (`co.uk`), or is the base domain or below it. A name is one
tenant's, and a tenant has 20 at most. Domains go with their tenant
through moves, failovers, archiving and restoring, and the cluster's
backups; purging the tenant releases them.

**DNS.** At the domain's DNS provider, point the name at the cluster:

- a name below a zone's apex (`app.customer.com`): a CNAME to the
  tenant's own name, `acme.<baseDomain>`. It follows the nodes as
  `cluster.dns` keeps the wildcard records;
- a zone's apex (`customer.com`), which may have no CNAME: A and AAAA
  records with the nodes' public addresses (`cluster.publicAddresses`, or
  `publicAddresses` on a node alone, which `murder setup` writes), or
  the provider's ALIAS or ANAME record to `acme.<baseDomain>`. These are
  kept by hand: change them when a node is replaced.

`domain add` and the panel print the records to create.

**What happens.** The leader checks the name's DNS at once: a CNAME that
leads to `acme.<baseDomain>` or to the base domain, or addresses that are
all the nodes' public ones (`cluster.publicAddresses`; on a node alone,
the top-level `publicAddresses`; without them, those `<tenant>.<baseDomain>`
has). A name that does not
point at the cluster yet is checked again after a minute, then less and
less often, an hour apart at most (`check` asks at once). No CA is asked
for a name that does not point here: its rate limits count failures. Once
it does, every edge routes the name to the tenant, exactly as its own
name (moves, holds, 503s for a suspended tenant), and the leader orders
its certificate from `edge.tls.ca` (Let's Encrypt by default) with the
HTTP-01 challenge on port 80 or the TLS-ALPN-01 challenge on port 443:
every node's edge answers both, so the customer's DNS provider needs no
API. The certificate is renewed 30 days
before it expires; the old one is served until the new one is there. A
failed order is tried again after 5 minutes, then less and less often,
6 hours apart at most. Plain HTTP for a domain with its certificate goes
to HTTPS; before it has one, port 80 answers 503.

**Removing a domain.** `domain remove`, and purging the tenant, stop
every edge routing the name at once and delete its certificate and key
from the state, so nothing renews it. The certificate is kept, without
its key, for the leader to revoke with the CA (reason: cessation of
operation), signed with the cluster's account that ordered it: the CA
then no longer vouches that the name is the tenant's. This is best
effort. A failure is tried again after 5 minutes, then less and less
often, five times in all; then the leader gives up and the certificate
simply runs out. A certificate that expired meanwhile is only
forgotten, and one the CA has revoked already counts as revoked. Each
outcome is logged and in the audit log (`domain.revoked`,
`domain.revoke-failed`, with the serial). A certificate from another CA
than `edge.tls.ca` is set to now, or ordered while no CA was set,
cannot be revoked this way.

**One canonical host.** A tenant reachable at several names, such as
`customer.com` and `www.customer.com`, or a domain and its own
`acme.<baseDomain>`, can name one of them canonical with `domain primary`
(or the panel's Domains box, after a review): one of its domains, or its
own name below the base domain. Every edge then answers a request for
any other host of the tenant with a 308 redirect to the canonical one,
over HTTPS, with the same path and query; a 308 keeps the method and
body, so a form posted to the old name is posted again. Plain HTTP goes
straight to `https://<canonical>` in one redirect, so a name that only
redirects needs no certificate of its own for plain HTTP; HTTPS to it
does. Requests for `/.well-known/acme-challenge/` are answered before any
redirect, so names that redirect still get and renew their certificates.
A domain becomes canonical in effect only once the edges can serve it:
once its DNS was checked and, with TLS, it has its certificate; until
then every host serves the tenant as before, and nothing is sent where it
would fail. `tenant show` has the host as `canonical`, `domain list`
marks it. Removing the canonical domain removes the redirect. Browsers
remember a permanent redirect: pick the canonical host before announcing
the others.

The states, in `domain list` and the panel: `verifying` (not checked yet),
`pending` (its DNS does not point at the cluster, said why; with
`edge.tls` not `acme`, no certificate is ordered), `issuing`, `active`,
`renewing` (due, or a renewal failed: the certificate is still served
until it expires) and `failed` (ordering it failed, say the CA could not
reach the name; said why, with the next attempt). Each change is in the audit log (`domain.added`,
`domain.verified`, `domain.pending`, `domain.issued`, `domain.renewed`,
`domain.failed`, `domain.removed`, `domain.revoked`,
`domain.revoke-failed`, and `tenant.canonical` for a change of canonical
host). On the leader, `/metrics` has
`murder_domain_state`, `murder_domain_certificate_expiry_timestamp_seconds`
and `murder_domain_failing` for each domain: alert on an expiry less than
20 days away, or on a domain failing for a day.

What it needs:

- `edge.tls.mode: acme` with `email` (the account's contact): the same
  CA as the wildcard. With `internal` or `off`, domains are checked and
  routed, without certificates (plain HTTP when TLS is off). `internal`
  with `ca` set (`email` optional), as for a test cluster, still orders
  domains' certificates from that CA (say Let's Encrypt's staging one).
- Port 80 or port 443 open to the internet on every node the name leads
  to: the CA asks any of them, from several places. Keep both open in the
  provider's firewall where you can (`test/hetzner` opens 80 and 443). The
  edge listens on `edge.http` (`:80`) and `edge.https` (`:443`) already;
  the leader checks that every node's edge answers a challenge, at its
  private address and `edge.http`'s port, before the CA is asked, so the
  edges keep listening on port 80 even where the internet does not reach
  it.
- Every node on a release with schema 18: the leader orders nothing until
  every node runs one, so a rolling upgrade finishes first. Canonical
  hosts and revocation need schema 19 on every node: the leader orders,
  renews and revokes nothing until every node runs it.

The cluster has one ACME account per CA, made with the first order, its
key sealed with the master key in the state: every leader uses it.
**Port 80 closed or filtered.** The leader offers the CA both challenges
it answers, HTTP-01 and TLS-ALPN-01 (RFC 8737), and the client library
(acmez) picks one; when the CA cannot reach port 80 (a connection or
timeout failure), the order is made again with TLS-ALPN-01, which the CA
checks with a TLS handshake on port 443 that asks for the `acme-tls/1`
protocol. Every edge answers that handshake itself, with a certificate
made for the challenge, from the challenges in its routes, as it answers
HTTP-01: a node that does not run the tenant answers as well as the one
that does, and the handshake is never relayed. Only a client that asks
for `acme-tls/1` alone gets it; browsers never do. What does not work:
a proxy or CDN in front of the nodes that ends TLS itself (TLS-ALPN-01
needs the handshake to reach the edge, and HTTP-01 then needs port 80);
and a failed attempt still counts against the CA's limits on failed
validations, so open port 80 where you can. Ports 80 and 443 both
filtered leave a domain `failed`, said why.

## Back up and restore

`murder backup <directory>` asks the agent for a copy of the database as it
is at one moment (a plain copy of a live database can be torn), checks it and
keeps it as `murder-<UTC time>.db`, removing all but the newest 48 (`-keep`).
`murder-backup.timer` has the agent keep one every hour in
`/var/backups/murder` (`murder backup -scheduled /var/backups/murder`); copy
that directory off the server as well. The database holds secrets only
sealed: keep the master key apart from it, offline.

A copy holds every tenant's sealed secrets, so one made by hand is an access
to every tenant's data: `murder backup -reason TEXT -ticket REF <directory>`
records it in the [access register](#record-access-to-tenants-data) first.
The timer's copies are the system's own keeping, recorded nowhere: the agent
writes them itself into `/var/backups/murder`, readable by root alone as the
database is, and hands them to no one. `-scheduled` writes there only, and
`murder backup /var/backups/murder` without `-scheduled` (the unit of an
earlier release) is taken as the timer's. Opening one of those copies to read
a tenant's data is an access, recorded by hand (`murder access log`).

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
of the cluster, first. Nor does a rotation begin while a drain, a balance, a
replace or a reboot holds the cluster: it says which, and changes nothing
(see [One operation at a time](cluster.md#one-operation-at-a-time)). A rotation stopped part way (a node lost meanwhile,
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
sudo murder backups verify acme -identity key.txt -reason "restore drill" -ticket OPS-12   # read it through as a restore would, compare it with its snapshot
sudo murder tenant archive acme
sudo murder backups restore acme -at 2026-11-01T18:50:55Z -identity key.txt -reason "data deleted by mistake" -ticket ZD-1234
sudo murder tenant restore acme
```

`verify`, `restore`, `state` and `forget` read or delete a tenant's data
(`state`, every tenant's), so each records the access in the
[access register](#record-access-to-tenants-data) through the agent before
it decrypts anything, and refuses when it cannot. The agent's own weekly
verifications (`identityFile`) are the system's, recorded nowhere.

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

## Record access to tenants' data

A tenant's data (its secrets, its logs, its backups, a copy of the state
with every tenant's sealed secrets) is its customer's. People who run the
cluster reach it only when the customer asks, during an incident, or when
the law requires it, and each such access is recorded in the **access
register**: who, when, which tenant, what, why, and at whose request. A
customer can be given their tenant's entries.

The register is part of the audit log: each access is an event of action
`access.<kind>` (`secret`, `logs`, `backup-verify`, `backup-restore`,
`backup-forget`, `state`, `manual`), replicated to every node and kept, as
every event is. `murder events` shows them among the rest.

**Every read through Murder is recorded first, or refused.** Each command
that reads a tenant's data takes `-reason` (why) and `-ticket` (the
customer's ticket the read answers, or `incident` or `legal`, each perhaps
followed by `:REF`, when no customer asked):

```bash
sudo murder tenant secret acme ADMIN_PASSWORD -reason "set up their SSO" -ticket ZD-1234
sudo murder tenant logs acme -reason "export fails since Monday" -ticket ZD-1234
sudo murder tenant logs acme -access 4711          # again, under the same entry, for an hour
sudo murder backups verify acme -identity key.txt -reason "restore drill" -ticket incident:INC-7
sudo murder backup -reason "move to new hardware" -ticket incident /root/copy
```

The agent checks everything else first, records the access (the person sudo
names, as `jan via root`), and only then hands the data out: if the access
cannot be recorded (no quorum, say), nothing is read. While an upgrade has
changes frozen (a minute or so), no new access is recorded either, so none
is read that needs one: rolling the upgrade back puts back the database from
before it, which would lose the record. An entry recorded before the freeze
(`-access ID`, the panel's) still serves its reads: the database from before
holds it. The panel asks for the
purpose and the ticket before it shows a tenant's logs; its entry serves that
person's reads of those logs for an hour, so a page that follows new lines
records one entry. The socket's panel, where no one signs in, also asks who
reads (`jan (panel socket)` in the register), and keeps each browser's
entries apart. A read may name an entry recorded before (`-access ID`,
the panel does so) only for the same thing, by the same person, within an
hour (a day for the restore that recorded it). `backups verify`, `restore`,
`state` and `forget` talk to the bucket themselves: they record the access
through the agent before they decrypt anything.

**Access outside Murder is recorded by hand.** Root on a node can reach a
tenant's data around Murder: `docker exec` into its container, its dataset
under `/srv/murder/tenants/<tenant>` (or the ZFS mountpoint), a backup
decrypted with `age` by hand, a copy of the database read with the master
key. Murder cannot stop that. **The rule is organizational: nobody reaches a
tenant's data any other way than through the commands above unless they
record it with `murder access log`**, before, or as soon as they can:

```bash
sudo murder access log -tenant acme -what "docker exec into its container" -reason "a stuck job" -ticket incident:INC-9
sudo murder access log -tenant acme -what "read its dataset's files" -reason "court order" -ticket legal:12/2026 -at 2026-11-07T08:30:00Z
```

**The customer's extract.** `murder access list` gives the register, oldest
first: with `-tenant`, the tenant's entries and those of copies of the whole
state (every tenant's, shown as tenant `*`); `-since` (a date or RFC 3339)
starts it later; `-format csv` or `json` makes the file to send. A CSV
cell that begins with `=`, `+`, `-` or `@` is written with a `'` before it,
so a spreadsheet does not take it for a formula.

```bash
sudo murder access list -tenant acme -since 2026-01-01 -format csv > acme-access.csv
```

Columns: `id`, `recorded_at` (when the agent recorded it), `accessed_at`
(when the access was: the same, or as `-at` or an offline record says),
`actor`, `tenant`, `kind`, `what`, `purpose`, `ticket`, `offline_record`.

**Without an agent.** In a disaster recovery the agent may be down, or the
cluster without a quorum, or changes frozen by an upgrade that failed, so
nothing can be recorded. `backups verify`, `backups state` and `access log`
take `-offline`: when the agent cannot record the access (it does not
answer, answers that it cannot write, or that changes are frozen; never when
it refuses the access itself), the access is written to
`/var/lib/murder/access-offline.jsonl` on that host instead (readable by root
only, on disk before the read goes ahead) and a warning says so. Once the
agent records again, `sudo murder access sync` records each one (its time as
when the access was, its person as the record says) and empties the file;
the agent records each record once, however many syncs send it, on whichever
node. A line cut short (a full disk, say) is no record: the next one starts
on a line of its own, and a sync leaves the damaged line in the file and
says so, for a person to read and record with `murder access log`. `murder
doctor` warns while records or damaged lines wait in the file. `restore` and `forget` need the agent anyway, and
take no `-offline`. Restoring the state by hand (`murder restore`) is done
with the agent stopped: record the copy you read with `backups state
-offline`, and sync once the agent runs.

What the register does not record:

- the system's own work: scheduled and on-request backups, the agent's
  weekly verifications, replication and moves between nodes, the edge
  serving requests;
- what root does on a node around Murder, unless recorded by hand (the rule
  above);
- reads by Murder releases before the register, during a rolling upgrade:
  a node of an older release still serves a secret or logs without asking
  why, until it runs this release.
