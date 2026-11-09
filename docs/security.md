# Security model

- **The CLI is root-equivalent.** Anyone who can use the API socket (root, or
  members of `socketGroup`) controls every tenant on the node, including its
  secrets and data.
- **Staff access to tenants' data is recorded.** Reading a tenant's
  secrets, logs or backups, or a copy of the state, through Murder needs a
  purpose and a ticket, and the agent records the access in the replicated
  audit log before it hands any data out; it refuses the read if it cannot
  record it ([the access register](operations.md#record-access-to-tenants-data)).
  One exception is deliberate: in a disaster recovery, `backups verify`
  and `backups state` with `-offline` read a backup once the access is on
  that host's disk, in a file `murder access sync` records later, when the
  agent cannot record it. The route table the edge reads carries tenants'
  domains' private keys, sealed: only root (the edge) reads it.
  This binds people who use Murder's commands and panel, not root: root on
  a node reaches tenants' data around Murder (`docker exec`, the datasets,
  backups decrypted by hand). Recording such access with `murder access log`
  is an organizational rule, not a technical control. The person named is
  the one sudo names (`SUDO_USER`), the panel's signed-in person, or the
  name given on the socket's panel; give each operator an account of their
  own. The register is as trustworthy as
  those who can write the state: root on any node holds the master key and
  could, like any member, write events or remove them on every node; the
  copies of the state in the backup bucket (locked, in production) keep
  what was there.
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
- **The host lets in only what it serves, if asked.** With `hostFirewall`
  set (see [Harden the node](operations.md#harden-the-node)), the same table drops traffic to the host
  other than SSH, the edge, the panel's tunnel and the cluster's ports
  from private networks. The netns test checks it from outside as well.
- **The edge runs confined.** It faces the internet, so
  `murder-edge.service` keeps it to binding low ports
  (`CapabilityBoundingSet=CAP_NET_BIND_SERVICE`; systemd binds its public
  ports for it through `murder-edge.socket`, and it binds only what the
  socket does not hold, such as `edge.metrics`), a read-only system
  (`ProtectSystem=strict`; it writes below `/var/lib/murder` and
  `/run/murder-edge` only), no new privileges, the system calls of an
  ordinary service and its own `/tmp` and devices
  (`systemd-analyze security` rates it 2.0, from 9.4). The agent and the
  panel replace the binary during an upgrade, and the agent runs nft and
  drives Docker, so they are not confined the same way.
- **Tenant data is out of reach of host users.** Data directories sit below a
  root-only (`0700`) directory, and ZFS datasets are created with `setuid=off`
  and `devices=off`. A setuid file a tenant writes there is useless to host
  users.
- **Tenants' own domains.** Only those who use the API (root, the
  socket's group, people of the panel) add a domain to a tenant. A domain
  is routed only once its DNS was seen to point at the cluster, and the
  leader asks a CA only for such a name: adding someone else's name gives
  nothing until they point it here. The replicated state holds each
  domain's certificate chain, in the clear, and its private key sealed
  with the master key (AES-256-GCM, bound to the domain's name), and the
  cluster's ACME account key, sealed alike. They are in every copy of the
  state and every backup of it, as tenants' secrets are, and a master key
  rotation seals them again in the same change. Each edge opens the keys
  with the master key file, which it now reads outside a cluster too, and
  keeps them in memory only; the routes it saves on disk (`edge.routes`)
  hold them sealed. The HTTP-01 key authorizations every edge answers are
  public by design and expire within 15 minutes. Removing a domain, or
  purging its tenant, deletes its certificate and key from the state;
  certificates are not revoked.
- **Docker's `userns-remap` is not supported.** The data owner is the
  container user's numeric id on the host, which remapping would change.
- **Nodes prove who they are with keys of their own.** Nodes talk over
  mutual TLS on the private network, with certificates of an authority
  derived from the master key. The master key is on every node and in
  every invitation, so it only proves that a certificate is the
  cluster's. Each node also has an identity key (`node.key` in its
  cluster directory, readable by root only, never sent anywhere). Its
  certificate is issued over that key, and the replicated state pins the
  key's public half to the node's name. A peer is a member only if the
  state names it and its certificate carries the key pinned for that
  name. Every request between nodes, every Raft connection, and every
  request a node sends on a connection it keeps is checked this way.
  - **What this keeps out:** anyone with the master key but without a
    member's `node.key`. That covers a node removed from the cluster, a
    copy of `/etc/murder`, and a cluster restored from a backup. Such a
    party is refused the state, writes, Raft, binary installs, moves and
    copies, whatever name its certificate gives. Each refusal is a
    warning in the refusing agent's log, naming the peer's address and
    the node its certificate names, and why: the first at once, then at
    most one a minute for each reason and node, with how many were left
    out (`suppressed`), as a refused node tries again every second or so.
    A node joins only with
    an invitation, which admits its name and identity key for an hour.
    The invitation names the inviting node and its identity key. The new
    host sends the secret, and takes the master key and the members' keys,
    only from a node that proves it holds that key. So someone with the
    master key who intercepts the host's connection, or takes over the
    inviter's address, gets nothing and hands out nothing. Removing a node
    deletes its row, and with it its key. Only root on a member can give a
    node another key (`murder cluster rekey`, for a node that lost its
    `node.key`); the leader applies it, never while changes are frozen.
  - **Members are trusted with the state.** A member is a node you run.
    It holds the master key and every secret, and as a voter it can win
    an election and append what it likes to the log. Nothing here
    protects the state from a member; the keys exist to keep
    non-members out. Some checks guard against our own bugs, and keep a
    batch from doing different things on different nodes:
    - The state refuses any statement that would change a pinned key.
    - A batch's statements run under SQLite's authorizer. It allows reads
      and row changes of the replicated tables, and of the meta table's
      keys, but not its values, which differ between nodes. It allows no
      function call, no transaction control, no schema change, no pragma,
      no attached database and no read of SQLite's own tables.
    - A batch may not write the meta rows the store keeps for itself (the
      node's name, the version, the applied index). The store's own count
      of those rows after a batch runs under the same rules, triggers
      included.
    - These checks cover what we found a batch could do differently on
      different nodes; they are not a proof that nothing else can.
    - Only the leader adds and removes nodes, admits hosts and registers
      keys. It registers a key only for the node whose certificate
      carries it.
  - **What the master key still gives:**
    - It opens every tenant's sealed secrets in any copy of the state.
    - It makes certificates the edges' relay port (`edge.relayPort`, on
      the private network) accepts. Edges have no identity keys, so the
      relay checks only that a certificate is the cluster's. A master key
      holder, a removed node included, can send requests to any tenant
      through a node's relay. The relay believes the client address and
      scheme the sender gives, so the `X-Forwarded-For` a tenant sees may
      be made up. That is all it gives: the relay serves tenants' own
      HTTP, as the public ports do, and nothing of the cluster. A tenant
      should not rely on the client address for access control on a
      cluster whose master key may have leaked: rotate the key.
  - **The upgrade window.** In a cluster from before identity keys, a
    node the state holds no key for is taken by its name, as before: the
    master key passes for it. That includes such a node on its way out,
    taken out of the state but not yet out of the voters. The same goes
    for every node while a leader of the older release runs, since it
    checks no keys. Each node
    registers its key once every node runs a release that has keys and
    changes are not frozen, so the window closes within seconds of the
    end of a rolling upgrade. Until every node shows a key in
    `murder cluster status`, finish the upgrade promptly, and keep the
    master key's copies as close as before.
