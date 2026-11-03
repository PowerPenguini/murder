# Security model

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
