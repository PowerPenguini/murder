# The administration panel

`murder panel` serves a web panel for the cluster: its nodes, tenants, apps,
backups and audit log, and the actions the CLI has. It is a separate process
on every node, like the edge, and asks the local agent over its socket, as
the CLI does: it holds no state of its own, and every change it makes is in
the audit log as `<person> via murder-panel`.

## Decisions

- **Who:** a few people, all with the same rights. Each has an account; the
  audit log names the person.
- **Reach:** never from the internet. Every node runs a WireGuard endpoint
  inside the panel process (userspace, no kernel interface: tenants cannot
  reach it, and nothing changes on the host's network). The panel listens
  only inside the tunnel. `murder admin device add NAME` shows a QR code for
  the WireGuard app (phones, laptops); a device can be removed at any time.
  Each node has its own address in the tunnel, so the panel stays reachable
  through any node that is up.
- **Sign-in:** passkeys (WebAuthn), with one-time recovery codes kept
  offline. No passwords.
- **Where state lives:** accounts, passkeys, sessions and devices are in the
  replicated state, so every node's panel knows them.
- **Look:** black and white, sharp edges, monospace, a crow. No JavaScript
  frameworks and nothing loaded from elsewhere (a strict CSP); htmx and
  server-sent events for live pages and actions.

## Stages

Each stage is one PR, reviewed and tested before the next.

1. **Read-only views** (this PR): the process, the design, the overview
   (nodes, tenants, their copies, recent events), a tenant's page and the
   audit log. On a unix socket only root can use, through an SSH tunnel,
   until accounts exist; it answers only as localhost.
2. **Accounts** (after the tunnel, see 3): passkeys, sessions, recovery codes, invitations
   (`murder admin invite NAME` for the first person, then from the panel).
3. **The tunnel:** WireGuard in the panel process (wireguard-go with a
   userspace network stack), devices and their QR codes (`murder admin
   device`), `murder admin connect` for computers without the app, HTTPS
   with the wildcard certificate as `panel.<baseDomain>`, and the leader's
   DNS records for that name (the tunnel addresses of the nodes that
   serve). Done before accounts: passkeys are bound to the name they were
   made for, so the panel has its final name first.
4. **Live pages and tenant actions:** updates as they happen; create,
   suspend, resume, move, roll, archive, restore, purge; logs.
5. **Cluster actions:** upgrades, drain and replace, a manual failover,
   backups and restores.
6. **Apps and secrets:** edit app specs, a tenant's settings and secrets
   (secrets are never shown whole).
