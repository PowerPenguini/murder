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
2. **Accounts** (after the tunnel, see 3): schema 13 keeps people (a random
   WebAuthn user handle each), passkeys (the credential as JSON), one-time
   enrollments by SHA-256 (invitations for 24 hours, ten recovery codes),
   sessions by SHA-256 (12 hours) and a secret every panel shares. The
   panel is the relying party as its name: sign-in is discoverable (no
   name typed); ceremonies in flight are in a cookie signed with the shared
   secret, so any node may finish what another began; a counter that went
   back refuses the passkey; changes are taken only from the panel's own
   origin (SameSite=Strict cookies and the Origin header). `murder admin
   invite NAME` for the first person; the Access page for the rest, and
   for devices. The socket's panel, root's through SSH, needs no sign-in.
3. **The tunnel:** WireGuard in the panel process (wireguard-go with a
   userspace network stack), devices and their QR codes (`murder admin
   device`), `murder admin connect` for computers without the app, HTTPS
   with the wildcard certificate as `panel.<baseDomain>`, and the leader's
   DNS records for that name (the tunnel addresses of the nodes that
   serve). Done before accounts: passkeys are bound to the name they were
   made for, so the panel has its final name first.
4. **Live pages and tenant actions:** pages fetch themselves every 5 s and
   put in their new content (not while someone types, nor on a page that
   shows something once); a tenant's page suspends, resumes, rolls, moves,
   archives, restores and purges it (the last two with its name typed),
   and shows its logs; the overview creates tenants, on the node asked.
   Each action is a form posted to the panel, which acts through the
   agent's API for the person signed in and sends back to the page with
   what it did, once (a cookie, so a reload repeats nothing).
5. **Cluster actions:** the Cluster page shows each node's release and
   schema, drains a node to another, removes one (its name typed), fails
   over one that does not answer (its name typed: only once it is off),
   takes changes again after a stopped upgrade ("thaw" typed), backs up
   this node's tenants, and upgrades the cluster with an uploaded binary
   ("upgrade" typed; the panel checks that it says a murder version, then
   installs it node by node, this one last, as `murder upgrade` does: the
   same code, in `internal/clusterops`). Upgrades, drains and backups are
   jobs: their page follows what they say. One change of the cluster (an
   upgrade or a drain) runs at a time, from either of a node's panels; a
   thaw waits for it too. The binary is uploaded beside the database, not
   to /tmp. Jobs live in the panel process that began them, so a panel
   replaced by an upgrade restarts only once its jobs ended, taking none
   meanwhile; a panel asked to stop serves on until its upgrade or drain
   ended (up to an hour, as its unit says: an upgrade killed halfway
   leaves changes frozen until it is run again, and resumes). Uploads a
   stopped panel left are removed when it starts. Restoring a backup stays on a node (`murder backups
   restore`). Static files carry a hash of their content in their
   address, so a new release never runs an old page's script.
6. **Apps and secrets:** edit app specs, a tenant's settings and secrets
   (secrets are never shown whole).
