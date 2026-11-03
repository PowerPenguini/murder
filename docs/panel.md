# The administration panel

`murder panel` serves a web panel for the cluster: its nodes, tenants, apps,
backups and audit log, and the actions the CLI has. It is a separate process
on every node, like the edge, and asks the local agent over its socket, as
the CLI does: it holds no state of its own, and every change it makes is in
the audit log as `<person> via murder-panel`.

## Using the panel

`murder panel` serves a web panel over the agent's API: the cluster's nodes
(up, down, failed, the leader), every tenant with its state and its copy, a
tenant's runtime, spec, limits and environment, the audit log, and who may
sign in. Its pages follow the cluster every few seconds. On a tenant's page
you suspend or resume it, roll it (a new container), move it to another
node, archive, restore or purge it (these last ones only with its name
typed), and read its logs; the overview creates tenants. It was built in
the stages below. Run it on every node (deploy/murder-panel.service).

It is never on the internet. Every node serves it inside a WireGuard tunnel
that runs in the panel process itself, so nothing changes on the host's
network and tenants cannot reach it; only devices you add reach it:

```yaml
cluster:
  publicAddresses: [203.0.113.10]   # the tunnel's endpoint
panel:
  tunnel:
    address: 10.77.0.1              # this node's, different on every node, below .100
    # network: 10.77.0.0/24         # the same on every node; devices get .100 and up
    # port: 51820                   # UDP, open it in the provider's firewall
  # name: panel.example.com         # panel.<baseDomain> by default
```

The panel needs `edge.tls` (internal or acme): it serves HTTPS with the
wildcard certificate, as `panel.<baseDomain>` (so no tenant may be named
`panel`; rename one from before, or set `panel.name`). The nodes must be a
cluster: the devices are kept in its state. Then, on any node:

```bash
sudo murder admin device add laptop          # a QR code for the WireGuard app
sudo murder admin device add laptop -out laptop.conf
sudo murder admin device list
sudo murder admin device remove laptop       # shut out within seconds, during an upgrade too
```

The device's private key is in the QR code and the configuration, shown
once and kept nowhere (`-out` only ever makes a new file, readable by you).
A node that cannot read the devices as the cluster has them, cut off from
it, lets no device in. Each node is a peer of its own, so the panel stays
reachable through any node that is up. The panel's name must lead to the
nodes' tunnel addresses: with `cluster.dns`, the leader keeps its records
(of the nodes that serve), otherwise add them, or a hosts file entry. Some
home routers drop DNS answers with private addresses; a hosts file entry
works around that.

People sign in with passkeys only: no passwords. Every person has the same
rights, and the audit log names who did what (`jan (panel)`). The first
person is invited from a node:

```bash
sudo murder admin invite jan        # a link, valid 24 hours, once
sudo murder admin user list
sudo murder admin user remove jan
```

Opened on a device in the tunnel, the link adds a passkey (on the device, or
a phone near it) and shows ten recovery codes, once: each adds one passkey,
should every passkey be lost, at the same page (`/enroll`, "an invitation,
or a lost passkey?"). Signed in, the Access page invites others, removes
people and passkeys, makes new recovery codes, and adds or removes devices
(a QR code, shown once). The Cluster page shows each node's release,
drains, removes and fails over nodes, backs up this node's tenants, takes
changes again after a stopped upgrade, and upgrades the cluster with an
uploaded `murder` binary; the slow ones run as jobs, with a page that
follows them. The Apps page creates apps and edits their specs as YAML,
as `murder app apply` reads them (keep the files in git in step), and
follows an app's rollout: pause, resume, promote, abort it, or restore
its tenants with their data from before they moved, losing what they wrote
since (abort and restore with the app's name typed). A tenant's settings page changes its app, image, env,
own secrets and limits, whether it is a canary or held out of rollouts,
and gives a secret a new value. Secrets' values are never shown.
Sessions last 12 hours; signing in and out is in
the audit log. Should everyone lose everything, `murder admin invite` on a
node starts over.

On a computer without the WireGuard app:

```bash
murder admin connect -device laptop.conf     # then open https://panel.example.com:8443
```

It listens on 127.0.0.1:8443 and passes connections through the tunnel; add
`127.0.0.1 panel.example.com` to the hosts file. `-listen` takes another
address, but port 8443 only: passkeys sign in at that port alone. As a last resort, root on a
node reaches the panel on its unix socket (`panel.socket`) through SSH:
`ssh -L 7480:/run/murder/panel.sock root@node`, then http://localhost:7480.

To try the pages without a cluster: `go run ./internal/panel/demo`.

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
- **Look:** black and white, quiet and sharp: lines instead of boxes, a
  state as a dot and a word, labels in light capitals, monospace for
  data (hashes, addresses, logs, YAML, figures), row actions in a
  "⋯" menu and dangerous ones behind a typed name, their button waiting
  for it (`forms.js`; the agent checks again). The overview leads with
  what needs attention, said in words: a node failed or not answering, a
  tenant unhealthy or failed and why, one near its memory limit, one
  without a copy; a tenant's page says what is wrong with it and where to
  fix it. A tenant's pages share a head: where it is, its state, and its
  Overview, Logs and Settings. A form sent once is not sent again while it
  works; wide tables scroll on a phone. Events say their actions in words
  (the code on hover) and can show only what people did; a tenant's logs
  filter, with warnings and errors standing out; the cluster says whether
  its nodes run one release; an error page says what happened and what to
  do. Charts are SVG drawn by
  the server (`charts.go`) in one grey ramp, validated as an ordinal ramp on
  the page, the brightest step kept for what needs attention: live tenants
  by state, each node's tenants, copies and reserved memory, where each
  tenant runs and keeps its copy (a column per node), the events of the
  last day by hour, a tenant's of the last fortnight by day, and each
  app's tenants. A tooltip names every mark; the tables stay beside them.
  What tenants use (see "Use" in [node.md](node.md)) shows as each tenant's memory
  and CPU against its limits and its requests a minute, each node's CPU
  and memory, and on a tenant's page its last day: CPU and memory against
  their limits, requests and errors, the 95th percentile of its response
  time, network and disk, and its data against its disk limit. The mark is a crow's head
  with two copies behind it, in one colour (`static/crow.svg`; the head
  alone for the tab icon, `static/icon.svg`). No JavaScript frameworks and
  nothing loaded from elsewhere (a strict CSP, so no inline styles); pages
  fetch themselves every 5 s (`live.js`) and actions are plain forms.

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
6. **Apps and secrets:** the Apps page lists apps and creates one; an
   app's page edits its spec as its file (`spec.AppFile`: what `murder
   app apply` reads, checked the same way; an app with no scratch mounts
   says `tmpfs: {}`), refusing a save over a change made since the page
   was read (the agent checks it as it stores: `PUT /v1/apps/NAME?if=`
   the app's updatedAt, or `absent` to create only; `murder app apply`
   stores unconditionally, as the files in git are what counts), and deletes an app no
   tenant, live or archived, runs (its name typed). A tenant's settings page changes its app, image, env,
   own secrets and limits (one form, refused if the tenant changed since
   it was read; an env value that would not read back as itself, such as
   one of several lines, is quoted as Go quotes it), and gives a live
   tenant's secret a new value (its name typed). The
   panel never asks for a secret's value: it shows names and kinds only.
   A refused form comes back as typed, saying why. The events page also
   filters by app.
7. **Updates and revisions:** an app given a tag shows the tag in its file
   (a save keeps the image it is pinned to), the image it runs, and a
   newer one the leader found in its registry, with "Update and roll out"
   (`POST /v1/apps/NAME/update?if=`) and "Check now". Its revisions are a
   table; "Go back to this" stores one again (`POST
   /v1/apps/NAME/revisions/N/restore?if=`). Both are refused over a change
   made since the page was read. The Apps page marks apps with a newer
   image.
