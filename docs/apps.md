# Apps and tenants

A tenant runs an **app**: a spec shared by every tenant of it, kept in git and
applied from CI or by hand. The tenant adds only what is its own: a name and a
few overrides. Changing an app rolls the change out to its tenants, in
waves (see [Rollouts](#rollouts)).

An app spec needs only an image and a user; everything else has a default.
The app is named after its file:

```yaml
# apps/web.yaml
image: ghcr.io/example/web:1.4  # a tag; the agent pins it to a digest when applying
user: "10001:10001"             # numeric uid:gid; never root; also owns the data
healthPath: /api/health         # default "/"
env:
  PUBLIC_URL: "{{url}}"
  TRUSTED_PROXIES: "{{network}}"
secrets: [SESSION_SECRET]       # 32 random bytes, hex; or a mapping such as ADMIN_PASSWORD: password
```

```bash
murder app apply apps/web.yaml -wait 10m         # store the app; roll it out to its tenants, in waves
murder tenant create acme -app web -wait 3m       # on the node running the fewest tenants
murder tenant create delta -app web -node nbg-1   # on that node
murder tenant create beta -app web -env TZ=Europe/Berlin -memory 768
murder tenant set gamma -image ghcr.io/example/web:1.5   # an image of its own
murder tenant set gamma -app-image                       # back to the app's image
```

`app apply` reads and checks every file before it changes anything and
reports each app as created, updated or unchanged, with the tenants the
change rolls out to. A change that would make a live tenant invalid is
refused as a whole, before the image is pulled. Archived tenants run the
app's current spec when restored; one that no longer fits is refused then
and can be fixed with `tenant set` while archived. `-wait` is
one budget for all the apps' rollouts and tenants, watched side by side,
so a retry after a failed rollout fails until they are healthy.

## Rollouts

A change of an app, applied, updated or rolled back, moves its tenants in
waves, driven by the cluster's leader:

1. the canaries (`murder tenant set NAME -canary true`), or the first tenant
   by name when there are none; they must stay healthy for 2 minutes;
2. a tenth of the tenants, then half, then the rest; each wave must stay
   healthy for a minute before the next moves.

A tenant is through when it runs the new revision, healthy, without its
container restarting. Until its wave moves, a tenant keeps the revision it
ran, and a restart, a move or a failover keeps it there too.

- A tenant that does not come up healthy within 5 minutes goes back to the
  revision before, **its data too**: its node rolls the ZFS dataset back to
  the snapshot taken when the old container stopped. Whatever it wrote
  since is lost; that is the point, as a failed upgrade may have half
  migrated it. The rollout pauses. Until it does, a moved tenant takes no request (they wait, then get
  503, as from any tenant that does not answer), so its data goes back
  only if it took none: if it never
  passed a health check on the new revision (its node records that it did
  before it serves a request, so a new leader knows too), the leader that
  moved it looked at it every few seconds since with nothing changing the
  rollout meanwhile (no pause or resume, however brief), and only while it
  runs on the node it moved on. Otherwise (a new leader, a pause, a move) the
  rollout pauses with the tenant's revision and data as they are. Without ZFS, the data stays as it
  is. A node that should take the data back but has no snapshot of then
  keeps the tenant stopped; `murder app rollout abort` starts it on its
  data as it is.
- A tenant that came up and then fails its health check for 30 seconds, or
  restarts, pauses the rollout and stays as it is, for someone to look.
- A node that does not say how its tenant runs pauses the rollout after 5
  minutes; nothing goes back.
- A held tenant (`-hold true`) stays on the revision it runs, out of every
  rollout, until `-hold false`; then it takes the newest revision.
- A suspended tenant moves with its wave and holds no wave up: nothing of
  it runs to watch.

```bash
murder app rollout web                # its waves: tenants on the new revision, waiting, held
murder app rollout pause web          # stop after the wave that runs
murder app rollout resume web
murder app rollout promote web        # every waiting tenant now
murder app rollout abort web          # every moved tenant back to the revision before; data stays
murder app apply apps/web.yaml -now   # no waves: every tenant now, ending a rollout that runs
```

`-now` (on `apply`, `update` and `rollback`) is for a fix that cannot wait,
or for an app with one tenant; applying the same spec again with `-now`
moves what its rollout has yet to move. `-wait` waits for the rollout of
the app's newest revision, changed by this apply or an earlier one. A newer change while a rollout runs starts
it again from the first wave, with the tenants already moved kept where
they are until their wave. The audit log names each wave (`app.wave`),
each tenant taken back and the end (`rollout.done`). Revisions a tenant
still runs are kept beyond the last 20.

**Overrides** are the tenant's own `-image`, `-env`, `-secret` and limits
(`-memory`, `-cpus`, `-pids`, `-disk`). `tenant set` changes only the flags it is
given, and refuses to overwrite a change someone made since it read the
tenant; `-unset-env`, `-unset-secret` and `-app-image` drop an override, and a
limit of 0 goes back to the app's.

**Secrets** get a value once per tenant, stored sealed. A value is kept even
if the secret leaves the spec and comes back, so a mistaken edit cannot lose
it; `murder tenant rotate` gives it a new value.

**Defaults:**
- `port` 8080, `dataPath` /data, `healthPath` /;
- a read-only root with a 256 MB `/tmp` tmpfs (`writableRoot: true` and `tmpfs` change that);
- the agent's default limits.

**Env placeholders:** values may use `{{name}}`, `{{host}}`, `{{url}}` and
`{{network}}`. `{{network}}` is the tenant network's CIDR; the edge proxy
connects from it, so trust it for proxy headers.

**Images:**
- Tags are pinned to the digest the registry names for them when stored, so
  every node runs the same bytes. An app keeps the image its tag was pinned
  to: applying the same tag again, say with another env, takes no newer
  image by the way, and says when the registry holds one. `murder app update
  web` (or `app apply -update`, for CI that pushes a moving tag) takes the
  tag's newest image and rolls it out; it changes nothing if the registry
  does not answer. Another tag in the file is pinned anew.
- The cluster's leader asks the registry every `imageChecks` (5m; 0 never)
  which image each app's tag names now, without pulling it; a tag on Docker
  Hub at most hourly, as Hub counts each question as a pull. A newer image
  is only recorded: `app list` and the panel show it, the audit log says
  `app.image-found`, and `murder_app_newer_image` is 1 until someone takes
  it. Nothing rolls out by itself. `murder app check` asks at once.
  `murder_app_image_check_failing` is 1 for an app whose registry did not
  answer at the leader's last check. Tenants with an image of their own
  (`tenant set -image`) keep it; their tags are not followed.
- Every change of an app is a revision; the last 20 are kept. `murder app
  history web` lists them and `murder app rollback web` stores the one
  before the current again (`-to N` another), with its image and tag. An
  app stored by an older release gets its first revision at its next change.
- A tag that only exists locally is pinned to the local image.
- Images are kept in a registry outside the cluster, so a node that takes a
  tenant over can still pull its image when another node is gone. GitHub's
  container registry (ghcr.io) is the one set up here; any other works the
  same way. Images are named in full (`ghcr.io/example/web:stable`).
- Private registries need a login on every node, under `registries` in
  the agent config: the registry's host, a user and a file holding the
  token, read at every pull, so a token replaced there is used at once.
  For ghcr.io, a classic token with only `read:packages`; for another
  registry, another entry:

  ```yaml
  registries:
    - {host: ghcr.io, username: powerpenguini, tokenFile: /etc/murder/ghcr.token}
    - {host: registry.example.com, username: murder, tokenFile: /etc/murder/own.token}
  ```

  A registry without a login there may have one in `registryAuthFile`, a
  Docker `config.json` with an `auths` entry per registry. Credential
  helpers are not supported. The Docker engine never reads what `docker
  login` saved; the client has to send it.

Day-to-day operations:

```bash
murder app list                         # apps, their tenants, images and newer images found
murder tenant list                      # app, state, health, image, URL; archived tenants too
murder tenant show acme
murder tenant secret acme ADMIN_PASSWORD
murder tenant rotate acme ADMIN_PASSWORD -wait 3m   # new value; the container restarts
murder tenant logs acme -tail 100
murder tenant suspend acme              # stop it, keep its address (visitors get 503)
murder tenant resume acme -wait 3m
murder tenant archive acme              # take it out of service; data, secrets and overrides are kept
murder tenant restore acme -wait 3m     # back with its data, secrets and overrides, on the app's current spec
murder tenant purge acme -yes           # delete an archived tenant and its data for good
murder app check                        # ask the registries now for newer images of the apps' tags
murder app update web -wait 3m          # take the newest image of web's tag and roll it out
murder app history web                  # web's recent revisions
murder app rollback web -wait 3m        # back to the revision before the current one, in waves
murder app rollout web                  # how web's rollout goes
murder app delete web                   # once no tenant (live or archived) runs it
murder events -tenant acme              # the audit log: who changed what, and when
```

An archive is one change: the edge stops routing to the tenant at once, and
the reconcile pass it starts removes the container and network (a pass that
cannot, retries). A new tenant cannot take an archived tenant's name until
that tenant is restored or purged, and it keeps its network range (and so
its address) until it is purged: no other tenant can be routed to a
container of it that is still running.
