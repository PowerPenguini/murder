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
container restarting. A soak is timed on the leader's own clock, from
the first look at which it sees the tenant healthy on a fresh health check,
and counts only while the tenant's node keeps checking it: a result that
was already old when the leader first saw it (a new leader, say) starts
no soak. Until its wave moves, a tenant keeps the revision it
ran, and a restart, a move or a failover keeps it there too.

Murder never takes a tenant back by itself. Anything that goes wrong
pauses the rollout, naming the tenants, for someone to decide:

- a tenant that does not come up healthy on the new revision within 5
  minutes;
- a tenant that came up and then fails its health check for 30 seconds, or
  restarts;
- a node that does not say how its tenant runs for 5 minutes.

From a paused rollout there are two ways back (and `resume`, to go on):

- `murder app rollout abort web` takes every tenant the rollout moved back
  to the revision before, **its data as it is**. Use it when the old
  revision reads what the new one wrote.
- `murder app rollout restore web -yes` takes them back to the revision
  before **with their data from before they moved**: each tenant's node
  rolls its ZFS dataset back to the newest snapshot holding the old image's
  data, taken when that image's container stopped (each such snapshot
  records the image in its `io.murder:image` property), and the old image
  starts on that. A tenant whose image changed again since its wave goes
  back to the old image's data all the same, not to the newest snapshot. Every write to those tenants
  since they moved is lost; that is the point when a failed upgrade half
  migrated the data. `-tenant NAME` (repeatable) takes only those named
  back. Without `-yes` it only says which tenants it would take back and
  what is lost. The rollout pauses (or stays paused), and a tenant takes no
  request until its data is back: they wait, then get 503, as from any
  tenant that does not answer.

Restore refuses, changing nothing, a tenant the rollout did not move, one
that moved to another node since its wave or is moving now (that node has
no snapshot of its data from before), and one whose image the rollout did
not change (no snapshot was taken as it moved): abort takes those back as
they are. Without ZFS there is no snapshot, and the old image starts on the
data as it is. A node that should take the data back but no longer has the
snapshot keeps the tenant stopped; `murder app rollout abort` then starts it
on its data as it is, even once the rollout is over. Snapshots taken by
releases before the image was recorded are never gone back to: which
image's data they hold is not known. Pruning keeps the newest snapshot a
tenant may go back to, however old.

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
murder app rollout restore web -yes   # every moved tenant back, data from before it moved: writes since are lost
murder app rollout restore web -tenant acme -yes   # only acme
murder app apply apps/web.yaml -now   # no waves: every tenant now, ending a rollout that runs
```

`-now` (on `apply`, `update` and `rollback`) is for a fix that cannot wait,
or for an app with one tenant; applying the same spec again with `-now`
moves what its rollout has yet to move. While a tenant's data waits to go
back, `-now` is refused: let the restore finish, or abort the rollout. `-wait` waits for the rollout of
the app's newest revision, changed by this apply or an earlier one. A newer change while a rollout runs starts
it again from the first wave, with the tenants already moved kept where
they are until their wave. The audit log names each tenant a wave moves
(`app.rolled-out`), each pause (`rollout.paused`), each tenant taken back
with its data (`rollout.restored`) or as it is (`rollout.rolled-back`), and
the end (`rollout.done`, `rollout.aborted`). Revisions a tenant
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
- Paths an image declares with `VOLUME` are kept in the tenant's data. One
  at or below `dataPath` is bound from the same directory of the data
  (made for the app's user if missing; the image's own files there are not
  copied in), so it is snapshotted, copied to the replica and backed up
  with the rest. A volume at a `tmpfs` path is that tmpfs. An image with a
  volume anywhere else is refused when its container is made, before the
  old container stops, so the tenant keeps running the image it had: set
  `dataPath` to that path, or to a directory it is in. A container that
  already runs such an image keeps running as it is until it is replaced.
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
murder tenant logs acme -previous      # the last lines of the container before (it exited, or was replaced)
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

Each time a tenant's app exits and its node starts it again, the audit log
says so once, with the exit code and, for a kill, the memory limit it ran
under ("exit 137: killed (SIGKILL), most likely for using more than its
256 MB of memory"); the panel judges whether an app's limit is too low from
the kills at the limit in effect only.

A container's lines go with it, so a node keeps the last 1000 lines (at
most 64 KB) of each tenant's previous container in `previous-logs/` beside
its database, readable by root only, until the next one goes: when the app
exits and the agent starts it again (its lines up to the exit, with the
exit code), and when the container is replaced (a roll, a new image, spec,
limits or secret) or removed as the tenant is suspended. `murder tenant
logs NAME -previous` and the panel's Logs show them. None are kept when
the node itself stopped (a power loss, a kernel panic, a failover), when a
tenant moved to another node (the old node drops them as the tenant
leaves), for an exit while the agent was down, or for a container an older
release left to the engine's own restart policy.

An archive is one change: the edge stops routing to the tenant at once, and
the reconcile pass it starts removes the container and network (a pass that
cannot, retries). A new tenant cannot take an archived tenant's name until
that tenant is restored or purged, and it keeps its network range (and so
its address) until it is purged: no other tenant can be routed to a
container of it that is still running.
