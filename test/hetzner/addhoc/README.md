# AddHoc on the Hetzner test cluster

Tools that drive real AddHoc tenants through the edges, from the client
server (they need python3 only):

- `addhoc_api.py`: an API client pinned to a node, as `curl --resolve`; it
  signs a staff account in and enrolls its second factor, keeping the TOTP
  key in `.mfa-<host>` (0600, ignored by git).
- `seed.py TENANT IP EMAIL DRIVERS VEHICLES FUELINGS`: a company's data,
  with a receipt scan (250 kB) per fueling.
- `load_auth.py SECONDS tenant=ip,ip,ip:email ...`: signed-in load (fuelings
  list, session, drivers, a receipt), one sign-in per tenant. Each request
  opens a new TLS connection, so its latencies include the handshake.
- `verify_receipts.py TENANT IP EMAIL`: downloads every receipt and checks it
  against the SHA-256 the app recorded at upload.
- `sse_check.py TENANT IP EMAIL SECONDS`: holds the notification stream open.

Passwords come from the environment (`PW`, `PW_<tenant>`), read from
`murder tenant secret NAME APP_ADMIN_PASSWORD`. Set them without typing the
password into a command line or the shell's history, for example
`read -rs PW && export PW`.

To run AddHoc on the cluster: build its image (`docker build -f
backend/Dockerfile -t addhoc:test .` in AddHoc), `docker save addhoc:test |
gzip > addhoc.tar.gz`, `nodes.sh image addhoc.tar.gz` (a file: it is read
once for each node, and `docker load` unpacks gzip itself), then `murder app apply` with `examples/apps/addhoc.yaml` whose
image is `addhoc:test`.
