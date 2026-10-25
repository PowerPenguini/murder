# Corral

Corral runs one container per tenant across a few servers and moves a tenant
to another server when its server fails. It is built for small fleets of
single-tenant web apps that keep their state in a local data directory.

See [docs/design.md](docs/design.md) for the agreed architecture and build
stages. Stage 2 (single server) is the current goal.
