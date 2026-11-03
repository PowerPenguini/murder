# Developing Murder

```bash
go test ./...                                   # unit tests
go test -tags docker -count=1 ./integration     # full tenant lifecycle on the local Docker engine
test/vm/go-test.sh ./internal/volume            # ZFS driver tests inside a throwaway VM
make -C test/hetzner up deploy cluster smoke TOKEN_FILE=path   # a real cluster on Hetzner Cloud
make -C test/hetzner down                       # delete it (servers bill by the hour)
```

**The Hetzner cluster:** three nodes in three locations (fsn1, nbg1,
hel1) on one private network, each with a volume for its ZFS pool, and the
test app as tenant `t-<node>` on each. `smoke` reads every tenant through
every edge; `nodes.sh load SECONDS` keeps reading while you run `nodes.sh
upgrade`, `make spare` (a fourth, bigger node) and `murder cluster
replace`, or switch a node off. The token file holds a Hetzner Cloud
project token with read and write access.

**What the Docker test does:** it builds a tiny scratch image and runs a tenant
through Murder's real components:
- create, then route through the edge;
- write data, then change the image and keep the data;
- restart the agent and adopt the container;
- suspend and resume;
- archive, restore with the data, purge.
