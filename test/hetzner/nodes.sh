#!/usr/bin/env bash
# Drives the test cluster the OpenTofu config created; the Makefile calls it.
#
#   nodes.sh wait               wait for cloud-init on every node
#   nodes.sh deploy [NODE...]   build Murder and the test app, install both
#   nodes.sh cluster            init n1 and bootstrap; init and join the rest
#   nodes.sh join NODE          init and join one node (the spare)
#   nodes.sh smoke              one tenant per node, read through every edge
#   nodes.sh ssh NODE [CMD]     a shell, or a command, on a node
#   nodes.sh image TAR          load a docker image (docker save) on every node
#   nodes.sh upgrade            build Murder and upgrade the cluster with it
#   nodes.sh load SECONDS       read every tenant through every edge, count failures
#   nodes.sh status             the cluster and its tenants as n1 sees them
#   nodes.sh counter TENANT S   write a counter to a tenant for S seconds: outages, writes lost
#   nodes.sh poweroff NODE      power a node off through Hetzner's API, as if it died
#   nodes.sh poweron NODE       power it on again
#   nodes.sh partition NODE     cut a node off the private network; it keeps running
#   nodes.sh dns                the wildcard records as Hetzner's name servers answer
#
# With .state/dns-zone naming a zone of the project's Hetzner Cloud DNS (any
# name: Hetzner's name servers answer for it undelegated), the base domain is
# that zone and the leader keeps its wildcard records; `dns` reads them.
set -euo pipefail

HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO=$(cd "$HERE/../.." && pwd)
STATE=$HERE/.state
KEY=$STATE/id_ed25519
DOMAIN=murder.test
if [[ -s $STATE/dns-zone ]]; then DOMAIN=$(cat "$STATE/dns-zone"); fi

die() { echo "nodes.sh: $*" >&2; exit 1; }
log() { echo "==> $*" >&2; }

nodes_json() { tofu -chdir="$HERE" output -json nodes; }
names() { nodes_json | jq -r 'keys[]'; }
public_ip() { nodes_json | jq -r --arg n "$1" '.[$n].public // empty'; }
private_ip() { nodes_json | jq -r --arg n "$1" '.[$n].private // empty'; }

SSH_OPTS=(-i "$KEY" -o StrictHostKeyChecking=accept-new -o UserKnownHostsFile="$STATE/known_hosts"
  -o ConnectTimeout=10 -o ServerAliveInterval=15 -o LogLevel=ERROR)

on() {
  local node=$1; shift
  local ip; ip=$(public_ip "$node"); [[ -n $ip ]] || die "no node $node"
  ssh "${SSH_OPTS[@]}" "root@$ip" "$@"
}

put() {
  local node=$1 dest=$2; shift 2
  scp -q "${SSH_OPTS[@]}" "$@" "root@$(public_ip "$node"):$dest"
}

cmd_wait() {
  for node in $(names); do
    log "waiting for $node"
    for _ in $(seq 120); do
      on "$node" test -e /var/lib/murder-test/ready 2>/dev/null && break
      sleep 5
    done
    on "$node" test -e /var/lib/murder-test/ready || die "$node is not ready; see /var/log/cloud-init-output.log there"
    on "$node" 'zpool list -H -o name,size tank'
  done
  local client; client=$(tofu -chdir="$HERE" output -raw client 2>/dev/null || true)
  if [[ -n $client ]]; then
    log "waiting for the client"
    for _ in $(seq 60); do
      ssh "${SSH_OPTS[@]}" "root@$client" test -e /var/lib/murder-test/ready 2>/dev/null && return
      sleep 5
    done
    die "the client is not ready"
  fi
}

config_for() {
  local node=$1
  cat <<EOF
node: $node
baseDomain: $DOMAIN
database: /var/lib/murder/murder.db
masterKeyFile: /etc/murder/master.key
socket: /run/murder/agent.sock
socketGroup: murder
dockerSocket: /var/run/docker.sock
networkPool: 10.240.0.0/16
firewall: nftables
metrics: 127.0.0.1:9464
volumes:
  driver: zfs
  parent: tank/murder/tenants
edge:
  http: ":80"
  https: ":443"
  metrics: 127.0.0.1:9465
  tls:
    mode: internal
    storage: /var/lib/murder/certs
cluster:
  address: $(private_ip "$node"):7400
  dir: /var/lib/murder/cluster
EOF
  # Automatic failover with the project's token, unless FAILOVER=0.
  if [[ ${FAILOVER:-1} == 1 ]]; then
    printf '  failover:\n    hetzner:\n      tokenFile: /etc/murder/hcloud.token\n      servers:\n'
    for name in $(names); do printf '        %s: murder-test-%s\n' "$name" "$name"; done
  fi
  # The public address, and the DNS records with a zone set up.
  printf '  publicAddresses: [%s]\n' "$(public_ip "$node")"
  if [[ -s $STATE/dns-zone ]]; then
    printf '  dns:\n    provider: hetzner\n    tokenFile: /etc/murder/hcloud.token\n'
  fi
  cat <<EOF
defaults: {memoryMB: 128, cpus: 1, pids: 256, diskGB: 2}
reconcileInterval: 10s
EOF
  # Backups, when .state/backup holds S3 keys (see test/hetzner/README.md).
  if [[ -s $STATE/backup/s3.access ]]; then
    local bucket=$STATE/backup/bucket-${BACKUP_MODE:-test}.txt
    if [[ ! -s $bucket || ! -s $STATE/backup/recipient.txt ]]; then
      echo "backups: $bucket and $STATE/backup/recipient.txt are needed beside the S3 keys" >&2
      exit 1
    fi
    cat <<EOF
backups:
  mode: ${BACKUP_MODE:-test}
  endpoint: https://hel1.your-objectstorage.com
  region: hel1
  bucket: $(cat "$bucket")
  accessKeyFile: /etc/murder/s3.access
  secretKeyFile: /etc/murder/s3.secret
  recipient: $(cat "$STATE/backup/recipient.txt")
  every: ${BACKUP_EVERY:-1h}
EOF
  fi
}

build() {
  mkdir -p "$STATE/build"
  log "building murder and the test app"
  # A build ID of its own makes every build a new binary to upgrade to,
  # even of the same code.
  (cd "$REPO" && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags "-buildid=murder-test-$(date +%s)" -o "$STATE/build/murder" ./cmd/murder)
  (cd "$REPO" && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -o "$STATE/build/testapp" ./integration/testapp)
  printf 'FROM scratch\nCOPY testapp /testapp\nENTRYPOINT ["/testapp"]\n' > "$STATE/build/Dockerfile"
}

install_on() {
  local node=$1
  log "installing on $node"
  config_for "$node" > "$STATE/build/agent-$node.yaml"
  on "$node" 'mkdir -p /tmp/murder-test /etc/murder'
  put "$node" /tmp/murder-test/ "$STATE/build/murder" "$STATE/build/testapp" "$STATE/build/Dockerfile" \
    "$REPO"/deploy/murder-*.service "$REPO"/deploy/murder-backup.timer "$REPO"/deploy/murder-sysctl.conf
  put "$node" /etc/murder/agent.yaml "$STATE/build/agent-$node.yaml"
  if [[ -s $STATE/backup/s3.access ]]; then
    put "$node" /etc/murder/ "$STATE/backup/s3.access" "$STATE/backup/s3.secret"
    on "$node" 'chmod 600 /etc/murder/s3.access /etc/murder/s3.secret'
  fi
  # The token goes over stdin, never on a command line.
  if [[ ${FAILOVER:-1} == 1 ]]; then
    [[ -n ${HCLOUD_TOKEN:-} ]] || die "failover needs HCLOUD_TOKEN (or FAILOVER=0)"
    printf '%s' "$HCLOUD_TOKEN" | on "$node" 'umask 077; cat > /etc/murder/hcloud.token'
  fi
  on "$node" 'set -e; cd /tmp/murder-test
    install -m 0644 murder-*.service murder-backup.timer /etc/systemd/system/
    install -m 0644 murder-sysctl.conf /etc/sysctl.d/90-murder.conf && sysctl -q -p /etc/sysctl.d/90-murder.conf
    systemctl daemon-reload
    if [ -x /usr/local/bin/murder ] && systemctl is-active -q murder-agent; then
      echo "murder runs already: upgrade it with murder upgrade"
    else
      install -m 0755 murder /usr/local/bin/murder
    fi'
}

# A locally built image is pinned by its ID, which differs between two
# builds: the test app is built once, on the first node, and every node
# loads that same image.
load_image() {
  local image=$STATE/build/testapp-image.tar
  if [[ ! -s $image ]]; then
    on "$1" 'cd /tmp/murder-test && docker build -q -t murder-testapp:test . >/dev/null && docker save murder-testapp:test' > "$image.part"
    mv "$image.part" "$image"
  fi
  on "$1" 'docker load -q >/dev/null' < "$image"
}

cmd_deploy() {
  build
  local targets=("$@"); ((${#targets[@]})) || mapfile -t targets < <(names)
  for node in "${targets[@]}"; do install_on "$node"; load_image "$node"; done
}

start() {
  on "$1" 'systemctl enable --now murder-firewall murder-agent murder-edge murder-backup.timer >/dev/null 2>&1
    for i in $(seq 60); do [ -S /run/murder/agent.sock ] && exit 0; sleep 1; done
    journalctl -u murder-agent -n 30 --no-pager; exit 1'
}

# The master key leaves n1 only for the other nodes, through a 0600 file
# in .state; it is never printed.
init_with_key() {
  local node=$1
  if [[ $node != n1 ]]; then
    [[ -s $STATE/master.key ]] || die "no master key from n1 yet; run the cluster step first"
    put "$node" /etc/murder/master.key "$STATE/master.key"
    on "$node" 'chmod 0600 /etc/murder/master.key'
  fi
  on "$node" 'murder init -config /etc/murder/agent.yaml >/dev/null'
  if [[ $node == n1 ]]; then
    (umask 077; scp -q "${SSH_OPTS[@]}" "root@$(public_ip n1):/etc/murder/master.key" "$STATE/master.key")
  fi
  start "$node"
}

cmd_join() {
  local node=$1
  log "joining $node"
  init_with_key "$node"
  on "$node" "murder cluster join $(private_ip n1):7400"
}

cmd_cluster() {
  log "bootstrapping n1"
  init_with_key n1
  on n1 'murder cluster bootstrap'
  for node in $(names); do
    [[ $node == n1 ]] && continue
    cmd_join "$node"
  done
  on n1 'murder cluster status'
}

# curl a tenant through one node's edge, over HTTPS with the node's own
# internal certificate.
via() {
  local node=$1 tenant=$2; shift 2
  local path=$1; shift
  curl -sk -m 20 --resolve "$tenant.$DOMAIN:443:$(public_ip "$node")" "$@" "https://$tenant.$DOMAIN$path"
}

cmd_smoke() {
  local all; mapfile -t all < <(names)
  log "applying the test app"
  on n1 'cat > /tmp/murder-test/testapp.yaml <<EOF
image: murder-testapp:test
user: "1000:1000"
healthPath: /health
env:
  DATA_DIR: /data
EOF
murder app apply /tmp/murder-test/testapp.yaml -wait 3m'
  for node in "${all[@]}"; do
    local tenant=t-$node
    local listed; listed=$(on n1 "murder tenant list")
    if ! grep -qw "$tenant" <<< "$listed"; then
      log "creating $tenant on $node"
      on "$node" "murder tenant create $tenant -app testapp -wait 3m"
    fi
    via "$node" "$tenant" /value -X PUT --data "value of $tenant" -o /dev/null -w '' || die "cannot write $tenant"
  done
  local failed=0
  for node in "${all[@]}"; do
    for target in "${all[@]}"; do
      local tenant=t-$target got
      got=$(via "$node" "$tenant" /value -w ' %{http_code} %{time_total}s') || got="curl failed"
      printf '%-4s -> %-6s %s\n' "$node" "$tenant" "$got"
      [[ $got == "value of $tenant 200 "* ]] || failed=1
    done
  done
  ((failed == 0)) || die "some reads failed"
  log "every tenant answered through every edge"
}

# Loads an image from a "docker save" archive on every node, so tenants
# can run an image that is in no registry, such as a branch's build.
cmd_image() {
  local archive=${1:?archive}
  for node in $(names); do
    log "loading $(basename "$archive") on $node"
    on "$node" 'docker load -q' < "$archive"
  done
}

cmd_upgrade() {
  build
  # From a node of the cluster: the first may have been replaced.
  local node=${UPGRADE_FROM:-n2}
  on "$node" "mkdir -p /tmp/murder-test"
  put "$node" /tmp/murder-test/murder-new "$STATE/build/murder"
  on "$node" 'murder upgrade /tmp/murder-test/murder-new'
}

# Reads each tenant through each node's edge (load.sh) for SECONDS, from
# the client server when there is one. EDGES and TENANTS pick the nodes and
# tenants (all by default).
cmd_load() {
  local seconds=${1:-60} edges tenants pairs="" client
  read -ra edges <<< "${EDGES:-$(names | tr '\n' ' ')}"
  if [[ -n ${TENANTS:-} ]]; then read -ra tenants <<< "$TENANTS"
  else mapfile -t tenants < <(on n2 'murder tenant list -json' | jq -r '.[].spec.name'); fi
  for node in "${edges[@]}"; do pairs+="$node=$(public_ip "$node") "; done
  client=$(tofu -chdir="$HERE" output -raw client 2>/dev/null || true)
  if [[ -n $client ]]; then
    scp -q "${SSH_OPTS[@]}" "$HERE/load.sh" "root@$client:/root/load.sh"
    ssh "${SSH_OPTS[@]}" "root@$client" /root/load.sh "$seconds" "$DOMAIN" "'$pairs'" "'${tenants[*]}'"
  else
    "$HERE/load.sh" "$seconds" "$DOMAIN" "$pairs" "${tenants[*]}"
  fi
}

# hcloud calls Hetzner's API with the token from stdin, not the command line.
hcloud() {
  local method=$1 path=$2
  printf 'Authorization: Bearer %s\n' "$HCLOUD_TOKEN" | curl -sS -f -X "$method" -H @- "https://api.hetzner.cloud/v1$path"
}

server_id() {
  hcloud GET "/servers?name=murder-test-$1" | jq -r '.servers[0].id // empty'
}

cmd_power() {
  local action=$1 node=$2 id
  id=$(server_id "$node"); [[ -n $id ]] || die "no server murder-test-$node"
  log "$action $node (server $id) through Hetzner's API"
  hcloud POST "/servers/$id/actions/$action" > /dev/null
}

# Cuts the node off the private network, where the cluster talks; it keeps
# running and serving its tenants over its public address.
cmd_partition() {
  local node=$1 address
  address=$(private_ip "$node"); [[ -n $address ]] || die "no private address of $node"
  log "cutting $node off the private network ($address)"
  on "$node" "dev=\$(ip -o -4 addr show | awk -v a=$address '\$4 ~ \"^\"a\"/\" {print \$2}'); [ -n \"\$dev\" ] && ip link set \"\$dev\" down && echo \"down: \$dev\""
}

cmd_counter() {
  local tenant=$1 seconds=${2:-120} pairs="" client
  for node in $(names); do pairs+="$node=$(public_ip "$node") "; done
  client=$(tofu -chdir="$HERE" output -raw client 2>/dev/null || true)
  [[ -n $client ]] || die "the counter runs on the client server"
  scp -q "${SSH_OPTS[@]}" "$HERE/counter.sh" "root@$client:/root/counter.sh"
  ssh "${SSH_OPTS[@]}" "root@$client" /root/counter.sh "$seconds" "$DOMAIN" "$tenant" "'$pairs'"
}

# Asks one of Hetzner's name servers directly, so no resolver's cache
# stands between the records and the answer.
cmd_dns() {
  python3 -I "$HERE/dnsquery.py" "probe.$DOMAIN" hydrogen.ns.hetzner.com A
  python3 -I "$HERE/dnsquery.py" "probe.$DOMAIN" hydrogen.ns.hetzner.com AAAA
}

cmd_status() {
  on "${STATUS_FROM:-n2}" 'murder cluster status; echo; murder tenant list'
}

cmd=${1:-}; shift || true
case $cmd in
  wait) cmd_wait ;;
  deploy) cmd_deploy "$@" ;;
  cluster) cmd_cluster ;;
  join) cmd_join "${1:?node}" ;;
  smoke) cmd_smoke ;;
  status) cmd_status ;;
  upgrade) cmd_upgrade ;;
  image) cmd_image "${1:?archive}" ;;
  load) cmd_load "${1:-60}" ;;
  counter) cmd_counter "${1:?tenant}" "${2:-120}" ;;
  poweroff) cmd_power poweroff "${1:?node}" ;;
  poweron) cmd_power poweron "${1:?node}" ;;
  partition) cmd_partition "${1:?node}" ;;
  dns) cmd_dns ;;
  ssh) node=${1:?node}; shift; on "$node" "$@" ;;
  *) sed -n 2,18p "$0" >&2; exit 2 ;;
esac
