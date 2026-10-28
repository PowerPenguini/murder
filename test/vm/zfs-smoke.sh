#!/usr/bin/env bash
# ZFS smoke test, run as root inside the test VM:
#   test/vm/vm.sh ssh 'sudo bash -s' < test/vm/zfs-smoke.sh
# Creates a throwaway pool on the spare disk, exercises full and incremental
# send/receive, and destroys the pool again.
set -euo pipefail

POOL=${POOL:-murdersmoke}
VDEV=${VDEV:-/dev/disk/by-id/virtio-murder-vdev0}
step() { printf '\n== %s\n' "$*"; }
t0=$(date +%s.%N)

zpool list -H "$POOL" >/dev/null 2>&1 && zpool destroy -f "$POOL"
trap 'zpool destroy -f "$POOL" 2>/dev/null || true' EXIT

step "zfs version"
zfs version

step "create pool on $VDEV"
zpool create -f -o ashift=12 -O compression=lz4 -m none "$POOL" "$VDEV"
zpool list "$POOL"

step "dataset with mountpoint, write file, snapshot"
zfs create -o mountpoint=/mnt/$POOL/src "$POOL/src"
echo "hello from $(hostname) at $(date -u +%FT%TZ)" >/mnt/$POOL/src/hello.txt
dd if=/dev/urandom of=/mnt/$POOL/src/blob bs=1M count=16 status=none
zfs snapshot "$POOL/src@s1"

step "full send | receive into $POOL/dst"
zfs send "$POOL/src@s1" | zfs receive -o mountpoint=/mnt/$POOL/dst "$POOL/dst"
cmp /mnt/$POOL/src/hello.txt /mnt/$POOL/dst/hello.txt
cmp /mnt/$POOL/src/blob /mnt/$POOL/dst/blob
echo "verified: $(cat /mnt/$POOL/dst/hello.txt)"

step "incremental snapshot and send -i"
echo "second write" >>/mnt/$POOL/src/hello.txt
echo "new file" >/mnt/$POOL/src/new.txt
zfs snapshot "$POOL/src@s2"
zfs send -i "$POOL/src@s1" "$POOL/src@s2" | zfs receive -F "$POOL/dst"
cmp /mnt/$POOL/src/hello.txt /mnt/$POOL/dst/hello.txt
cmp /mnt/$POOL/src/new.txt /mnt/$POOL/dst/new.txt
echo "verified incremental: $(tail -1 /mnt/$POOL/dst/hello.txt), new.txt present"
zfs list -t all -o name,used,refer,mountpoint -r "$POOL"

step "destroy pool"
zpool destroy "$POOL"
trap - EXIT
rmdir /mnt/$POOL/src /mnt/$POOL/dst /mnt/$POOL 2>/dev/null || true
zpool list -H "$POOL" 2>/dev/null && { echo "pool still exists" >&2; exit 1; }
awk -v a="$t0" -v b="$(date +%s.%N)" 'BEGIN { printf "\nSMOKE OK in %.1fs\n", b - a }'
