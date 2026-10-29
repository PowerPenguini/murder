#!/usr/bin/env bash
# Local QEMU/KVM test VM with real ZFS (Ubuntu LTS cloud image).
# See test/vm/README.md. Every subcommand is safe to re-run.
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/../.." && pwd)

VM_DIR=${VM_DIR:-$REPO_ROOT/.vm}
VM_RELEASE=${VM_RELEASE:-26.04}          # Ubuntu LTS: 26.04 (resolute) or 24.04 (noble)
VM_CPUS=${VM_CPUS:-2}
VM_MEM=${VM_MEM:-1536}                   # MiB
VM_DISK_SIZE=${VM_DISK_SIZE:-10G}        # virtual size of the root overlay (sparse)
VM_VDEV_SIZE=${VM_VDEV_SIZE:-2G}         # virtual size of the spare zpool disk (sparse)
VM_SSH_PORT=${VM_SSH_PORT:-2229}         # host port forwarded to guest :22 on 127.0.0.1
VM_BOOT_TIMEOUT=${VM_BOOT_TIMEOUT:-900}  # seconds `up` waits for SSH + cloud-init + zfs
VM_USER=ubuntu

IMG_DIR=$VM_DIR/images
KEY=$VM_DIR/ssh/id_ed25519
RUN=$VM_DIR/run
OVERLAY=$RUN/root.qcow2
VDEV=$RUN/vdev0.qcow2
SEED=$RUN/seed.iso
PIDFILE=$RUN/qemu.pid
PORTFILE=$RUN/ssh.port
MONITOR=$RUN/monitor.sock
SERIAL=$RUN/serial.log

die() { echo "vm.sh: error: $*" >&2; exit 1; }
log() { echo "vm.sh: $*" >&2; }

release_codename() {
  case $VM_RELEASE in
    24.04) echo noble ;;
    26.04) echo resolute ;;
    *) die "unsupported VM_RELEASE=$VM_RELEASE (use 24.04 or 26.04)" ;;
  esac
}

base_image() { echo "$IMG_DIR/ubuntu-$VM_RELEASE-server-cloudimg-amd64.img"; }

need() {
  local missing=()
  for c in "$@"; do command -v "$c" >/dev/null || missing+=("$c"); done
  ((${#missing[@]} == 0)) || die "missing host tools: ${missing[*]} (sudo pacman -S --needed qemu-base libisoburn openssh)"
}

qemu_pid() {
  [[ -f $PIDFILE ]] || return 1
  local pid; pid=$(<"$PIDFILE")
  [[ -n $pid ]] && kill -0 "$pid" 2>/dev/null && grep -q qemu "/proc/$pid/cmdline" 2>/dev/null || return 1
  echo "$pid"
}

is_running() { qemu_pid >/dev/null; }

ssh_port() { if [[ -f $PORTFILE ]]; then cat "$PORTFILE"; else echo "$VM_SSH_PORT"; fi; }

ssh_opts() {
  printf '%s\n' -i "$KEY" -p "$(ssh_port)" \
    -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR \
    -o BatchMode=yes -o ConnectTimeout=5 -o ServerAliveInterval=15 -o IdentitiesOnly=yes
}

vssh() {
  local opts; mapfile -t opts < <(ssh_opts)
  ssh "${opts[@]}" "$VM_USER@127.0.0.1" "$@"
}

ensure_key() {
  [[ -f $KEY ]] && return
  mkdir -p "$(dirname "$KEY")"; chmod 700 "$(dirname "$KEY")"
  ssh-keygen -q -t ed25519 -N '' -C murder-test-vm -f "$KEY"
  log "generated SSH key $KEY"
}

ensure_base() {
  local img url name sums
  img=$(base_image); name=$(basename "$img")
  [[ -f $img ]] && return
  mkdir -p "$IMG_DIR"
  url=https://cloud-images.ubuntu.com/releases/$(release_codename)/release
  log "downloading $name (~0.6-0.9 GB) from $url"
  sums=$(curl -fsSL "$url/SHA256SUMS") || die "cannot fetch $url/SHA256SUMS"
  curl -fL --progress-bar -o "$img.part" "$url/$name" || { rm -f "$img.part"; die "download failed"; }
  local want got
  want=$(awk -v n="*$name" '$2==n || $2==substr(n,2) {print $1}' <<<"$sums")
  [[ -n $want ]] || { rm -f "$img.part"; die "no checksum for $name in SHA256SUMS"; }
  got=$(sha256sum "$img.part" | awk '{print $1}')
  [[ $want == "$got" ]] || { rm -f "$img.part"; die "checksum mismatch for $name"; }
  grep -F "$name" <<<"$sums" >"$img.sha256"
  curl -fsSL "$url/unpacked/build-info.txt" >"$img.build-info" 2>/dev/null || true
  chmod 444 "$img.part"; mv "$img.part" "$img"
  log "base image ready: $img"
}

make_seed() {
  local dir; dir=$(mktemp -d "$RUN/seed.XXXXXX")
  cat >"$dir/meta-data" <<EOF
instance-id: murder-test-vm
local-hostname: murder-vm
EOF
  cat >"$dir/user-data" <<EOF
#cloud-config
hostname: murder-vm
ssh_pwauth: false
users:
  - name: $VM_USER
    groups: [sudo]
    shell: /bin/bash
    sudo: ALL=(ALL) NOPASSWD:ALL
    lock_passwd: true
    ssh_authorized_keys:
      - $(cat "$KEY.pub")
package_update: true
package_upgrade: false
packages:
  - zfsutils-linux
  - docker.io
  - nftables
runcmd:
  # Keep apt quiet so tests never race background upgrades for the dpkg lock.
  - [systemctl, disable, --now, apt-daily.timer, apt-daily-upgrade.timer, unattended-upgrades.service]
  - [modprobe, zfs]
EOF
  cat >"$dir/network-config" <<EOF
version: 2
ethernets:
  nic0:
    match: {name: "e*"}
    dhcp4: true
EOF
  xorriso -as mkisofs -quiet -output "$SEED" -volid cidata -joliet -rock \
    "$dir/user-data" "$dir/meta-data" "$dir/network-config" 2>/dev/null
  rm -rf "$dir"
}

port_free() {
  ! (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null
}

cmd_up() {
  need qemu-system-x86_64 qemu-img xorriso ssh ssh-keygen curl sha256sum flock
  [[ -r /dev/kvm && -w /dev/kvm ]] || die "/dev/kvm is not accessible (add yourself to the kvm group)"
  mkdir -p "$RUN"
  exec 9>"$VM_DIR/.lock"
  flock -w 900 9 || die "another vm.sh up/down is holding $VM_DIR/.lock"

  if is_running; then
    log "already running (pid $(qemu_pid), ssh 127.0.0.1:$(ssh_port))"
  else
    ensure_key
    ensure_base
    if [[ ! -f $OVERLAY ]]; then
      qemu-img create -q -f qcow2 -F qcow2 -b "$(base_image)" "$OVERLAY" "$VM_DISK_SIZE"
      log "created overlay $OVERLAY on $(basename "$(base_image)")"
    fi
    [[ -f $VDEV ]] || qemu-img create -q -f qcow2 "$VDEV" "$VM_VDEV_SIZE"
    [[ -f $SEED ]] || make_seed
    port_free "$VM_SSH_PORT" || die "127.0.0.1:$VM_SSH_PORT is in use; set VM_SSH_PORT to a free port"
    echo "$VM_SSH_PORT" >"$PORTFILE"
    rm -f "$MONITOR" "$PIDFILE"
    : >"$SERIAL"
    qemu-system-x86_64 \
      -name murder-test-vm -machine q35,accel=kvm -cpu host \
      -smp "$VM_CPUS" -m "$VM_MEM" \
      -drive "file=$OVERLAY,if=none,id=root,format=qcow2,discard=unmap" \
      -device virtio-blk-pci,drive=root,bootindex=0 \
      -drive "file=$VDEV,if=none,id=vdev0,format=qcow2,discard=unmap" \
      -device virtio-blk-pci,drive=vdev0,serial=murder-vdev0 \
      -drive "file=$SEED,if=none,id=seed,format=raw,readonly=on" \
      -device virtio-blk-pci,drive=seed \
      -netdev "user,id=net0,hostfwd=tcp:127.0.0.1:$VM_SSH_PORT-:22" \
      -device virtio-net-pci,netdev=net0 \
      -device virtio-rng-pci \
      -display none -serial "file:$SERIAL" \
      -monitor "unix:$MONITOR,server,nowait" \
      -pidfile "$PIDFILE" -daemonize 9>&- \
      || die "qemu failed to start (see output above)"
    log "booting (pid $(<"$PIDFILE"), ssh 127.0.0.1:$VM_SSH_PORT, console log $SERIAL)"
  fi

  local deadline=$((SECONDS + VM_BOOT_TIMEOUT))
  until vssh true 2>/dev/null; do
    is_running || die "VM exited during boot; see $SERIAL"
    ((SECONDS < deadline)) || die "timed out waiting for SSH; see $SERIAL"
    sleep 2
  done
  local rc=0
  vssh 'cloud-init status --wait >/dev/null' || rc=$?
  # cloud-init exits 2 for "done with recoverable errors"; zfs checks below decide.
  ((rc == 0 || rc == 2)) || die "cloud-init failed (rc=$rc); try: $0 ssh sudo cloud-init status --long"
  vssh 'sudo modprobe zfs && zfs version' >&2 || die "zfs is not usable in the VM; try: $0 ssh sudo cloud-init status --long"
  vssh 'sudo docker version --format "docker {{.Server.Version}}" && sudo nft --version' >&2 \
    || die "docker or nft is not usable in the VM (a VM made before they were added needs destroy and up)"
  log "ready (ssh 127.0.0.1:$(ssh_port))"
}

cmd_ssh() {
  is_running || die "VM is not running; run: $0 up"
  if (($# == 0)); then
    local opts; mapfile -t opts < <(ssh_opts)
    exec ssh -t "${opts[@]}" "$VM_USER@127.0.0.1"
  fi
  # Like ssh(1): arguments are joined with spaces and run by the guest's shell.
  # A leading -t allocates a tty so Ctrl-C reaches the remote command.
  local opts; mapfile -t opts < <(ssh_opts)
  if [[ $1 == -t ]]; then shift; opts+=(-t); fi
  (($#)) || die "usage: $0 ssh [-t] [cmd...]"
  exec ssh "${opts[@]}" "$VM_USER@127.0.0.1" "$@"
}

cmd_copy() {
  (($# == 2)) || die "usage: $0 copy <local-path> <remote-path>"
  is_running || die "VM is not running; run: $0 up"
  [[ -e $1 ]] || die "no such local path: $1"
  local opts; mapfile -t opts < <(ssh_opts)
  # scp uses -P for the port; translate the -p from ssh_opts.
  local i; for i in "${!opts[@]}"; do [[ ${opts[$i]} == -p ]] && opts[$i]=-P; done
  exec scp -q -r "${opts[@]}" "$1" "$VM_USER@127.0.0.1:$2"
}

monitor_cmd() {
  [[ -S $MONITOR ]] && command -v python3 >/dev/null || return 1
  python3 -I - "$MONITOR" "$1" <<'PY' 2>/dev/null
import socket, sys, time
s = socket.socket(socket.AF_UNIX); s.settimeout(3); s.connect(sys.argv[1])
s.sendall((sys.argv[2] + "\n").encode()); time.sleep(0.3); s.close()
PY
}

wait_exit() {
  local pid=$1 secs=$2
  for ((i = 0; i < secs * 2; i++)); do kill -0 "$pid" 2>/dev/null || return 0; sleep 0.5; done
  return 1
}

cmd_down() {
  local pid
  if ! pid=$(qemu_pid); then
    log "not running"; rm -f "$PIDFILE" "$MONITOR"; return 0
  fi
  exec 9>"$VM_DIR/.lock"; flock -w 120 9 || die "another vm.sh command holds $VM_DIR/.lock"
  log "shutting down (pid $pid)"
  vssh 'sudo systemctl poweroff' >/dev/null 2>&1 || true
  if ! wait_exit "$pid" 60; then
    log "guest did not power off over SSH; sending ACPI powerdown"
    monitor_cmd system_powerdown || true
    if ! wait_exit "$pid" 30; then
      log "still running; killing qemu"
      kill "$pid" 2>/dev/null || true
      wait_exit "$pid" 10 || kill -9 "$pid" 2>/dev/null || true
    fi
  fi
  rm -f "$PIDFILE" "$MONITOR"
  log "stopped"
}

cmd_destroy() {
  cmd_down
  rm -rf "$RUN"
  log "removed $RUN (overlay, vdev disk, seed); kept base images in $IMG_DIR and key in $(dirname "$KEY")"
}

cmd_status() {
  local img; img=$(base_image)
  echo "vm dir:   $VM_DIR"
  if [[ -f $img ]]; then echo "base:     $img ($(du -h "$img" | cut -f1))"; else echo "base:     missing (downloaded by 'up')"; fi
  if [[ -f $OVERLAY ]]; then echo "overlay:  $OVERLAY ($(du -h "$OVERLAY" | cut -f1) used)"; else echo "overlay:  none"; fi
  [[ -f $VDEV ]] && echo "vdev:     $VDEV ($(du -h "$VDEV" | cut -f1) used) -> guest /dev/disk/by-id/virtio-murder-vdev0"
  echo "disk:     $(du -sh "$VM_DIR" 2>/dev/null | cut -f1) total under $VM_DIR"
  if is_running; then
    echo "state:    running (pid $(qemu_pid))"
    if vssh true 2>/dev/null; then
      echo "ssh:      ok (127.0.0.1:$(ssh_port), user $VM_USER)"
    else
      echo "ssh:      not reachable yet (127.0.0.1:$(ssh_port))"
    fi
  else
    echo "state:    stopped"
  fi
}

usage() {
  cat <<EOF
usage: $0 <command>
  up               create the VM if missing, boot it, wait for SSH and zfs
  ssh [-t] [cmd...] open a shell, or run cmd in the guest (args joined like ssh)
  copy <src> <dst> copy a local file/dir to a guest path (relative to ~$VM_USER)
  down             shut the VM down cleanly
  destroy          shut down and delete overlay, vdev disk and seed (keeps base image)
  status           show image, disk usage and VM state
env: VM_RELEASE=$VM_RELEASE VM_CPUS=$VM_CPUS VM_MEM=$VM_MEM VM_SSH_PORT=$VM_SSH_PORT VM_DIR=$VM_DIR
EOF
}

case ${1:-} in
  up) shift; cmd_up "$@" ;;
  ssh) shift; cmd_ssh "$@" ;;
  copy) shift; cmd_copy "$@" ;;
  down) shift; cmd_down ;;
  destroy) shift; cmd_destroy ;;
  status) shift; cmd_status ;;
  -h | --help | help) usage ;;
  *) usage >&2; exit 2 ;;
esac
