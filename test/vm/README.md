# Test VM

A local QEMU/KVM virtual machine for tests that need a real ZFS kernel module
(and later Docker) on a host that has neither. It boots an Ubuntu 26.04 LTS
cloud image, whose stock kernel ships `zfs.ko`, so cloud-init only installs
`zfsutils-linux` with no DKMS build.

## Prerequisites

```sh
sudo pacman -S --needed qemu-base libisoburn openssh curl
```

You also need read/write access to `/dev/kvm` and Go on the host. The first
`up` downloads the base image (about 820 MB) and checks it against Ubuntu's
`SHA256SUMS`.

## Commands

```sh
test/vm/vm.sh up                 # create if missing, boot headless, wait for SSH + zfs
test/vm/vm.sh ssh                # interactive shell (user ubuntu, passwordless sudo)
test/vm/vm.sh ssh sudo zpool list   # args are joined and run by the guest shell, like ssh
test/vm/vm.sh copy ./file dest   # scp -r into the guest (paths relative to ~ubuntu)
test/vm/vm.sh status
test/vm/vm.sh down               # clean poweroff (ACPI, then kill as a last resort)
test/vm/vm.sh destroy            # delete overlay, vdev disk and seed; keep the base image

test/vm/go-test.sh ./... -run TestSend -v      # build on host, run as root in the VM
test/vm/vm.sh ssh 'sudo bash -s' < test/vm/zfs-smoke.sh   # ZFS send/receive smoke test
```

Every command is safe to re-run; `up` is a no-op (about 3 s) when the VM is
ready. Timings on this machine: first `up` with a fresh overlay about 5 min
(mostly `apt install zfsutils-linux`), `up` from an existing overlay about
70 s, `down` about 8 s.

Settings come from the environment: `VM_RELEASE` (`26.04` or `24.04`),
`VM_CPUS` (2), `VM_MEM` (1536 MiB), `VM_SSH_PORT` (2229, bound to 127.0.0.1
only), `VM_BOOT_TIMEOUT` (900 s) and `VM_DIR` (`<repo>/.vm`). Run `destroy`
before changing `VM_RELEASE`.

## Disks and files

Everything lives in the git-ignored `.vm/` directory:

- `images/` holds the read-only base image. It is never modified.
- `run/root.qcow2` is the copy-on-write overlay the VM boots from (about
  300 MB after provisioning, 10 GB virtual).
- `run/vdev0.qcow2` is a spare 2 GB (sparse) disk for zpools. In the guest it
  is `/dev/disk/by-id/virtio-murder-vdev0`. For more vdevs (mirrors, raidz)
  use file vdevs, for example `truncate -s 256M /var/tmp/v1`.
- `ssh/id_ed25519` is the dedicated SSH key, injected through the cloud-init
  NoCloud seed `run/seed.iso`.
- `run/serial.log` is the guest console, the first place to look when boot
  fails.

Expect about 1.2 GB on disk, growing up to about 3 GB if tests fill the vdev
disk; `destroy` reclaims everything except the base image. The VM uses up to
1.5 GB of RAM while running.

## The `zfs` build tag

Tests that need real ZFS (root, the kernel module, a spare disk) go in files
starting with `//go:build zfs`, so plain `go test ./...` on the host never
compiles them. `go-test.sh` builds every matching package that has tests with
`go test -c -tags zfs` (`GOOS=linux GOARCH=amd64 CGO_ENABLED=0`), copies each
binary and the package's `testdata/` into the VM, and runs it as root from a
copy of the package directory. It streams output and exits non-zero if any
package fails to build or pass.

- Common `go test` flags (`-run`, `-v`, `-count`, `-timeout`, `-failfast`, ...)
  are translated to `-test.*`; everything after `-args` goes to the binary
  unchanged. `-json` produces `go test -json` output.
- Tests see `MURDER_TEST_VM=1` and `MURDER_TEST_VDEV` (the spare disk path).
  Other `MURDER_TEST_*` variables are forwarded from the host.
- Tests should create and destroy their own pools, using unique pool names and
  `t.Cleanup`, because the VM is reused between runs.
- `GO_TEST_TAGS` overrides the tag list (default `zfs`), for example
  `GO_TEST_TAGS=zfs,docker` later on.
