#!/usr/bin/env bash
# Cross-compile Go test binaries on the host and run them as root in the test VM.
#
#   test/vm/go-test.sh <package-pattern> [go test flags] [-args custom flags]
#
# Run it from the module you want to test (normally the repo root). Common
# `go test` flags (-run, -v, -count, -timeout, ...) are translated to the
# compiled binary's -test.* form. Everything after -args is passed unchanged.
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/../.." && pwd)
VM=$SCRIPT_DIR/vm.sh
VM_DIR=${VM_DIR:-$REPO_ROOT/.vm}
export VM_DIR
GO_TEST_TAGS=${GO_TEST_TAGS:-zfs}
VDEV=/dev/disk/by-id/virtio-murder-vdev0

die() { echo "go-test.sh: error: $*" >&2; exit 1; }

(($# >= 1)) || die "usage: $0 <package-pattern> [go test flags] [-args ...]"
pattern=$1; shift

# Translate go test flags into test binary flags.
testflags='artifacts bench benchmem benchtime blockprofile blockprofilerate count coverprofile cpu cpuprofile
failfast fullpath fuzz fuzzminimizetime fuzztime list memprofile memprofilerate mutexprofile mutexprofilefraction outputdir
parallel run short shuffle skip timeout trace v'
bin_args=()
json=0
while (($#)); do
  a=$1; shift
  if [[ $a == -args || $a == --args ]]; then bin_args+=("$@"); break; fi
  if [[ $a == -json || $a == --json ]]; then json=1; continue; fi
  if [[ $a =~ ^--?([a-z]+)(=.*)?$ ]] && [[ " ${testflags//$'\n'/ } " == *" ${BASH_REMATCH[1]} "* ]]; then
    bin_args+=("-test.${BASH_REMATCH[1]}${BASH_REMATCH[2]}")
  else
    bin_args+=("$a")
  fi
done

mapfile -t pkgs < <(go list -tags "$GO_TEST_TAGS" \
  -f '{{if or .TestGoFiles .XTestGoFiles}}{{.ImportPath}}|{{.Dir}}{{end}}' "$pattern")
((${#pkgs[@]})) || die "no test packages match '$pattern' (with -tags $GO_TEST_TAGS)"

mkdir -p "$VM_DIR/run"
build=$(mktemp -d "$VM_DIR/run/gotest.XXXXXX")
remote=/tmp/murder-gotest.$$.$RANDOM
cleanup() {
  "$VM" ssh "rm -rf $remote" >/dev/null 2>&1 || true
  rm -rf "$build"
}
trap cleanup EXIT

# Make sure the VM is running and ready (no-op when it already is).
"$VM" up >/dev/null 2>"$build/up.log" || { cat "$build/up.log" >&2; die "vm.sh up failed"; }

# Keep a tty when interactive so Ctrl-C reaches the remote test binary.
tty_flag=()
[[ -t 0 && -t 1 ]] && tty_flag=(-t)

# Forward MURDER_TEST_* variables from the host environment.
envs=(MURDER_TEST_VM=1 "MURDER_TEST_VDEV=$VDEV")
while IFS='=' read -r k _; do
  [[ $k == MURDER_TEST_* && $k != MURDER_TEST_VM && $k != MURDER_TEST_VDEV ]] && envs+=("$k=${!k}")
done < <(env)

summary_fd=1
((json)) && summary_fd=2   # keep stdout pure JSON
status=0
for entry in "${pkgs[@]}"; do
  pkg=${entry%%|*}; dir=${entry#*|}
  name=$(tr '/.' '__' <<<"$pkg").test
  out=$build/${name%.test}
  mkdir -p "$out"
  if ! GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go test -c -tags "$GO_TEST_TAGS" -o "$out/$name" "$pkg"; then
    echo "FAIL	$pkg [build failed]" >&$summary_fd; status=1; continue
  fi
  [[ -d $dir/testdata ]] && cp -r "$dir/testdata" "$out/"
  rdir=$remote/${name%.test}
  tar -C "$out" -cf - . | "$VM" ssh "mkdir -p $rdir && tar -C $rdir -xf -" \
    || die "copying $name into the VM failed"

  # The binary runs from a copy of its package directory (holding testdata/), as go test does.
  cmd="cd $rdir && sudo env $(printf '%q ' "${envs[@]}")./$name"
  ((${#bin_args[@]})) && cmd+=" $(printf '%q ' "${bin_args[@]}")"
  start=$SECONDS
  rc=0
  if ((json)); then
    # Same as go test -json: verbose test2json output converted on the host.
    "$VM" ssh "$cmd -test.v=test2json" 2>&1 | go tool test2json -t -p "$pkg" || true
    rc=${PIPESTATUS[0]}
  else
    "$VM" ssh "${tty_flag[@]}" "$cmd" || rc=$?
  fi
  if ((rc == 0)); then
    echo "ok  	$pkg	$((SECONDS - start))s" >&$summary_fd
  else
    echo "FAIL	$pkg	$((SECONDS - start))s (exit $rc)" >&$summary_fd; status=1
  fi
done
exit $status
