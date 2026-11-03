#!/bin/sh
# Installs murder from its GitHub releases, checked against the release's
# SHA-256 sums, as /usr/local/bin/murder. Then `murder setup` sets the host
# up as a node.
#
#   curl -fsSL https://github.com/PowerPenguini/murder/releases/latest/download/install.sh | sudo sh
#
# MURDER_VERSION=v0.1.0 picks a release (the latest by default).
set -eu

repo=PowerPenguini/murder
version=${MURDER_VERSION:-latest}
target=/usr/local/bin/murder

fail() {
	echo "install.sh: $*" >&2
	exit 1
}

[ "$(uname -s)" = Linux ] || fail "Murder runs on Linux only"
case $(uname -m) in
x86_64 | amd64) arch=amd64 ;;
aarch64 | arm64) arch=arm64 ;;
*) fail "no build for $(uname -m): build from source (go build ./cmd/murder)" ;;
esac
[ "$(id -u)" = 0 ] || fail "run as root, to install $target (curl … | sudo sh)"
command -v sha256sum >/dev/null || fail "sha256sum is missing"
# A node is upgraded with `murder upgrade`, which keeps tenants and
# requests going; this script only installs.
if command -v systemctl >/dev/null && systemctl is-active --quiet murder-agent 2>/dev/null; then
	fail "Murder runs here: upgrade it with \`sudo murder upgrade <new binary>\`, which upgrades every node of the cluster"
fi

# MURDER_BASE_URL serves the files from elsewhere, such as a mirror.
if [ -n "${MURDER_BASE_URL:-}" ]; then
	base=$MURDER_BASE_URL
elif [ "$version" = latest ]; then
	base=https://github.com/$repo/releases/latest/download
else
	base=https://github.com/$repo/releases/download/$version
fi

fetch() {
	if command -v curl >/dev/null; then
		curl -fsSL --retry 3 -o "$2" "$1"
	elif command -v wget >/dev/null; then
		wget -q -O "$2" "$1"
	else
		fail "neither curl nor wget is installed"
	fi
}

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT INT TERM
name=murder-linux-$arch
echo "Downloading $name ($version)"
fetch "$base/$name" "$work/$name" || fail "could not download $base/$name"
fetch "$base/SHA256SUMS" "$work/SHA256SUMS" || fail "could not download $base/SHA256SUMS"
(
	cd "$work"
	grep " $name\$" SHA256SUMS >"$name.sum" || fail "SHA256SUMS lists no $name"
	sha256sum -c --quiet "$name.sum" || fail "$name does not match its SHA-256 sum: not installed"
)
install -m 0755 "$work/$name" "$target.new"
mv -f "$target.new" "$target"
"$target" version
cat <<'NEXT'

Set this host up as a node:

  sudo murder setup -domain example.com

or join it to a cluster, with an invitation from one of its nodes:

  sudo murder setup -join -
NEXT
