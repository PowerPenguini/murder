// Package deploy holds the files a node is installed with: the systemd
// units, the sysctl settings and the Docker drop-in. "murder setup"
// installs them from the binary; they are plain files here to read and to
// install by hand.
package deploy

import "embed"

// Files are the install files, by their names in this directory.
//
//go:embed *.service *.timer murder-sysctl.conf docker-after-zfs.conf
var Files embed.FS
