//go:build !linux

package pluginhost

import "errors"

func mountTmpfs(string, int) error { return errors.New("plugins' containers run on Linux only") }

func unmountTmpfs(string) error { return nil }
