//go:build !linux

package apiserver

import (
	"context"
	"net"
)

// ConnContext records nobody where peer credentials are not available; the
// agent runs on Linux, where they are.
func ConnContext(ctx context.Context, _ net.Conn) context.Context { return ctx }
