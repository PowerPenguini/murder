//go:build !linux

package api

import (
	"context"
	"net"
)

// ConnContext records nobody where peer credentials are not available; the
// agent runs on Linux, where they are.
func ConnContext(ctx context.Context, _ net.Conn) context.Context { return ctx }
