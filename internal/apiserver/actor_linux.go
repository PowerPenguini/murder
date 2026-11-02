package apiserver

import (
	"context"
	"net"
	"os/user"
	"strconv"
	"syscall"
)

// ConnContext records the user at the other end of a unix socket connection,
// for the audit log. Use it as http.Server.ConnContext.
func ConnContext(ctx context.Context, conn net.Conn) context.Context {
	unix, ok := conn.(*net.UnixConn)
	if !ok {
		return ctx
	}
	raw, err := unix.SyscallConn()
	if err != nil {
		return ctx
	}
	var cred *syscall.Ucred
	var credErr error
	if err := raw.Control(func(fd uintptr) {
		cred, credErr = syscall.GetsockoptUcred(int(fd), syscall.SOL_SOCKET, syscall.SO_PEERCRED)
	}); err != nil || credErr != nil {
		return ctx
	}
	uid := strconv.FormatUint(uint64(cred.Uid), 10)
	name := "uid " + uid
	if found, err := user.LookupId(uid); err == nil {
		name = found.Username
	}
	return context.WithValue(context.WithValue(ctx, actorKey{}, name), uidKey{}, cred.Uid)
}
