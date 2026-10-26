package api

import (
	"context"
	"net"
	"net/http"
	"os/user"
	"strconv"
	"strings"
	"syscall"
	"unicode"
)

type actorKey struct{}

// ActorHeader lets a trusted client, such as the panel, name the person it
// acts for. It is recorded next to the socket user, never instead of it:
// anyone who can use the socket controls every tenant anyway.
const ActorHeader = "X-Corral-Actor"

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
	return context.WithValue(ctx, actorKey{}, name)
}

// actor names who made a request: the socket user, and the person a
// client says it acts for.
func actor(r *http.Request) string {
	who, _ := r.Context().Value(actorKey{}).(string)
	if who == "" {
		who = "unknown"
	}
	if behalf := cleanActor(r.Header.Get(ActorHeader)); behalf != "" {
		return behalf + " via " + who
	}
	return who
}

// cleanActor keeps a client-supplied name short and printable.
func cleanActor(name string) string {
	name = strings.Map(func(r rune) rune {
		if unicode.IsPrint(r) {
			return r
		}
		return -1
	}, strings.TrimSpace(name))
	if len(name) > 64 {
		name = name[:64]
	}
	return name
}
