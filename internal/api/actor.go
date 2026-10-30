package api

import (
	"net/http"
	"os"
	"strings"
	"unicode"
)

type actorKey struct{}

// uidKey holds the user ID at the other end of the socket.
type uidKey struct{}

// agentUID is the user the agent runs as: root on a node.
var agentUID = uint32(os.Getuid())

// root reports whether the request comes from the agent's own user (root
// on a node), or from where the peer is unknown: not the agent's socket,
// such as a test's server.
func root(r *http.Request) bool {
	uid, known := r.Context().Value(uidKey{}).(uint32)
	return !known || uid == 0 || uid == agentUID
}

// ActorHeader lets a trusted client, such as the panel, name the person it
// acts for. It is recorded next to the socket user, never instead of it:
// anyone who can use the socket controls every tenant anyway.
const ActorHeader = "X-Murder-Actor"

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
	if runes := []rune(name); len(runes) > 64 {
		name = string(runes[:64])
	}
	return name
}
