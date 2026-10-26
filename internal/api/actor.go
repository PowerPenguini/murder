package api

import (
	"net/http"
	"strings"
	"unicode"
)

type actorKey struct{}

// ActorHeader lets a trusted client, such as the panel, name the person it
// acts for. It is recorded next to the socket user, never instead of it:
// anyone who can use the socket controls every tenant anyway.
const ActorHeader = "X-Corral-Actor"

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
