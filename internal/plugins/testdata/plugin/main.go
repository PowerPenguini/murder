// Command plugin is a plugin for the tests of the edge's: each path of a
// request does one thing a plugin may, or may not.
package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/PowerPenguini/murder/pluginsdk"
)

func main() {
	pluginsdk.Register(manifest, forward)
	pluginsdk.Run()
}

var manifest = pluginsdk.Manifest{
	Name:        "test",
	Version:     "1",
	Hosts:       []string{"api.example.com", "*.tiles.example.com"},
	Key:         pluginsdk.Key{Header: "X-Key", Prefix: "Token "},
	Metrics:     []string{"widgets", "bytes"},
	RequestBody: 1 << 20,
}

// calls counts the requests this container served.
var calls atomic.Int64

func forward(r *pluginsdk.Request) (*pluginsdk.Forward, error) {
	calls := calls.Add(1)
	to := &pluginsdk.Forward{URL: "https://api.example.com" + r.Path, Detail: "svc",
		Header: map[string][]string{"X-Calls": {fmt.Sprint(calls)}, "X-Tenant": {r.Tenant}, "X-Saw-Key": {r.Get("X-Key") + r.Get("Authorization")}}}
	if r.Query != "" {
		to.URL += "?" + r.Query
	}
	switch strings.TrimPrefix(r.Path, "/") {
	case "panic":
		panic("on purpose")
	case "sleep":
		select {
		case <-r.Context().Done():
		case <-time.After(time.Hour):
		}
	case "refuse":
		return nil, pluginsdk.Refuse(418, "no tea for %s", r.Tenant)
	case "elsewhere":
		to.URL = "https://evil.example.org/steal"
	case "plain":
		to.URL = "http://api.example.com/plain"
	case "tile":
		to.URL = "https://a.tiles.example.com/tile"
	case "header":
		to.Header["x-key"] = []string{"mine"}
	case "config":
		to.Header["X-Config"] = []string{string(r.Config)}
	case "rewrite":
		to.Body = []byte(strings.ToUpper(string(r.Body)))
	case "count":
		to.Meter = &meter{}
	case "badcount":
		to.Meter = &meter{metric: "requests"}
	}
	return to, nil
}

// meter counts the widgets a JSON answer says, and its bytes.
type meter struct {
	metric string
	body   []byte
}

func (m *meter) Response(r *pluginsdk.Response, c *pluginsdk.Counter) bool {
	if m.metric != "" {
		c.Count(m.metric, "", 1)
	}
	return r.Status == 200
}

func (m *meter) Body(chunk []byte, c *pluginsdk.Counter) bool {
	m.body = append(m.body, chunk...)
	return true
}

func (m *meter) End(complete bool, c *pluginsdk.Counter) {
	var answer struct {
		Widgets int64 `json:"widgets"`
	}
	if json.Unmarshal(m.body, &answer) == nil {
		c.Count("widgets", "blue", answer.Widgets)
	}
	detail := "partial"
	if complete {
		detail = "whole"
	}
	c.Count("bytes", detail, int64(len(m.body)))
}
