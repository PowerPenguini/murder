//go:build wasip1

// Command module is a plugin's module for the tests of the edge's: each
// path of a request does one thing a module may, or may not.
package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/PowerPenguini/murder/pluginsdk"
)

func init() {
	pluginsdk.Register(pluginsdk.Manifest{
		Name:        "test",
		Version:     "1",
		Hosts:       []string{"api.example.com", "*.tiles.example.com"},
		Key:         pluginsdk.Key{Header: "X-Key", Prefix: "Token "},
		Metrics:     []string{"widgets", "bytes"},
		RequestBody: 1 << 20,
	}, forward)
}

func main() {}

// calls counts the requests this instance served.
var calls int

func forward(r *pluginsdk.Request) (*pluginsdk.Forward, error) {
	calls++
	to := &pluginsdk.Forward{URL: "https://api.example.com" + r.Path, Detail: "svc",
		Header: map[string][]string{"X-Calls": {fmt.Sprint(calls)}, "X-Tenant": {r.Tenant}, "X-Saw-Key": {r.Get("X-Key") + r.Get("Authorization")}}}
	if r.Query != "" {
		to.URL += "?" + r.Query
	}
	switch strings.TrimPrefix(r.Path, "/") {
	case "spin":
		for {
		}
	case "panic":
		panic("on purpose")
	case "sleep":
		time.Sleep(time.Hour)
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

func (m *meter) Response(r *pluginsdk.Response) bool {
	if m.metric != "" {
		pluginsdk.Count(m.metric, "", 1)
	}
	return r.Status == 200
}

func (m *meter) Body(chunk []byte) bool {
	m.body = append(m.body, chunk...)
	return true
}

func (m *meter) End(complete bool) {
	var answer struct {
		Widgets int64 `json:"widgets"`
	}
	if json.Unmarshal(m.body, &answer) == nil {
		pluginsdk.Count("widgets", "blue", answer.Widgets)
	}
	detail := "partial"
	if complete {
		detail = "whole"
	}
	pluginsdk.Count("bytes", detail, int64(len(m.body)))
}
