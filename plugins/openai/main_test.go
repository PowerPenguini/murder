package main

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/PowerPenguini/murder/pluginsdk"
)

func chat(body string) *pluginsdk.Request {
	return &pluginsdk.Request{Plugin: "openai", Method: "POST", Path: "/v1/chat/completions", Hosts: []string{"api.openai.com"}, Body: []byte(body)}
}

// A streamed chat completion is asked for its usage, as written, however
// the tenant wrote its stream_options: the body keeps the rest as it was.
func TestAStreamIsAskedForItsUsageAsWritten(t *testing.T) {
	for given, want := range map[string]string{
		`{"model":"m","stream":true}`:                `{"stream_options":{"include_usage":true},"model":"m","stream":true}`,
		` {}`:                                        ``,
		`{"stream":true, "stream_options":null}`:     `{"stream":true, "stream_options":{"include_usage":true}}`,
		`{"stream":true,"stream_options":{}}`:        `{"stream":true,"stream_options":{"include_usage":true}}`,
		`{"stream":true,"stream_options":{ "x":1 }}`: `{"stream":true,"stream_options":{"include_usage":true, "x":1 }}`,
		`{"stream":true,"stream_options":{"include_usage":false,"x":"}"}}`: `{"stream":true,"stream_options":{"include_usage":true,"x":"}"}}`,
		`{"stream":true,"stream_options":{"include_usage":"true"}}`:        `{"stream":true,"stream_options":{"include_usage":true}}`,
		`{"stream":true,"stream_options":{"include_usage":true}}`:          ``,
		`{"stream":"true"}`: ``,
		`{"messages":[{"content":"\"stream\":true,"}],"stream":true,"model":"m"}`:          `{"stream_options":{"include_usage":true},"messages":[{"content":"\"stream\":true,"}],"stream":true,"model":"m"}`,
		`{"messages":[{"stream":false}],"stream":true,"stream_options":{"a":[1,{"b":2}]}}`: `{"messages":[{"stream":false}],"stream":true,"stream_options":{"include_usage":true,"a":[1,{"b":2}]}}`,
	} {
		to, err := forward(chat(given))
		if err != nil {
			t.Errorf("%s: %v", given, err)
			continue
		}
		if string(to.Body) != want {
			t.Errorf("%s: sent %s, not %s", given, to.Body, want)
		}
		if want != "" && !json.Valid(to.Body) {
			t.Errorf("%s: sent %s, no JSON", given, to.Body)
		}
	}
}

// A field the plugin reads, written twice or in another case, could be
// read another way by the provider: the request is refused.
func TestAFieldWrittenTwoWaysIsRefused(t *testing.T) {
	for _, given := range []string{
		`{"stream":true,"stream_options":{"Include_Usage":true}}`,
		`{"stream":true,"stream_options":{"include_usage":false,"Include_Usage":true}}`,
		`{"stream":true,"stream_options":{"include_usage":true,"include_usage":false}}`,
		`{"stream":true,"Stream":false}`,
		`{"stream":true,"stream":false}`,
		`{"stream":true,"stream_options":{"include_usage":false},"Stream_Options":{"include_usage":true}}`,
		`{"model":"gpt-5","MODEL":"cheap"}`,
		`{"stream":true,"stream":false}`,
	} {
		_, err := forward(chat(given))
		if refusal, ok := err.(*pluginsdk.Refusal); !ok || refusal.Status != 400 {
			t.Errorf("%s: %v", given, err)
		}
	}
	// A body that is no JSON object goes on as it is, for the provider to
	// refuse.
	for _, given := range []string{`[1]`, `{"stream":`, `"x"`} {
		if to, err := forward(chat(given)); err != nil || to.Body != nil {
			t.Errorf("%s: %v %s", given, err, to.Body)
		}
	}
}

// A body longer than the plugin reads passes unread: its tokens are
// estimated by its length, at the least what the plugin reads.
func TestABodyTooLongIsEstimatedByItsLength(t *testing.T) {
	r := chat("")
	r.Body, r.BodyTooLong = nil, true
	to, err := forward(r)
	if err != nil {
		t.Fatal(err)
	}
	if m := to.Meter.(*meter); m.size <= manifest.RequestBody || to.Body != nil {
		t.Fatalf("size %d, body %d", m.size, len(to.Body))
	}
	if !manifest.PassLongBodies || !strings.Contains(manifest.UI.Config[1].Pattern, "POST") {
		t.Fatal("the manifest")
	}
}
