//go:build wasip1

// Command anthropic is a plugin for Anthropic's Messages API, written as a
// module: an example of a plugin of one's own (see docs/plugins.md).
//
//	GOOS=wasip1 GOARCH=wasm go build -buildmode=c-shared -o anthropic.wasm ./examples/plugins/anthropic
//	murder plugin apply examples/plugins/anthropic/anthropic.yaml
//
// It passes on the endpoints whose answers say what they used, and the
// models; a plugin's config adds others under allow. It counts tokens by
// the model that answered: input, output, and those written to and read
// from the cache, all of them together as tokens.
package main

import (
	"bytes"
	"encoding/json"
	"mime"
	"strings"

	"github.com/PowerPenguini/murder/pluginsdk"
)

// endpoints are the endpoints passed on: those of the batches and files
// keep what a tenant sends, where every other tenant would reach it under
// the one key.
var endpoints = []string{
	"POST /v1/messages",
	"POST /v1/messages/count_tokens",
	"GET /v1/models",
	"GET /v1/models/*",
}

func init() {
	pluginsdk.Register(pluginsdk.Manifest{
		Name:    "anthropic",
		Version: "1.0.0",
		Hosts:   []string{"api.anthropic.com"},
		Key:     pluginsdk.Key{Header: "x-api-key"},
		Metrics: []string{"tokens", "tokens.input", "tokens.output", "tokens.cache_read", "tokens.cache_write"},
		UI: &pluginsdk.UI{
			Title:       "Anthropic",
			Description: "Claude through Anthropic's Messages API: tokens read from each answer's usage, streamed or not, by the model that answered.",
			Detail:      "model",
			Metrics: []pluginsdk.MetricUI{
				{Name: "tokens", Label: "Tokens", Unit: "tokens"},
				{Name: "tokens.input", Label: "Input tokens", Unit: "tokens"},
				{Name: "tokens.output", Label: "Output tokens", Unit: "tokens"},
				{Name: "tokens.cache_read", Label: "Read from the cache", Unit: "tokens"},
				{Name: "tokens.cache_write", Label: "Written to the cache", Unit: "tokens"},
				{Name: "requests", Label: "Requests", Unit: "requests"},
			},
			Config: []pluginsdk.ConfigUI{
				{Name: "allow", Label: "More endpoints", Help: "Endpoints passed on beyond messages, token counting and models, as POST /v1/messages/batches: what they keep, every tenant reaches under the one key."},
			},
		},
		// The answer says the model: no body is read, and bodies of any
		// size pass as they come.
	}, forward)
}

func main() {}

// config is what a plugin's spec may say under config.
type config struct {
	// Allow are endpoints passed on beyond the module's own.
	Allow []string `json:"allow"`
}

func forward(r *pluginsdk.Request) (*pluginsdk.Forward, error) {
	var c config
	if len(r.Config) > 0 {
		if err := json.Unmarshal(r.Config, &c); err != nil {
			return nil, pluginsdk.Refuse(500, "the plugin's config: %v", err)
		}
	}
	path := strings.TrimSuffix(r.Path, "/")
	if !allowed(append(endpoints, c.Allow...), r.Method, path) {
		return nil, pluginsdk.Refuse(403, "plugin %s does not pass %s %s on: only endpoints whose use it counts, and none that keep what tenants send", r.Plugin, r.Method, path)
	}
	url := "https://api.anthropic.com" + r.Path
	if r.Query != "" {
		url += "?" + r.Query
	}
	return &pluginsdk.Forward{URL: url, Meter: &meter{}}, nil
}

// allowed reports whether one of the endpoints is method and path; a path
// ending in /* stands for one more segment.
func allowed(endpoints []string, method, path string) bool {
	for _, endpoint := range endpoints {
		want, pattern, _ := strings.Cut(endpoint, " ")
		if want != method {
			continue
		}
		if prefix, oneMore := strings.CutSuffix(pattern, "/*"); oneMore {
			if rest, ok := strings.CutPrefix(path, prefix+"/"); ok && rest != "" && !strings.Contains(rest, "/") {
				return true
			}
		} else if path == pattern {
			return true
		}
	}
	return false
}

// usage is what an answer says it used. Input tokens are those neither
// written to the cache nor read from it.
type usage struct {
	InputTokens              *int64 `json:"input_tokens"`
	OutputTokens             *int64 `json:"output_tokens"`
	CacheCreationInputTokens *int64 `json:"cache_creation_input_tokens"`
	CacheReadInputTokens     *int64 `json:"cache_read_input_tokens"`
}

// take keeps what u says, over what was said before: a stream's
// message_delta says the totals so far.
func (u *usage) take(other *usage) {
	if other == nil {
		return
	}
	for _, field := range [][2]**int64{
		{&u.InputTokens, &other.InputTokens}, {&u.OutputTokens, &other.OutputTokens},
		{&u.CacheCreationInputTokens, &other.CacheCreationInputTokens}, {&u.CacheReadInputTokens, &other.CacheReadInputTokens},
	} {
		if *field[1] != nil {
			*field[0] = *field[1]
		}
	}
}

func value(n *int64) int64 {
	if n == nil {
		return 0
	}
	return *n
}

// meter reads an answer: JSON whole, a stream event by event.
type meter struct {
	model  string
	stream bool
	usage  usage
	said   bool
	// kept is the JSON answer, or the stream's line not yet ended.
	kept []byte
}

// maxKept is the most of a JSON answer kept to read its usage from: the
// edge runs modules interpreted, and reads it in about a second.
const maxKept = 1 << 20

func (m *meter) Response(r *pluginsdk.Response) bool {
	if r.Status < 200 || r.Status > 299 {
		return false
	}
	media, _, _ := mime.ParseMediaType(r.Get("Content-Type"))
	m.stream = media == "text/event-stream"
	return m.stream || media == "application/json"
}

func (m *meter) Body(chunk []byte) bool {
	if !m.stream {
		if len(m.kept)+len(chunk) > maxKept {
			// Too long for an answer that says its usage: none.
			m.kept = nil
			return false
		}
		m.kept = append(m.kept, chunk...)
		return true
	}
	m.kept = append(m.kept, chunk...)
	for {
		end := bytes.IndexByte(m.kept, '\n')
		if end < 0 {
			break
		}
		m.line(bytes.TrimRight(m.kept[:end], "\r"))
		m.kept = m.kept[end+1:]
	}
	if len(m.kept) > maxKept {
		m.kept = nil
	}
	return true
}

// line reads one line of a stream: message_start says the model and the
// input, message_delta the output so far.
func (m *meter) line(line []byte) {
	data, ok := bytes.CutPrefix(line, []byte("data:"))
	if !ok {
		return
	}
	var event struct {
		Type    string `json:"type"`
		Message *struct {
			Model string `json:"model"`
			Usage *usage `json:"usage"`
		} `json:"message"`
		Usage *usage `json:"usage"`
	}
	if json.Unmarshal(bytes.TrimSpace(data), &event) != nil {
		return
	}
	if event.Message != nil {
		if event.Message.Model != "" {
			m.model = event.Message.Model
		}
		m.usage.take(event.Message.Usage)
		m.said = m.said || event.Message.Usage != nil
	}
	m.usage.take(event.Usage)
	m.said = m.said || event.Usage != nil
}

func (m *meter) End(complete bool) {
	if !m.stream {
		// count_tokens, which costs nothing, says no usage.
		var answer struct {
			Model string `json:"model"`
			Usage *usage `json:"usage"`
		}
		if json.Unmarshal(bytes.TrimSpace(m.kept), &answer) != nil || answer.Usage == nil {
			return
		}
		if answer.Model != "" {
			m.model = answer.Model
		}
		m.usage.take(answer.Usage)
		m.said = true
	} else if len(m.kept) > 0 {
		m.line(bytes.TrimRight(m.kept, "\r"))
	}
	if !m.said {
		return
	}
	input, output := value(m.usage.InputTokens), value(m.usage.OutputTokens)
	written, read := value(m.usage.CacheCreationInputTokens), value(m.usage.CacheReadInputTokens)
	pluginsdk.Count("tokens.input", m.model, input)
	pluginsdk.Count("tokens.output", m.model, output)
	pluginsdk.Count("tokens.cache_write", m.model, written)
	pluginsdk.Count("tokens.cache_read", m.model, read)
	pluginsdk.Count("tokens", m.model, input+output+written+read)
}
