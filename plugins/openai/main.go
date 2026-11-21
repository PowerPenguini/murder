// Command openai is Murder's plugin for OpenAI's API, or another that
// speaks it, as Groq, Mistral or vLLM behind HTTPS do: the key goes as a
// bearer token, and tokens are counted from each answer's usage, by the
// model that answered. Murder's releases publish it as
// ghcr.io/powerpenguini/murder-openai (see plugins/Dockerfile and
// docs/plugins.md).
//
// It passes on the endpoints whose answers say what they used, and the
// models; a plugin's config adds others under allow. Its spec's hosts name
// another provider in place of OpenAI, and config's upstream a path its
// API is under, as https://api.groq.com/openai.
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"mime"
	"net/url"
	"strconv"
	"strings"

	"github.com/PowerPenguini/murder/pluginsdk"
)

func main() {
	pluginsdk.Register(manifest, forward)
	pluginsdk.Run()
}

// The metrics it counts, beyond the requests Murder counts.
const (
	metricTokens          = "tokens"
	metricInputTokens     = "tokens.input"
	metricOutputTokens    = "tokens.output"
	metricCachedTokens    = "tokens.cached"
	metricEstimatedTokens = "tokens.estimated"
)

var manifest = pluginsdk.Manifest{
	Name:    "openai",
	Version: "1.0.0",
	Hosts:   []string{"api.openai.com"},
	Key:     pluginsdk.Key{Header: "Authorization", Prefix: "Bearer "},
	Metrics: []string{metricTokens, metricInputTokens, metricOutputTokens, metricCachedTokens, metricEstimatedTokens},
	// A JSON body is read, for its model and whether it streams; audio and
	// images to edit pass on as they come.
	RequestBody:      16 << 20,
	RequestBodyTypes: []string{"application/json"},
	UI: &pluginsdk.UI{
		Title:       "OpenAI",
		Description: "OpenAI's API, or one that speaks it: tokens counted from each answer's usage, by the model that answered.",
		Detail:      "model",
		Metrics: []pluginsdk.MetricUI{
			{Name: metricTokens, Label: "Tokens", Unit: "tokens"},
			{Name: metricInputTokens, Label: "Input tokens", Unit: "tokens"},
			{Name: metricOutputTokens, Label: "Output tokens", Unit: "tokens"},
			{Name: metricCachedTokens, Label: "Cached input tokens", Unit: "tokens"},
			{Name: metricEstimatedTokens, Label: "Estimated tokens", Unit: "tokens"},
			{Name: "requests", Label: "Requests", Unit: "requests"},
		},
		Config: []pluginsdk.ConfigUI{
			{Name: "upstream", Label: "Upstream", Help: "Where the API is, as https://api.groq.com/openai: one of the plugin's hosts, and a path. The first of its hosts if left out."},
			{Name: "allow", Label: "More endpoints", Help: "Endpoints passed on beyond those whose use is counted, as POST /v1/batches: what they keep, every tenant reaches under the one key."},
		},
	},
}

// endpoints are the endpoints of OpenAI's API a tenant reaches: those
// whose answers say what they used, and the models. Those that keep or
// list what was sent (files, batches, stored responses and completions,
// fine-tuning, vector stores) are left out: every tenant would reach every
// other's, under the one key. So are those that spend without saying
// what (speech, the Realtime API's WebSockets).
var endpoints = []string{
	"POST /v1/chat/completions",
	"POST /v1/responses",
	"POST /v1/completions",
	"POST /v1/embeddings",
	"POST /v1/moderations",
	"POST /v1/images/generations",
	"POST /v1/images/edits",
	"POST /v1/images/variations",
	"POST /v1/audio/transcriptions",
	"POST /v1/audio/translations",
	"GET /v1/models",
	"GET /v1/models/*",
}

// config is what a plugin's spec may say under config.
type config struct {
	// Upstream is where the API is: https://, a host of the plugin's, and
	// a path, if any.
	Upstream string `json:"upstream"`
	// Allow are endpoints passed on beyond the plugin's own: a method and
	// a path, the path ending in /* for any one segment more.
	Allow []string `json:"allow"`
}

func forward(r *pluginsdk.Request) (*pluginsdk.Forward, error) {
	var c config
	if len(r.Config) > 0 {
		if err := json.Unmarshal(r.Config, &c); err != nil {
			return nil, pluginsdk.Refuse(500, "the plugin's config: %v", err)
		}
	}
	upstream, err := upstreamOf(c.Upstream, r.Hosts)
	if err != nil {
		return nil, pluginsdk.Refuse(500, "the plugin's config: %v", err)
	}
	path, err := url.PathUnescape(r.Path)
	if err != nil {
		return nil, pluginsdk.Refuse(400, "the path is not one to pass on")
	}
	path = strings.TrimSuffix(path, "/")
	if !allowed(append(endpoints, c.Allow...), r.Method, path) {
		return nil, pluginsdk.Refuse(403, "plugin %s does not pass %s %s on: only endpoints whose use it counts, and none that keep what tenants send, where every tenant would reach every other's (allow in the plugin's config adds more)", r.Plugin, r.Method, path)
	}
	to := &pluginsdk.Forward{URL: upstream + r.Path}
	if r.Query != "" {
		to.URL += "?" + r.Query
	}
	m := &meter{}
	if r.Body != nil {
		m.size = int64(len(r.Body))
	} else if length, err := strconv.ParseInt(r.Get("Content-Length"), 10, 64); err == nil {
		m.size = length
	}
	var request struct {
		Model         string          `json:"model"`
		Stream        bool            `json:"stream"`
		StreamOptions json.RawMessage `json:"stream_options"`
	}
	if r.Body != nil && json.Unmarshal(r.Body, &request) == nil {
		m.model, m.stream = request.Model, request.Stream
		// A streamed chat completion says what it used only if asked: it is
		// asked, and the chunk that says it taken out of what the tenant
		// gets, as the tenant did not ask.
		if request.Stream && strings.HasSuffix(path, "chat/completions") && !askedUsage(request.StreamOptions) {
			// A body the edge would not send, past what the plugin reads,
			// is sent as it was: its use is estimated.
			if rewritten, ok := askUsage(r.Body, request.StreamOptions != nil); ok && len(rewritten) <= int(manifest.RequestBody) {
				to.Body, m.strip = rewritten, true
			}
		}
	}
	to.Meter = m
	return to, nil
}

// upstreamOf is where requests go: the config's upstream, or else the
// first of the plugin's hosts.
func upstreamOf(upstream string, hosts []string) (string, error) {
	if upstream == "" {
		if len(hosts) == 0 || strings.HasPrefix(hosts[0], "*.") {
			return "", errors.New("upstream names where the API is, as the plugin's first host is a wildcard")
		}
		return "https://" + hosts[0], nil
	}
	parsed, err := url.Parse(upstream)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.Port() != "" && parsed.Port() != "443" {
		return "", errors.New("upstream must be https://, a host, and a path if any, as https://api.groq.com/openai")
	}
	return strings.TrimSuffix(upstream, "/"), nil
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

// askedUsage reports whether a request's stream_options ask for the usage.
func askedUsage(options json.RawMessage) bool {
	var asked struct {
		IncludeUsage bool `json:"include_usage"`
	}
	return options != nil && json.Unmarshal(options, &asked) == nil && asked.IncludeUsage
}

// askUsage sets stream_options.include_usage in a request's body, keeping
// the rest as it was. A body that names no stream_options gets them first,
// as written: a long body is not read and written again.
func askUsage(body []byte, named bool) ([]byte, bool) {
	if !named {
		rest := bytes.TrimLeft(body, " \t\r\n")
		if len(rest) == 0 || rest[0] != '{' {
			return nil, false
		}
		rest = rest[1:]
		out := make([]byte, 0, len(body)+40)
		out = append(out, `{"stream_options":{"include_usage":true}`...)
		if next := bytes.TrimLeft(rest, " \t\r\n"); len(next) > 0 && next[0] != '}' {
			out = append(out, ',')
		}
		return append(out, rest...), true
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(body, &fields) != nil {
		return nil, false
	}
	options := map[string]json.RawMessage{}
	if raw, ok := fields["stream_options"]; ok && string(raw) != "null" {
		if json.Unmarshal(raw, &options) != nil {
			return nil, false
		}
	}
	options["include_usage"] = json.RawMessage("true")
	encoded, err := json.Marshal(options)
	if err != nil {
		return nil, false
	}
	fields["stream_options"] = encoded
	rewritten, err := json.Marshal(fields)
	return rewritten, err == nil
}

// usage is what an answer says it used: Chat Completions and Embeddings
// name prompt and completion tokens, the Responses API, images and
// transcriptions input and output tokens.
type usage struct {
	PromptTokens        int64 `json:"prompt_tokens"`
	CompletionTokens    int64 `json:"completion_tokens"`
	InputTokens         int64 `json:"input_tokens"`
	OutputTokens        int64 `json:"output_tokens"`
	PromptTokensDetails struct {
		CachedTokens int64 `json:"cached_tokens"`
	} `json:"prompt_tokens_details"`
	InputTokensDetails struct {
		CachedTokens int64 `json:"cached_tokens"`
	} `json:"input_tokens_details"`
}

func (u usage) input() int64  { return u.PromptTokens + u.InputTokens }
func (u usage) output() int64 { return u.CompletionTokens + u.OutputTokens }
func (u usage) cached() int64 {
	return u.PromptTokensDetails.CachedTokens + u.InputTokensDetails.CachedTokens
}

// meter follows an answer for its usage: a stream event by event, JSON as
// it passes.
type meter struct {
	// model is the one asked for, until the answer names one.
	model string
	// stream is set for a request that streams; strip for one whose
	// usage chunk the plugin asked for and the tenant did not, which it
	// takes out.
	stream, strip bool
	// size is the request body's length, to estimate its tokens by.
	size  int64
	usage *usage
	// text counts the characters of text streamed, to estimate the
	// output's tokens by when the answer ends without its usage.
	text int64
	// answered is the model the answer names.
	answered string
	// json is set for an answer of JSON, kept to be read at its end.
	json bool
	kept pluginsdk.Kept
}

// Response reads only successful answers: a stream, or JSON. Audio and
// files say nothing of what they used.
func (m *meter) Response(r *pluginsdk.Response, _ *pluginsdk.Counter) bool {
	if r.Status < 200 || r.Status > 299 {
		return false
	}
	media, _, _ := mime.ParseMediaType(r.Get("Content-Type"))
	m.json = media == "application/json"
	return m.json || media == "text/event-stream"
}

// Event reads one event's data; it takes out the usage chunk the plugin
// asked for and the tenant did not.
func (m *meter) Event(data []byte, _ *pluginsdk.Counter) bool {
	if bytes.Equal(data, []byte("[DONE]")) {
		return true
	}
	var event struct {
		Model   string          `json:"model"`
		Delta   json.RawMessage `json:"delta"`
		Choices []struct {
			Delta struct {
				Content   string `json:"content"`
				Refusal   string `json:"refusal"`
				ToolCalls []struct {
					Function struct {
						Arguments string `json:"arguments"`
					} `json:"function"`
				} `json:"tool_calls"`
			} `json:"delta"`
		} `json:"choices"`
		Usage    *usage `json:"usage"`
		Response *struct {
			Model string `json:"model"`
			Usage *usage `json:"usage"`
		} `json:"response"`
	}
	if json.Unmarshal(data, &event) != nil {
		return true
	}
	if event.Model != "" {
		m.answered = event.Model
	}
	for _, choice := range event.Choices {
		m.text += int64(len(choice.Delta.Content) + len(choice.Delta.Refusal))
		for _, call := range choice.Delta.ToolCalls {
			m.text += int64(len(call.Function.Arguments))
		}
	}
	if len(event.Delta) > 0 && event.Delta[0] == '"' {
		var text string
		if json.Unmarshal(event.Delta, &text) == nil {
			m.text += int64(len(text))
		}
	}
	if event.Response != nil {
		if event.Response.Model != "" {
			m.answered = event.Response.Model
		}
		if event.Response.Usage != nil {
			m.usage = event.Response.Usage
		}
	}
	if event.Usage != nil {
		m.usage = event.Usage
		if m.strip && len(event.Choices) == 0 {
			return false
		}
	}
	return true
}

// Body keeps what a JSON answer needs to be read at its end.
func (m *meter) Body(chunk []byte, _ *pluginsdk.Counter) bool {
	m.kept.Add(chunk)
	return true
}

// End counts what the answer used, or, when it ended without saying, an
// estimate: four bytes a token, of the request and of the text streamed.
func (m *meter) End(_ bool, c *pluginsdk.Counter) {
	if m.json {
		if whole, ok := m.kept.Whole(); ok {
			var answer struct {
				Model string `json:"model"`
				Usage *usage `json:"usage"`
			}
			if json.Unmarshal(whole, &answer) == nil {
				m.answered, m.usage = answer.Model, answer.Usage
			}
		} else {
			// A long answer, as embeddings of many inputs: its usage is at
			// its end, and its model at its end or its beginning.
			var u usage
			if pluginsdk.LastField(m.kept.Tail(), "usage", &u) {
				m.usage = &u
			}
			if !pluginsdk.LastField(m.kept.Tail(), "model", &m.answered) {
				pluginsdk.LastField(m.kept.Head(), "model", &m.answered)
			}
		}
	}
	// The model that answered, or else the one asked for: the provider
	// took it, as it answered with success. The edge counts one it cannot
	// name as other.
	model := m.model
	if m.answered != "" {
		model = m.answered
	}
	if m.usage == nil {
		if m.stream || m.text > 0 {
			input, output := (m.size+3)/4, (m.text+3)/4
			c.Count(metricInputTokens, model, input)
			c.Count(metricOutputTokens, model, output)
			c.Count(metricTokens, model, input+output)
			c.Count(metricEstimatedTokens, model, input+output)
		}
		return
	}
	input, output := m.usage.input(), m.usage.output()
	c.Count(metricInputTokens, model, input)
	c.Count(metricOutputTokens, model, output)
	c.Count(metricTokens, model, input+output)
	c.Count(metricCachedTokens, model, m.usage.cached())
}
