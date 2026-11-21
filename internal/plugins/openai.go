package plugins

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// openAIHandler passes a request to OpenAI on, and what it said.
type openAIHandler struct {
	x     *exchange
	model string
	// stream is set for a streamed answer; strip for one whose usage chunk
	// the proxy asked for and the app did not, which it then takes out.
	stream, strip bool
	// size is the request body's length, to estimate its tokens by.
	size int64
}

// request reads a request's JSON body, if any, for its model and whether
// it streams: a streamed chat completion is asked to end with its usage.
// It returns where the request goes.
func (h *openAIHandler) request(r *http.Request, rest string) (*url.URL, error) {
	x := h.x
	upstream, err := url.Parse(x.spec.Upstream)
	if err != nil {
		return nil, statusError{code: http.StatusBadGateway, message: "the plugin's upstream is not a URL"}
	}
	path, err := url.PathUnescape(rest)
	if err != nil || strings.Contains("/"+path+"/", "/../") {
		return nil, statusError{code: http.StatusBadRequest, message: "the path is not one to pass on"}
	}
	if !x.spec.AllowsEndpoint(r.Method, "/"+strings.TrimSuffix(path, "/")) {
		return nil, statusError{code: http.StatusForbidden, message: fmt.Sprintf("plugin %s does not pass %s /%s on: only endpoints whose use it counts, and none that keep what tenants send, where every tenant would reach every other's (allow in the plugin's spec adds more)", x.plugin, r.Method, path)}
	}
	target := &url.URL{Scheme: upstream.Scheme, Host: upstream.Host, Path: "/" + path, RawPath: "/" + rest, RawQuery: r.URL.RawQuery}
	h.size = max(r.ContentLength, 0)
	media, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if r.Body == nil || r.Body == http.NoBody || media != "application/json" {
		return target, nil
	}
	body, err := x.readBody(r)
	if err != nil {
		return nil, err
	}
	h.size = int64(len(body))
	var request struct {
		Model         string         `json:"model"`
		Stream        bool           `json:"stream"`
		StreamOptions map[string]any `json:"stream_options"`
	}
	if json.Unmarshal(body, &request) == nil {
		h.model, h.stream = cleanDetail(request.Model), request.Stream
		// A streamed chat completion says what it used only if asked.
		if request.Stream && strings.HasSuffix(strings.TrimSuffix(path, "/"), "chat/completions") {
			if asked, _ := request.StreamOptions["include_usage"].(bool); !asked {
				if rewritten, ok := askUsage(body); ok {
					body, h.strip = rewritten, true
				}
			}
		}
	}
	r.Body = io.NopCloser(bytes.NewReader(body))
	r.ContentLength = int64(len(body))
	r.Header.Set("Content-Length", strconv.Itoa(len(body)))
	r.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(body)), nil }
	return target, nil
}

// askUsage sets stream_options.include_usage in a request's body, keeping
// the rest as it was.
func askUsage(body []byte) ([]byte, bool) {
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

// openAIUsage is what an answer says it used: Chat Completions and
// Embeddings name prompt and completion tokens, the Responses API, images
// and transcriptions input and output tokens.
type openAIUsage struct {
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

func (u openAIUsage) input() int64  { return u.PromptTokens + u.InputTokens }
func (u openAIUsage) output() int64 { return u.CompletionTokens + u.OutputTokens }
func (u openAIUsage) cached() int64 {
	return u.PromptTokensDetails.CachedTokens + u.InputTokensDetails.CachedTokens
}

// openAIMeter follows an answer for its usage.
type openAIMeter struct {
	h     *openAIHandler
	usage *openAIUsage
	model string
	// text counts the characters of text streamed, to estimate the
	// output's tokens by when the answer ends without its usage.
	text int64
	// whole keeps a JSON answer up to maxKept; past it, only its head
	// and tail.
	whole []byte
	tail  []byte
	big   bool
}

// maxKept is how much of a JSON answer is kept to read its usage from;
// past it, its last tailKept bytes, where the usage is.
const (
	maxKept  = 4 << 20
	tailKept = 64 << 10
)

// authorize puts the key on as a bearer token.
func (h *openAIHandler) authorize(out *http.Request) {
	out.Header.Set("Authorization", "Bearer "+h.x.key)
	// Read uncompressed, to count what the answer says it used.
	out.Header.Set("Accept-Encoding", "identity")
}

func (h *openAIHandler) close() {}

// response meters an answer: a stream event by event, JSON as it passes.
// Only successful answers count tokens.
func (h *openAIHandler) response(response *http.Response) {
	m := &openAIMeter{h: h}
	media, _, _ := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if response.StatusCode >= 300 {
		return
	}
	switch {
	case media == "text/event-stream":
		response.Body = newEventStream(response.Body, m.event, m.done)
		response.Header.Del("Content-Length")
		response.ContentLength = -1
	case media == "application/json":
		response.Body = &meteredBody{body: response.Body, each: m.keep, done: m.doneJSON}
	default:
		// Audio and files: what they used is not said.
	}
}

// event reads one event's data; it returns false to take the event out:
// the usage chunk the proxy asked for and the app did not.
func (m *openAIMeter) event(data []byte) bool {
	if bytes.Equal(data, []byte("[DONE]")) {
		return true
	}
	var event struct {
		Type    string          `json:"type"`
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
		Usage    *openAIUsage `json:"usage"`
		Response *struct {
			Model string       `json:"model"`
			Usage *openAIUsage `json:"usage"`
		} `json:"response"`
	}
	if json.Unmarshal(data, &event) != nil {
		return true
	}
	if event.Model != "" {
		m.model = event.Model
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
			m.model = event.Response.Model
		}
		if event.Response.Usage != nil {
			m.usage = event.Response.Usage
		}
	}
	if event.Usage != nil {
		m.usage = event.Usage
		if m.h.strip && len(event.Choices) == 0 {
			return false
		}
	}
	return true
}

// keep keeps what a JSON answer needs to be read at its end.
func (m *openAIMeter) keep(chunk []byte) {
	if !m.big && len(m.whole)+len(chunk) <= maxKept {
		m.whole = append(m.whole, chunk...)
		return
	}
	if !m.big {
		m.big, m.tail = true, m.whole
		m.whole = nil
	}
	m.tail = append(m.tail, chunk...)
	if len(m.tail) > 2*tailKept {
		m.tail = append(m.tail[:0], m.tail[len(m.tail)-tailKept:]...)
	}
}

// doneJSON reads the usage of a JSON answer.
func (m *openAIMeter) doneJSON() {
	var answer struct {
		Model string       `json:"model"`
		Usage *openAIUsage `json:"usage"`
	}
	if !m.big {
		if json.Unmarshal(m.whole, &answer) == nil {
			m.model, m.usage = answer.Model, answer.Usage
		}
	} else if usage, ok := usageInTail(m.tail); ok {
		m.usage = usage
	}
	m.done()
}

// usageInTail finds the last "usage" object in the tail of a large
// answer, such as embeddings of many inputs.
func usageInTail(tail []byte) (*openAIUsage, bool) {
	at := bytes.LastIndex(tail, []byte(`"usage"`))
	if at < 0 {
		return nil, false
	}
	rest := bytes.TrimLeft(tail[at+len(`"usage"`):], " \t\r\n")
	rest, ok := bytes.CutPrefix(rest, []byte(":"))
	if !ok {
		return nil, false
	}
	var usage openAIUsage
	if json.NewDecoder(bytes.NewReader(rest)).Decode(&usage) != nil {
		return nil, false
	}
	return &usage, true
}

// done counts what the answer used, or, when it ended without saying,
// an estimate: four bytes a token, of the request and of the text
// streamed.
func (m *openAIMeter) done() {
	// The model that answered, or else the one asked for: the provider
	// took it, as it answered with success.
	model := m.h.model
	if m.model != "" {
		model = cleanDetail(m.model)
	}
	if m.usage == nil {
		if m.h.stream || m.text > 0 {
			input, output := (m.h.size+3)/4, (m.text+3)/4
			m.h.x.count(MetricInputTokens, model, input)
			m.h.x.count(MetricOutputTokens, model, output)
			m.h.x.count(MetricTokens, model, input+output)
			m.h.x.count(MetricEstimatedTokens, model, input+output)
		}
		return
	}
	input, output := m.usage.input(), m.usage.output()
	m.h.x.count(MetricInputTokens, model, input)
	m.h.x.count(MetricOutputTokens, model, output)
	m.h.x.count(MetricTokens, model, input+output)
	m.h.x.count(MetricCachedTokens, model, m.usage.cached())
}

// eventStream passes server-sent events on, line by line, so each is
// passed as soon as it came. A data line is shown to each, which may take
// its event out; lines past maxLine pass unread.
type eventStream struct {
	body    io.ReadCloser
	reader  *bufio.Reader
	each    func([]byte) bool
	done    func()
	out     []byte
	err     error
	skip    bool
	dropped bool
	ended   bool
}

// maxLine is the longest event line read: a Responses API's completed
// event holds the whole answer.
const maxLine = 16 << 20

func newEventStream(body io.ReadCloser, each func([]byte) bool, done func()) *eventStream {
	return &eventStream{body: body, reader: bufio.NewReaderSize(body, 32<<10), each: each, done: done}
}

func (s *eventStream) Read(buffer []byte) (int, error) {
	for len(s.out) == 0 {
		if s.err != nil {
			s.end()
			return 0, s.err
		}
		s.fill()
	}
	n := copy(buffer, s.out)
	s.out = s.out[n:]
	return n, nil
}

// fill reads the next line.
func (s *eventStream) fill() {
	line, err := s.reader.ReadSlice('\n')
	if err == bufio.ErrBufferFull {
		// A long line: kept whole up to maxLine, to be read.
		whole := append([]byte(nil), line...)
		for err == bufio.ErrBufferFull && len(whole) <= maxLine {
			line, err = s.reader.ReadSlice('\n')
			whole = append(whole, line...)
		}
		if err == bufio.ErrBufferFull {
			// Past maxLine: passed on unread, to its end.
			s.out, s.skip, s.err = whole, true, nil
			return
		}
		line = whole
	} else {
		line = append([]byte(nil), line...)
	}
	s.err = err
	if s.skip {
		s.out, s.skip = line, !bytes.HasSuffix(line, []byte("\n"))
		return
	}
	trimmed := bytes.TrimRight(line, "\r\n")
	if len(trimmed) == 0 && s.dropped {
		// The blank line that ends an event taken out.
		s.dropped = false
		return
	}
	if data, ok := bytes.CutPrefix(trimmed, []byte("data:")); ok {
		if !s.each(bytes.TrimPrefix(data, []byte(" "))) {
			s.dropped = true
			return
		}
	}
	s.out = line
}

func (s *eventStream) end() {
	if !s.ended {
		s.ended = true
		s.done()
	}
}

func (s *eventStream) Close() error {
	s.end()
	return s.body.Close()
}
