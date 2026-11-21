// Package pluginsdk writes plugins for Murder in Go: a WebAssembly module
// that says where a tenant's request goes and counts what it used, built
// with
//
//	GOOS=wasip1 GOARCH=wasm go build -buildmode=c-shared -o plugin.wasm .
//
// A plugin's main package registers it, and has an empty main:
//
//	func init() { pluginsdk.Register(manifest, forward) }
//	func main() {}
//
// The edge runs the module in a sandbox: it never sees the key, which the
// edge puts on each request where the manifest says, and its requests go
// only to the hosts the manifest names, over HTTPS. Each instance serves
// one tenant, one request at a time, so what it keeps between requests is
// only ever that tenant's. docs/plugins.md says the interface in full, for
// a module written in another language.
package pluginsdk

import (
	"encoding/json"
	"fmt"
	"net/textproto"
)

// ABI is the version of the interface this package builds modules for.
const ABI = 1

// Manifest is what the module says of itself: where its requests go,
// where the key goes on them, and what it counts.
type Manifest struct {
	// ABI is filled in by Register.
	ABI int `json:"abi"`
	// Name and Version say what the module is, as anthropic 1.2.0: Name
	// of lowercase letters, digits and ._-.
	Name    string `json:"name"`
	Version string `json:"version,omitempty"`
	// Hosts are the hosts its requests go to, over HTTPS: a name, or "*."
	// and a domain for every name below it. The key goes to no other.
	Hosts []string `json:"hosts"`
	// Key is where the edge puts the key on each request.
	Key Key `json:"key"`
	// Metrics are what it counts, beyond the requests Murder counts: the
	// metrics a plugin's limits may name, as tokens or tokens.input.
	Metrics []string `json:"metrics,omitempty"`
	// RequestBody is the most bytes of a request's body it reads, at most
	// 4 MB; 0 reads none, and bodies pass on as they come. The edge runs
	// modules interpreted: one built with Go reads a MB of JSON in about a
	// second.
	RequestBody int64 `json:"requestBody,omitempty"`
	// UI says how Murder's panel shows the plugin; nil shows its name and
	// its metrics' names only.
	UI *UI `json:"ui,omitempty"`
}

// UI is how Murder's panel shows a plugin: a title and what it does, how
// its metrics read (the first of them charted), what its counts are kept
// by, and the fields of its config.
type UI struct {
	Title       string `json:"title,omitempty"`
	Description string `json:"description,omitempty"`
	// Detail names what Count's detail is, as model or service.
	Detail  string     `json:"detail,omitempty"`
	Metrics []MetricUI `json:"metrics,omitempty"`
	Config  []ConfigUI `json:"config,omitempty"`
}

// MetricUI is how a metric reads: a label, as Input tokens, and the unit
// of its amounts, as tokens. Name may also be requests, which Murder
// counts.
type MetricUI struct {
	Name  string `json:"name"`
	Label string `json:"label,omitempty"`
	Unit  string `json:"unit,omitempty"`
}

// ConfigUI is a field of the plugin's config: its name in the config, a
// label, and what it is for.
type ConfigUI struct {
	Name  string `json:"name"`
	Label string `json:"label,omitempty"`
	Help  string `json:"help,omitempty"`
}

// Key is where the key goes on a request: in a header, after a prefix, as
// in Authorization: Bearer KEY, or in a parameter of the query.
type Key struct {
	Header string `json:"header,omitempty"`
	Prefix string `json:"prefix,omitempty"`
	Query  string `json:"query,omitempty"`
}

// Request is a tenant's request to the plugin, as the edge shows it to
// the module: without the tenant's credentials, or the key's header or
// parameter.
type Request struct {
	// Tenant and Plugin say whose request it is, and to which plugin.
	Tenant string `json:"tenant"`
	Plugin string `json:"plugin"`
	Method string `json:"method"`
	// Path is what follows the plugin's name: for
	// http://<gateway>:4180/anthropic/v1/messages, /v1/messages, as the
	// tenant wrote it.
	Path string `json:"path"`
	// Query is the query as the tenant wrote it, without the ?.
	Query  string              `json:"query,omitempty"`
	Header map[string][]string `json:"header"`
	// Config is the plugin's config, from its spec.
	Config json.RawMessage `json:"config,omitempty"`
	// Body is the request's body, up to the manifest's RequestBody; nil
	// for none, or when the manifest reads none.
	Body []byte `json:"-"`
}

// Get returns the first value of a header of the request.
func (r *Request) Get(name string) string {
	if values := r.Header[textproto.CanonicalMIMEHeaderKey(name)]; len(values) > 0 {
		return values[0]
	}
	return ""
}

// Forward is where a request goes on, and how it is metered.
type Forward struct {
	// URL is where the request goes: https:// and one of the manifest's
	// hosts.
	URL string
	// Header sets headers on the request, Drop takes them off: neither
	// those that say how it is carried, such as Host or Content-Length,
	// nor the key's.
	Header map[string][]string
	Drop   []string
	// Body, when not nil, is sent in place of the request's body.
	Body []byte
	// Detail is what the request is counted for, such as a model or a
	// service: at most 64 letters, digits and ._:/-, or else "other".
	Detail string
	// Meter reads the answer; nil counts only the request.
	Meter Meter
}

// Meter reads the provider's answer to one request, and counts what it
// used with Count.
type Meter interface {
	// Response is shown the answer's status and headers; it reports
	// whether Body should be shown the answer's body.
	Response(*Response) bool
	// Body is shown the answer's body, a chunk at a time, as it passes on
	// to the tenant; it reports whether it wants more.
	Body(chunk []byte) bool
	// End is told the body ended: read whole, or not, as when the tenant
	// stopped reading it. It is called only for a body Response asked
	// for.
	End(complete bool)
}

// Response is the provider's answer, as the edge shows it to the module.
type Response struct {
	Status int                 `json:"status"`
	Header map[string][]string `json:"header"`
}

// Get returns the first value of a header of the answer.
func (r *Response) Get(name string) string {
	if values := r.Header[textproto.CanonicalMIMEHeaderKey(name)]; len(values) > 0 {
		return values[0]
	}
	return ""
}

// Refusal refuses a request: returned by the function Register takes,
// the tenant is answered with its status and message.
type Refusal struct {
	Status  int    `json:"status"`
	Message string `json:"message"`
}

func (r *Refusal) Error() string { return r.Message }

// Refuse refuses a request with a status from 400 to 599, and a message.
func Refuse(status int, format string, args ...any) error {
	return &Refusal{Status: status, Message: fmt.Sprintf(format, args...)}
}

// count is one count, as the module answers it.
type count struct {
	Metric string `json:"metric"`
	Detail string `json:"detail"`
	Amount int64  `json:"amount"`
}

var counted []count

// Count counts an amount of one of the manifest's metrics, for a detail
// such as a model; from a Meter's methods.
func Count(metric, detail string, amount int64) {
	counted = append(counted, count{Metric: metric, Detail: detail, Amount: amount})
}
