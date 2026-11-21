# Plugins

A plugin passes tenants' requests on to a provider's API, such as OpenAI's
or HERE's, with a key the tenant never sees. It counts what each tenant
uses and can stop a tenant at a limit. Like an app, a plugin is
configuration: a kind Murder knows how to meter, and the limits its tenants
get. Murder meters OpenAI and HERE itself. For any other provider, a
plugin of kind `wasm` runs a WebAssembly module of your own, which says
where each request goes and what it used ([Plugins of your
own](#plugins-of-your-own)).

```yaml
# plugins/openai.yaml
kind: openai
limits:
  - {metric: tokens, per: month, amount: 2000000}            # hard: 429 once used
  - {metric: requests, per: day, amount: 5000, mode: soft}   # soft: only said
```

```bash
murder plugin apply plugins/openai.yaml plugins/here.yaml   # install them, named after their files
murder plugin key openai -file /root/openai.key            # the operator's key, sealed with the master key
murder plugin list                                         # kind, key, limits, the apps that list each
```

An app lists the plugins its tenants may use, and tells the app where they
are with `{{plugin.NAME.url}}`:

```yaml
# apps/web.yaml
plugins: [openai, here]
env:
  OPENAI_BASE_URL: "{{plugin.openai.url}}/v1"   # the SDKs read it
  OPENAI_API_KEY: murder                        # any value: the edge puts in the real key
  HERE_BASE_URL: "{{plugin.here.url}}"
```

`{{plugin.openai.url}}` is `http://<gateway>:4180/openai`: the edge on the
tenant's own node, at its network's gateway. The OpenAI SDKs need no change
beyond the base URL. For HERE, the path starts with the service's host:
`{{plugin.here.url}}/geocode.search.hereapi.com/v1/geocode?q=Berlin`, with
any `apiKey` or none.

An app that lists a plugin not installed is refused, as is deleting a
plugin an app lists.

## Kinds

**openai** goes to `upstream` (`https://api.openai.com` by default, or
another API that speaks OpenAI's), with `Authorization: Bearer` and the
key.
- It passes on only these endpoints:
  - `POST /v1/chat/completions`, `/v1/responses` and `/v1/completions`;
  - `POST /v1/embeddings` and `/v1/moderations`;
  - `POST /v1/images/generations`, `/edits` and `/variations`;
  - `POST /v1/audio/transcriptions` and `/translations`;
  - `GET /v1/models` and `/v1/models/ID`.
  Every other endpoint is answered `403`. A transcription billed by its
  length, and an image of a model whose answer says no `usage`, count
  only as requests.
- Endpoints that keep what a tenant sends would let every tenant reach
  every other's, under the one key. Files, batches, stored responses
  and completions, fine-tuning and vector stores are such endpoints.
- Speech and the Realtime API (WebSockets) spend without saying what. No
  protocol switch passes.
- `allow: ["POST /v1/batches", "GET /v1/batches/*"]` in the spec adds
  endpoints, `*` standing for one more path segment. What they use is
  counted only as far as their answers say it, and what they keep is
  shared by every tenant.
- It counts `requests` and `tokens`: `tokens.input`, `tokens.output`,
  their sum `tokens`, and `tokens.cached` (among the input tokens).
  Each count is kept by the model that answered.
- Tokens are read from the `usage` of the answer: Chat Completions,
  Responses, Embeddings, images and transcriptions.
- A streamed chat completion says what it used only if asked
  (`stream_options.include_usage`). The edge asks for it, and takes the
  usage chunk out again for an app that did not ask.
- The Responses API's `response.completed`, `.incomplete` and `.failed`
  events carry the usage.
- A stream that ends without saying what it used is estimated, at four
  bytes a token, from the request and the text streamed. An app that stops
  reading early is an example. The estimate is counted in the tokens, and
  also in `tokens.estimated`.
- Requests are counted without a model, since a tenant may name any.
  Tokens are counted by the model that answered, or else the one asked
  for once the provider took it. A name of more than 64 characters, or
  with others than letters, digits and `._:/-`, is counted as `other`.
- Answers come uncompressed, so the edge can read them; they stream to
  the app as they come.

**here** goes to `https://<host>/<path>` for a host in `hosts`, with the
key as the `apiKey` query parameter. The rest of the query passes as it was
written.
- The default hosts are the services that keep nothing of a request:
  - search: `geocode`, `revgeocode`, `multi-revgeocode`, `autosuggest`,
    `autocomplete`, `discover`, `browse` and `lookup`, all
    `.search.hereapi.com`;
  - routing: `router`, `matrix.router`, `isoline.router`,
    `intermodal.router` and `transit.router`, all `.hereapi.com`;
  - `wps.hereapi.com`;
  - maps and traffic: `maps`, `traffic.maps`, `vector`,
    `traffic.vector` and `data.traffic`, all `.hereapi.com`.
- Services that keep data under the account are left out: batch
  geocoding, geofencing, fleet and tour planning, and Data Hub. Every
  tenant would reach every other's data there.
- `hosts` in the spec replaces the defaults. `*.example.com` stands for
  every name below the domain.
- It counts `requests`, and `transactions` as HERE bills them (its
  supplemental pricing, April 2026). A request is one transaction, with
  these exceptions:
  - a matrix: origins × destinations, or 5 × the larger when both are 5
    or more;
  - an isoline request: one per isoline;
  - a multi reverse geocode: one per coordinate;
  - waypoint sequencing: one per 100 waypoints.
- Other exceptions, such as tour planning and map attributes, count one
  transaction a request.
- Only answers with success count transactions. Each count is kept by
  service, such as `geocode.search` or `matrix.router`. Hosts a wildcard
  allows are kept under the wildcard.
- An async matrix's results are fetched from a host outside
  `hereapi.com`. Add it to `hosts` if your tenants use one.

**wasm** runs a module: see below.

## Plugins of your own

A plugin of kind `wasm` names a WebAssembly module, which says where each
request goes and counts what it used. The edge runs the module in a
sandbox and does the rest:
- it knows the tenant and holds its limits;
- it puts the key on the request, which the module never sees;
- it counts the requests and keeps the module's counts.

```yaml
# plugins/anthropic.yaml
kind: wasm
module: anthropic.wasm        # from this file's directory
config:                       # whatever the module takes, sent with each request
  allow: ["POST /v1/messages/batches"]
limits:
  - {metric: tokens, per: month, amount: 2000000}
```

`murder plugin apply plugins/anthropic.yaml` sends the module with the
spec, which names it by its digest from then on (`sha256:…`). The agent
runs the module once, to read its manifest:
- the hosts its requests go to;
- where the key goes on them;
- the metrics it counts, which limits may name.

The module, of at most 16 MB, is kept in the replicated state, with its
backups, for as long as a plugin runs it. Each node's edge fetches it from
its agent and checks its digest. The edge keeps it in `edge-plugins` next
to `edge.routes`, a directory only root reads. An edge started while its
agent is down runs it from there.

The edge runs modules interpreted, never compiled to native code. Its unit
denies it memory both written and run (`MemoryDenyWriteExecute`), and a
module's code stays out of the process that holds every key. A call into
a module built with Go takes about a millisecond, and a new instance about
8. Reading a MB of JSON takes it about a second. A new
module is applied like any other change to the spec. Requests under way
finish on the old one. A module that no longer counts a metric a tenant's
own limit names is refused until that limit goes.

`murder plugin show NAME` shows the manifest. `murder plugin list` names
the module, as `wasm (anthropic 1.0.0)`.

The manifest's `ui` says how the panel shows the plugin (see below):
- a title and what the plugin does;
- each metric's label and unit, in order, the first the one charted;
- what counts are kept by, as `model` or `service`;
- the fields of its config, each with a label and what it is for.

A module without `ui` shows its name, and its metrics by their names.
OpenAI and HERE say theirs in Murder.

### Writing one in Go

[pluginsdk](../pluginsdk) writes a module in Go. The plugin registers its
manifest, and a function that says where each request goes:

```go
//go:build wasip1

package main

import "github.com/PowerPenguini/murder/pluginsdk"

func init() {
	pluginsdk.Register(pluginsdk.Manifest{
		Name:    "weather",
		Version: "1.0.0",
		Hosts:   []string{"api.weather.example"},
		Key:     pluginsdk.Key{Query: "appid"},  // or {Header: "Authorization", Prefix: "Bearer "}
		Metrics: []string{"calls.forecast"},
		UI: &pluginsdk.UI{Title: "Weather", Detail: "city",
			Metrics: []pluginsdk.MetricUI{{Name: "calls.forecast", Label: "Forecasts", Unit: "forecasts"}}},
	}, func(r *pluginsdk.Request) (*pluginsdk.Forward, error) {
		if r.Method != "GET" || r.Path != "/v1/forecast" {
			return nil, pluginsdk.Refuse(403, "only the forecast")
		}
		return &pluginsdk.Forward{URL: "https://api.weather.example" + r.Path + "?" + r.Query, Meter: &meter{}}, nil
	})
}

func main() {}

// meter counts a forecast answered.
type meter struct{}

func (meter) Response(r *pluginsdk.Response) bool {
	if r.Status == 200 {
		pluginsdk.Count("calls.forecast", "", 1)
	}
	return false // no need to read the body
}
func (meter) Body([]byte) bool { return false }
func (meter) End(bool)         {}
```

```bash
GOOS=wasip1 GOARCH=wasm go build -buildmode=c-shared -o weather.wasm .
```

[examples/plugins/anthropic](../examples/plugins/anthropic) is a whole
one, for Anthropic's Messages API:
- it passes on the messages, token counting and models endpoints, and
  no batches or files;
- it reads no request's body: the answer says the model;
- it reads the usage of a JSON answer of up to a MB, or of a stream's
  `message_start` and `message_delta`;
- it counts `tokens.input`, `tokens.output`, `tokens.cache_write`,
  `tokens.cache_read`, and their sum `tokens`, by the model.

What the edge does around a module:
- **The request.** The module sees the method, the path after the
  plugin's name, the query, the headers and the plugin's config. It sees
  the body too, up to the manifest's `requestBody` (at most 4 MB; larger
  bodies are answered `413`). A module that needs no body reads none, and
  bodies of any size then pass as they come. It never sees the tenant's credentials
  (`Authorization`, `X-Api-Key`, `Cookie` and the key's own header or
  parameter).
- **Where it goes.** The module answers a URL, which must be `https://`,
  on port 443, to a host of the manifest's, or of the spec's `hosts`,
  which replace them. It may set or drop headers, but not those that say
  how the request is carried (`Host`, `Content-Length`, `Cookie`,
  `Accept-Encoding`…) or the key's. It may send a body of its own in place
  of the tenant's. The edge then puts the key where the manifest says.
- **Refusing.** The module may refuse a request with a status from 400
  to 599, and a message.
- **The answer.** For a module that meters it, the edge shows the status
  and headers, then the body as it passes, uncompressed, and says when it
  ended: read whole, or not, as when the tenant stopped reading. The
  module's counts are kept by their detail, such as a model. Murder counts
  `requests` itself, by the request's detail.
- **The sandbox.** The module gets WASI's clock and randomness only:
  - no files, environment or network;
  - 128 MB of memory;
  - 10 seconds for a request, 1 for each chunk of an answer, 10 for its
    end.

  An instance serves one tenant, one request at a time, and serves it
  again: what a module keeps between requests is that tenant's only. The
  edge runs at most 256 instances at once.
- **Failures.** A module that fails, panics, runs out of time or does
  what it may not before the request goes on gets the request answered
  `502`. One that fails after it went on lets the answer pass, without
  what was still to count. That is a way past a hard limit for as long as
  the module fails. The edge logs each failure, with what the module
  wrote to its standard error, and counts them in
  `murder_plugin_module_failures_total`. The instance is then dropped.

### The interface, for other languages

A module is WebAssembly for WASI preview 1, a reactor (`_initialize`, if
it has one, is called once). It imports only `wasi_snapshot_preview1` and
exports its `memory` and these functions. A pointer and length pair is
two `i32`. An answer is one `i64`, the pointer in its high 32 bits and the
length in its low ones, valid until the next call. The edge writes each
input into memory from `murder_alloc`, which is the module's again once
the call it was for returns.

| Export | Takes | Answers |
|---|---|---|
| `murder_alloc(size) → ptr` | | memory for an input |
| `murder_manifest() → i64` | | the manifest |
| `murder_request(json, body) → i64` | the request, and its body (0, 0 for none) | where it goes |
| `murder_request_body() → i64` | | the body to send, when the answer said `body` |
| `murder_response(json) → i64` | the answer's status and headers | counts, and whether to read the body |
| `murder_body(chunk) → i32` | a chunk of the answer's body | 0 for more, 1 for enough, 2 for a failure |
| `murder_end(json) → i64` | whether the body was read whole | counts |

The JSON, version 1:

```text
manifest  {"abi": 1, "name": "x", "version": "1.0.0", "hosts": ["api.x.example"],
           "key": {"header": "X-Key", "prefix": ""} | {"query": "key"},
           "metrics": ["tokens"], "requestBody": 1048576,
           "ui": {"title", "description", "detail": "model",
                  "metrics": [{"name": "tokens", "label": "Tokens", "unit": "tokens"}],
                  "config": [{"name": "allow", "label": "More endpoints", "help": "..."}]}}
request   {"tenant", "plugin", "method", "path", "query", "header": {"Name": ["value"]}, "config": {...}}
forward   {"url", "header": {...}, "drop": [...], "body": false, "detail": "", "meter": false}
          | {"refuse": {"status": 403, "message": "..."}} | {"error": "..."}
response  {"status": 200, "header": {...}}
metered   {"counts": [{"metric", "detail", "amount"}], "body": false} | {"error": "..."}
end       {"complete": true}
```

An answer with `error` is a failure. An edge runs only modules of the
interface version it knows, so a module keeps working across Murder's
releases until the version changes.

## In the panel

The panel's Plugins page lists the plugins: kind and module, whether each
has its key, its limits, what its tenants used this month, and the tenants
at a limit, or past four fifths of one. A plugin's page shows:
- what it is, as its kind or module says;
- where its requests go, and where the key goes on them;
- its config;
- its last 30 days by day, of any metric it counts;
- each tenant against its limits;
- this month by model, service or whatever the module keeps its counts by;
- an export of this month as CSV.

There a key is set, or removed with the plugin's name typed. A tenant gets
limits of its own, none, or the plugin's again, or a key of its own. Keys
are never shown. A tenant's page has a Plugins box with its limits. The
overview says a tenant whose requests a hard limit refuses, as broken, and
one at a soft limit or near a limit.

## Keys

The operator's key is used for every tenant. It is read from a file, never
from the command line, and sealed with the master key, bound to the
plugin. The edge opens it in memory only.

`murder plugin key openai -tenant acme -file acme.key` gives a tenant a key
of its own instead. The edge then uses that key for the tenant. Murder
still counts the tenant's use, but the plugin's limits no longer hold for
it: the bill is the tenant's. Limits of the tenant's own still hold. Add
`-remove` to take a key away.

Both kinds of key are sealed again in a master key rotation, with tenants'
secrets.

## Limits

A limit caps a metric per `day` or per calendar `month`, in UTC.

- **hard** (the default): once a tenant has used the amount, its next
  requests are answered `429`, with `Retry-After` until the period ends,
  until the limit is raised.
- **soft**: requests still pass.

Either way, the audit log says `plugin.limit-near` when a tenant crosses
four fifths of a limit, and `plugin.limit-reached` when it crosses the
whole.

```bash
murder plugin limit openai -tenant acme tokens=5M/month requests=1000/day:soft   # its own, in place of the plugin's
murder plugin limit openai -tenant acme -none     # no limits at all
murder plugin limit openai -tenant acme -reset    # the plugin's again
murder plugin tenant acme                         # the limits that hold, and what it used today and this month
```

What a request costs in tokens is known only once it is answered. A hard
limit therefore refuses the *next* request. The one that crossed it is
answered in full, as is every request already under way when the limit
was reached. A tenant has at most 64 requests under way at once.

With `edge.metrics` set, the edge's `/metrics` has, for each limit of its
node's tenants, `murder_plugin_used` (what was used in the period) and
`murder_plugin_limit` (the amount), labelled by tenant, plugin, metric,
period and mode. Alerting on a soft limit uses these.

## What tenants used

```bash
murder plugin usage                                  # every tenant, this month so far
murder plugin usage -tenant acme -by day             # one tenant, a day at a time
murder plugin usage -from 2026-10-01 -to 2026-10-31 -by month -format csv > october.csv
```

The edge counts each request as it ends. Every 15 seconds it tells its
agent what it counted. The agent adds that to the replicated state, a day,
metric and detail at a time, in the same change as the report's number. A
report sent again is never counted twice. Usage is kept for 400 days, or
until its tenant is purged.

The agent answers with what the node's tenants used, cluster wide. The edge
holds limits by that, plus what it counted since.

**How far a tenant can go past a hard limit:**
- the request that crosses it, and those already under way, answered
  in full;
- while a tenant moves or fails over, what the old node's edge counted in
  its last 15 seconds;
- while the agent is down, nothing. The edge keeps its counts and goes on
  holding limits by them, and the agent takes them when it is back.

**What is lost:** the counts of the last 15 seconds when an edge stops at
once, as in a power loss or a kill. An edge that stops cleanly, or is
replaced in an upgrade, tells its counts before it goes. An edge started
while its agent is down knows nothing of what was used before. It holds
limits again once the agent answers.

In a cluster, usage is counted only once every node runs a release that
keeps it. Until then the edge keeps its counts. Usage is counted while an
upgrade has changes frozen too, as the upgrade replaces the edge then. A report the agent refuses
as malformed is dropped rather than sent again forever.

## Where the edge serves them

The edge serves plugins on `edge.pluginsPort` (4180; 0 serves none), on
every address. It answers only requests from the tenant networks of its
own node; anything else gets `403`. The tenant firewall lets the tenant
networks reach the port. `hostFirewall` keeps it closed to everyone else.

A tenant is known by the address its request comes from: its
container's own fixed address. The gateway's address is not enough: it
is the host's. The request reaches the gateway without address
translation. Tenants drop every capability, and a made-up source address
gets no answer back to complete a TCP connection. The app is given no
token of its own: `OPENAI_API_KEY` may be anything.

The edge reads only the bodies it meters: OpenAI's JSON, HERE's matrices
and multi reverse geocodes, each at most 64 MB, and a module's, up to what
its manifest says. It holds at most 512 MB of
them at once, and 128 MB of one tenant's, taking room as the bytes come.
A body that sends nothing for 30 seconds is given up on, as on the public
ports. A port taken by something else stops only the plugins: the edge
logs it and serves the rest.
The key the app sends, if any, is taken off, along with `Cookie`,
`OpenAI-Organization` and `OpenAI-Project`. The edge logs a provider's
errors without the URL (HERE's holds the key). It also takes the key out
of the headers of every answer, and out of HERE's error bodies.
