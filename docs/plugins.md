# Plugins

A plugin passes tenants' requests on to a provider's API, such as OpenAI's
or HERE's, with a key the tenant never sees. It counts what each tenant
uses and can stop a tenant at a limit. A plugin is an image: a small
program, run in a container on each node, that says where each request
goes and what it used. Its spec names the image and the limits its tenants
get. Murder's releases publish plugins for OpenAI, HERE and GUS's REGON
database
([Murder's plugins](#murders-plugins)); any other provider takes a plugin
of your own ([Plugins of your own](#plugins-of-your-own)).

```yaml
# plugins/openai.yaml
kind: container
image: ghcr.io/powerpenguini/murder-openai:latest   # pinned by digest as it is installed
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

## Murder's plugins

Each release publishes three images, built from [plugins](../plugins) in
Murder's repository for amd64 and arm64, tagged with the release's
version and `latest`: `ghcr.io/powerpenguini/murder-openai`,
`ghcr.io/powerpenguini/murder-here` and `ghcr.io/powerpenguini/murder-gus`. Applying a tag pins it, as any image
of a plugin: a node keeps running what was applied until it is applied
again. Murder's plugins were kinds of their own before, `kind: openai` and
`kind: here`; such a file is now refused with the image to name instead,
and a plugin installed so goes as the database is upgraded, with tenants'
keys and limits of it, and is applied again from its image.

[examples/plugins](../examples/plugins) has their files.

**openai** goes to OpenAI, or another API that speaks OpenAI's, with
`Authorization: Bearer` and the key.
- `hosts: [api.groq.com]` in the spec names another provider. Its API is
  at the first host, or at `config.upstream`, a host of the plugin's and
  a path, as `https://api.groq.com/openai`: a tenant's request for
  `/v1/chat/completions` goes to `https://api.groq.com/openai/v1/chat/completions`.
  Only `https://`, on port 443, carries the key: a server of your own
  behind plain HTTP, such as vLLM or Ollama, needs TLS in front of it.
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
- `config: {allow: ["POST /v1/batches", "GET /v1/batches/*"]}` adds
  endpoints, `*` standing for one more path segment. What they use is
  counted only as far as their answers say it, and what they keep is
  shared by every tenant.
- It counts `requests` and `tokens`: `tokens.input`, `tokens.output`,
  their sum `tokens`, and `tokens.cached` (among the input tokens).
  Each count is kept by the model that answered.
- Tokens are read from the `usage` of the answer: Chat Completions,
  Responses, Embeddings, images and transcriptions.
- A streamed chat completion says what it used only if asked
  (`stream_options.include_usage`). The plugin asks for it, and takes the
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
  with others than letters, digits and `._:/-*`, is counted as `other`.
- It reads a JSON body of up to 16 MB, for its model and whether it
  streams; a larger one is answered `413`. Audio and images to edit pass
  as they come, at any size.
- Answers come uncompressed, so the plugin can read them; they stream to
  the app as they come.

**here** goes to `https://<host>/<path>` for a host of its own, with the
key as the `apiKey` query parameter. The rest of the query passes as it was
written.
- Its hosts are the services that keep nothing of a request:
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
- `hosts` in the spec replaces them. `*.example.com` stands for every name
  below the domain.
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
- It reads a body of up to 16 MB, to count a matrix or a multi reverse
  geocode; a larger one is answered `413`.
- An async matrix's results are fetched from a host outside
  `hereapi.com`. Add it to `hosts` if your tenants use one.

**gus** goes to GUS's REGON database, BIR1, at
`https://wyszukiwarkaregon.stat.gov.pl/wsBIR/UslugaBIRzewnPubl.svc`:
companies and their local units, found by NIP, REGON or KRS, and their
reports.
- A tenant's app uses a BIR1 client as it is, with its address set to
  `{{plugin.gus.url}}/wsBIR/UslugaBIRzewnPubl.svc` and any key. The client
  logs in (`Zaloguj`) and sends the session it gets in the `sid` header of
  its next calls, as BIR1 has it.
- BIR1 takes the key in the body of the login, not in a header. The
  plugin writes the login itself, and the edge puts the operator's key in
  it (see [the key in the body](#the-interface-for-other-languages)).
- BIR1 answers only an envelope that names its own address in `wsa:To`.
  The plugin therefore writes every call again, as SOAP 1.2, from what it
  read of the tenant's, SOAP 1.2 or 1.1. Only BIR1's operations go on:
  `Zaloguj`, `Wyloguj`, `GetValue`, `DaneSzukajPodmioty`,
  `DanePobierzPelnyRaport` and `DanePobierzRaportZbiorczy`. Their
  parameters are checked: a NIP or KRS is 10 digits, a REGON 9 or 14, a
  report's name letters and digits. Anything else is answered `400`.
- The service's description (`?wsdl`, `/wsBIR/wsdl/….wsdl`) passes too.
  It names GUS's own address, so set the client's address by hand.
- It counts `searches`, an entity for each number searched for, so a
  search of 20 NIPs counts 20, kept by `nip`, `regon` or `krs`; and
  `reports`, kept by the report's name. Both count only what BIR1 answered
  with success, found or not.
- `hosts: [wyszukiwarkaregontest.stat.gov.pl]` uses GUS's test service, with
  its published key `abcde12345abcde12345`.

## Plugins of your own

A plugin of your own is an image too: a program that says where each
request goes and counts what it used. The agent runs it in a
container on each node, and the edge calls it there, on a unix socket.
The edge does the rest:
- it knows the tenant and holds its limits;
- it puts the key on the request, which the container never sees;
- it counts the requests and keeps the container's counts.

```yaml
# plugins/anthropic.yaml
kind: container
image: ghcr.io/acme/anthropic-plugin:1.0.0   # pinned by digest as it is installed
config:                       # whatever the plugin takes, sent with each request
  allow: ["POST /v1/messages/batches"]
limits:
  - {metric: tokens, per: month, amount: 2000000}
```

`murder plugin apply plugins/anthropic.yaml` has the agent pin the image
to its digest, pulling it as a tenant's image is pulled (with the same
registry logins), and read its manifest from its label,
`io.murder.plugin.manifest`, without running it:
- the hosts its requests go to;
- where the key goes on them;
- the metrics it counts, which limits may name.

The spec keeps the image pinned (`ghcr.io/acme/anthropic-plugin@sha256:…`),
and the manifest is kept with it in the replicated state. Every node's
agent pulls the image and runs one container of it, which serves every
tenant of the node. The edge uses a container only once it says, on its
socket, the manifest its image's label said. A new image is applied like
any other change to the spec: the agent starts its container at once, and
stops the old one two minutes later. A request still under way on it
then, such as a long stream, passes on without what was left to count. An image that no longer counts a metric a tenant's own limit
names is refused until that limit goes. A local image (`sha256:<id>`,
with a note saying so) runs only where it is; push it to a registry for a
cluster.

`murder plugin show NAME` shows the manifest. `murder plugin list` names
the plugin, as `container (anthropic 1.0.0)`.

The manifest's `ui` says how the panel shows the plugin (see below):
- a title and what the plugin does;
- each metric's label and unit, in order, the first the one charted;
- what counts are kept by, as `model` or `service`;
- the fields of its config, each with a label and what it is for.

A plugin without `ui` shows its name, and its metrics by their names.

### Writing one in Go

[pluginsdk](../pluginsdk) writes a plugin in Go. The program registers
its manifest, and a function that says where each request goes, and
runs:

```go
package main

import "github.com/PowerPenguini/murder/pluginsdk"

func main() {
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
	pluginsdk.Run()
}

// meter counts a forecast answered; one is made for each request.
type meter struct{}

func (meter) Response(r *pluginsdk.Response, c *pluginsdk.Counter) bool {
	if r.Status == 200 {
		c.Count("calls.forecast", "", 1)
	}
	return false // no need to read the body
}
func (meter) Body([]byte, *pluginsdk.Counter) bool { return false }
func (meter) End(bool, *pluginsdk.Counter)         {}
```

The image holds the program, and its manifest as its label, which the
program prints with `-manifest`:

```dockerfile
FROM golang:1.26 AS build
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /plugin .

FROM scratch
COPY --from=build /plugin /plugin
ENTRYPOINT ["/plugin"]
```

```bash
docker build --label "io.murder.plugin.manifest=$(go run . -manifest)" -t ghcr.io/acme/weather-plugin:1.0.0 .
docker push ghcr.io/acme/weather-plugin:1.0.0
```

[examples/plugins/anthropic](../examples/plugins/anthropic) is a whole
one, for Anthropic's Messages API, with its Dockerfile:
- it passes on the messages, token counting and models endpoints, and
  no batches or files;
- it reads no request's body: the answer says the model;
- it reads the usage of a JSON answer, whole or, past 64 KB, from its
  head and its tail (`pluginsdk.Kept`), or of a stream's `message_start`
  and `message_delta`;
- it counts `tokens.input`, `tokens.output`, `tokens.cache_write`,
  `tokens.cache_read`, and their sum `tokens`, by the model.

What the edge does around a container:
- **The request.** The container sees the method, the path after the
  plugin's name, the query, the headers, the hosts it may send it to and
  the plugin's config. It sees the body too, up to the manifest's
  `requestBody` (at most 16 MB; larger bodies are answered `413`), of the
  media types `requestBodyTypes` names, or of any. A plugin that needs no
  body reads none, and bodies of any size, or of another type, then pass as
  they come. It never sees the tenant's
  credentials (`Authorization`, `X-Api-Key`, `Cookie` and the key's own
  header or parameter).
- **Where it goes.** The container answers a URL, which must be
  `https://`, on port 443, to a host of the manifest's, or of the spec's
  `hosts`, which replace them. It may set or drop headers, but not those
  that say how the request is carried (`Host`, `Content-Length`, `Cookie`,
  `Accept-Encoding`…) or the key's. It may send a body of its own in place
  of the tenant's. The edge then puts the key where the manifest says.
- **Refusing.** The container may refuse a request with a status from 400
  to 599, and a message.
- **The answer.** For a plugin that meters it, the edge shows the status
  and headers, then the body as it passes, uncompressed, and says when it
  ended: read whole, or not, as when the tenant stopped reading. A stream
  of server-sent events may be shown event by event instead
  (`pluginsdk.EventMeter`), and the plugin may take an event out of what
  the tenant gets, as openai takes out the usage it asked for. It cannot
  add to the answer, nor change it otherwise. The
  plugin's counts are kept by their detail, such as a model. Murder counts
  `requests` itself, by the request's detail.
- **The container.** It runs as nobody (65534), whatever its image says,
  with:
  - no network at all: what it is shown stays in it;
  - no capability, and a root it cannot write, but a 64 MB `/tmp`;
  - 256 MB of memory, one CPU and 128 processes;
  - its socket in a tmpfs of 64 KB of its own, which only root reaches on
    the host;
  - 10 seconds for a request, 1 for each chunk of an answer, 10 for its
    end, and at most 256 calls at once from the edge.

  One container serves every tenant of its node, many requests at once:
  what it keeps between requests is shared by them, so keep what is a
  tenant's by its name. Its memory is every tenant's: the edge has at
  most three bodies of 16 MB under way to it at once, two of one
  tenant's, chunks of answers and events counted with them, and waits up
  to a second for room before a request is answered `503`
  (`murder_plugin_busy`), or an answer passes without what was left to
  count. A meter keeps what it reads in little memory: `pluginsdk.Kept`
  keeps a short answer whole, and of a long one its head and its tail,
  where JSON says its model and usage. A plugin in Go has `GOMEMLIMIT` set
  below its memory. The engine starts it again when it stops, as at
  boot. `docker logs murder-plugin-…` shows what it wrote.
- **Failures.** A container that fails, panics, runs out of time or does
  what it may not before the request goes on gets the request answered
  `502`; one not running yet, or that says another manifest, `503`. One
  that fails after it went on lets the answer pass, without what was still
  to count. That is a way past a hard limit for as long as the plugin
  fails. The edge logs each failure and counts them in
  `murder_plugin_failures_total`.

### The interface, for other languages

**The key in the body.** A manifest's key may go in the body,
`"key": {"body": true}`, for an API that takes it there, as BIR1's login
does. Each request then comes with a `keyMark`, new each time, that only
the plugin is told. The plugin writes it where the key goes in a body of
its own (`replaceBody`), and the edge puts the key in its place, as it
is. The edge never does so in the tenant's body, which the tenant could
have the provider say back to it. It takes the key out of the provider's
errors, as for a key in the query.

A plugin's container serves HTTP/1.1 on a unix socket at the path
`MURDER_PLUGIN_SOCKET` says (`/run/murder-plugin/plugin.sock`), which it
creates with mode `0666`. Each call is JSON, but a chunk of an answer's
body or an event's data, which come as they are. A request, and the
answer to it, are JSON and, after a line's end (`\n`), the body's bytes,
when it has one. A request the plugin meters is a session: the edge names
it in the header `Murder-Session` of each of its calls, and ends each with
`/end`, read or not; a session the edge left, as when it stopped, may be
let go of after 10 minutes.

| Call | Takes | Answers |
|---|---|---|
| `GET /manifest` | | the manifest |
| `POST /request` | the request, and its body after it | where it goes, and the body to send in place of the tenant's after it |
| `POST /response` | the answer's status and headers | counts, whether to read the body, and whether event by event |
| `POST /body` | a chunk of the answer's body | whether it wants more |
| `POST /event` | the data of one event | whether the event stays in the answer |
| `POST /end` | whether the body was read whole | counts |

The JSON, version 3:

```text
manifest  {"abi": 3, "name": "x", "version": "1.0.0", "hosts": ["api.x.example"],
           "key": {"header": "X-Key", "prefix": ""} | {"query": "key"} | {"body": true},
           "metrics": ["tokens"], "requestBody": 1048576, "requestBodyTypes": ["application/json"],
           "ui": {"title", "description", "detail": "model",
                  "metrics": [{"name": "tokens", "label": "Tokens", "unit": "tokens"}],
                  "config": [{"name": "allow", "label": "More endpoints", "help": "..."}]}}
request   {"tenant", "plugin", "method", "path", "query", "header": {"Name": ["value"]},
           "hosts": ["api.x.example"], "config": {...}, "keyMark": "murder-key-…", "bodyFollows": true}
           \n<the body>
forward   {"url", "header": {...}, "drop": [...], "replaceBody": false,
           "detail": "", "meter": false}
           [\n<the body, with replaceBody>]
          | {"refuse": {"status": 403, "message": "..."}} | {"error": "..."}
response  {"status": 200, "header": {...}}
metered   {"counts": [{"metric", "detail", "amount"}], "body": false, "events": false} | {"error": "..."}
body      {"more": true} | {"error": "..."}
event     {"keep": true} | {"error": "..."}
end       {"complete": true}
```

An answer with `error` is a failure. An edge runs only plugins of the
interface version it knows, so a plugin keeps working across Murder's
releases until the version changes; a plugin installed from an image of
another version is answered `503` until it is applied again from one of
this.

## In the panel

The panel's Plugins page lists the plugins: kind and image, whether each
has its key, its limits, what its tenants used this month, and the tenants
at a limit, or past four fifths of one. A plugin's page shows:
- what it is, as its manifest says;
- where its requests go, and where the key goes on them;
- its config;
- its last 30 days by day, of any metric it counts;
- each tenant against its limits;
- this month by model, service or whatever the plugin keeps its counts by;
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

The edge reads only the bodies a plugin reads, up to what its manifest
says: at most 16 MB each. It holds at most 512 MB of them at once, and
64 MB of one tenant's, taking room as the bytes come.
A body that sends nothing for 30 seconds is given up on, as on the public
ports. A port taken by something else stops only the plugins: the edge
logs it and serves the rest.
The key the app sends, if any, is taken off, along with `Cookie`,
`OpenAI-Organization` and `OpenAI-Project`. The edge logs a provider's
errors without the URL (HERE's holds the key). It also takes the key out
of the headers of every answer, and out of the error bodies of a plugin
whose key goes in the query, as HERE's, or in the body, as GUS's.
