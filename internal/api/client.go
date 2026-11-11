package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/PowerPenguini/murder/internal/access"
	"github.com/PowerPenguini/murder/internal/spec"
	"github.com/PowerPenguini/murder/internal/store"
)

// Client calls the agent's API over its unix socket.
type Client struct {
	http *http.Client
	base string
	// Actor, if set, names the person the client acts for in the audit log.
	Actor string
	// atOnce moves every tenant to an app change at once (AtOnce); first
	// those named, as its first wave (First).
	atOnce bool
	first  []string
}

// NewClient returns a client for the agent listening on socket.
func NewClient(socket string) *Client {
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		var dialer net.Dialer
		return dialer.DialContext(ctx, "unix", socket)
	}}
	return &Client{http: &http.Client{Transport: transport}, base: "http://murder"}
}

// NewClientURL returns a client for an agent API served over TCP at base,
// such as an httptest server.
func NewClientURL(base string) *Client { return &Client{http: http.DefaultClient, base: base} }

func (c *Client) do(ctx context.Context, method, path string, body, out any) error {
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(encoded)
	}
	request, err := http.NewRequestWithContext(ctx, method, c.base+path, reader)
	if err != nil {
		return err
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if c.Actor != "" {
		request.Header.Set(ActorHeader, c.Actor)
	}
	if lease := LeaseOf(ctx); lease != "" {
		request.Header.Set(LeaseHeader, lease)
	}
	response, err := c.http.Do(request)
	if err != nil {
		return fmt.Errorf("reach the murder agent: %w", err)
	}
	defer response.Body.Close()
	if err := responseError(response); err != nil {
		return err
	}
	switch out := out.(type) {
	case nil:
		return nil
	case *[]byte:
		*out, err = io.ReadAll(response.Body)
		return err
	case io.Writer:
		_, err = io.Copy(out, response.Body)
		return err
	default:
		return json.NewDecoder(response.Body).Decode(out)
	}
}

// Error is an API failure.
type Error struct {
	Status  int
	Message string
}

func (e *Error) Error() string { return e.Message }

// IsNotFound reports a missing tenant or resource.
func IsNotFound(err error) bool {
	var apiErr *Error
	return errors.As(err, &apiErr) && apiErr.Status == http.StatusNotFound
}

func tenantPath(name string) string { return "/v1/tenants/" + url.PathEscape(name) }

// List returns every tenant, archived ones too.
func (c *Client) List(ctx context.Context) ([]Tenant, error) {
	var tenants []Tenant
	return tenants, c.do(ctx, http.MethodGet, "/v1/tenants", nil, &tenants)
}

func (c *Client) Get(ctx context.Context, name string) (Tenant, error) {
	var tenant Tenant
	return tenant, c.do(ctx, http.MethodGet, tenantPath(name), nil, &tenant)
}

func appPath(name string) string { return "/v1/apps/" + url.PathEscape(name) }

func (c *Client) Apps(ctx context.Context) ([]App, error) {
	var apps []App
	return apps, c.do(ctx, http.MethodGet, "/v1/apps", nil, &apps)
}

func (c *Client) App(ctx context.Context, name string) (App, error) {
	var app App
	return app, c.do(ctx, http.MethodGet, appPath(name), nil, &app)
}

// PutApp creates or replaces an app and rolls it out to its tenants; the
// agent fills in defaults and pins the image.
func (c *Client) PutApp(ctx context.Context, name string, app spec.Tenant) (AppApplied, error) {
	var applied AppApplied
	return applied, c.do(ctx, http.MethodPut, c.rollout(appPath(name)), app, &applied)
}

// PutAppIf is PutApp only if the stored app is as cond expects; a 409
// otherwise, with nothing changed.
func (c *Client) PutAppIf(ctx context.Context, name string, app spec.Tenant, cond store.AppIf) (AppApplied, error) {
	path := appPath(name)
	switch {
	case cond.Absent:
		path += "?if=absent"
	case !cond.UpdatedAt.IsZero():
		path += "?if=" + url.QueryEscape(cond.UpdatedAt.UTC().Format(time.RFC3339Nano))
	}
	var applied AppApplied
	return applied, c.do(ctx, http.MethodPut, c.rollout(path), app, &applied)
}

// PutAppNewest is PutApp, pinning a tag to its newest image even when the
// app was given that tag before: PutApp keeps the image the app has.
func (c *Client) PutAppNewest(ctx context.Context, name string, app spec.Tenant) (AppApplied, error) {
	var applied AppApplied
	return applied, c.do(ctx, http.MethodPut, c.rollout(appPath(name)+"?update=true"), app, &applied)
}

// CheckApp asks the app's registry now which image its tag names.
func (c *Client) CheckApp(ctx context.Context, name string) (AppCheck, error) {
	var check AppCheck
	return check, c.do(ctx, http.MethodPost, appPath(name)+"/check", nil, &check)
}

// UpdateApp pins the app's tag to its newest image and rolls it out. With
// updatedAt set it is refused, with a 409, if the app changed since.
func (c *Client) UpdateApp(ctx context.Context, name string, updatedAt time.Time) (AppApplied, error) {
	path := appPath(name) + "/update"
	if !updatedAt.IsZero() {
		path += "?if=" + url.QueryEscape(updatedAt.UTC().Format(time.RFC3339Nano))
	}
	var applied AppApplied
	return applied, c.do(ctx, http.MethodPost, c.rollout(path), nil, &applied)
}

// AppRevisions lists the app's recent revisions, newest first.
func (c *Client) AppRevisions(ctx context.Context, name string) ([]store.Revision, error) {
	var revisions []store.Revision
	return revisions, c.do(ctx, http.MethodGet, appPath(name)+"/revisions", nil, &revisions)
}

// RestoreRevision stores one of the app's revisions again and rolls it
// out. With updatedAt set it is refused, with a 409, if the app changed
// since.
func (c *Client) RestoreRevision(ctx context.Context, name string, revision int64, updatedAt time.Time) (AppApplied, error) {
	path := appPath(name) + "/revisions/" + strconv.FormatInt(revision, 10) + "/restore"
	if !updatedAt.IsZero() {
		path += "?if=" + url.QueryEscape(updatedAt.UTC().Format(time.RFC3339Nano))
	}
	var applied AppApplied
	return applied, c.do(ctx, http.MethodPost, c.rollout(path), nil, &applied)
}

// AtOnce is the client, but an app change it makes moves every tenant at
// once, not in waves.
func (c *Client) AtOnce() *Client {
	copied := *c
	copied.atOnce = true
	return &copied
}

// First is the client, but an app change in waves it makes moves the
// tenants named at once, as its first wave, and the rest in waves.
func (c *Client) First(tenants []string) *Client {
	copied := *c
	copied.first = slices.Clone(tenants)
	return &copied
}

// rollout adds to an app change's path that it moves every tenant at once,
// or some first, when the client says so.
func (c *Client) rollout(path string) string {
	add := func(query string) {
		if strings.Contains(path, "?") {
			path += "&" + query
		} else {
			path += "?" + query
		}
	}
	if c.atOnce {
		add("rollout=now")
	}
	if len(c.first) > 0 {
		add("first=" + url.QueryEscape(strings.Join(c.first, ",")))
	}
	return path
}

// RolloutAction pauses, resumes, aborts or promotes an app's rollout.
func (c *Client) RolloutAction(ctx context.Context, name, action string) (App, error) {
	var app App
	return app, c.do(ctx, http.MethodPost, appPath(name)+"/rollout/"+url.PathEscape(action), nil, &app)
}

// RestoreRollout takes tenants of an app's rollout, those named or every
// one it moved, back to the revision before with their data from before
// they moved: what they wrote since is lost. The rollout pauses.
func (c *Client) RestoreRollout(ctx context.Context, name string, tenants []string) (RolloutRestored, error) {
	var restored RolloutRestored
	return restored, c.do(ctx, http.MethodPost, appPath(name)+"/rollout/restore", RolloutRestore{Tenants: tenants}, &restored)
}

// SetFlags makes a tenant a canary or held, or not.
func (c *Client) SetFlags(ctx context.Context, name string, flags TenantFlags) (Tenant, error) {
	var tenant Tenant
	return tenant, c.do(ctx, http.MethodPut, tenantPath(name)+"/flags", flags, &tenant)
}

// DeleteApp removes an app no tenant runs.
func (c *Client) DeleteApp(ctx context.Context, name string) error {
	return c.do(ctx, http.MethodDelete, appPath(name), nil, nil)
}

// Create adds a tenant of an app.
func (c *Client) Create(ctx context.Context, request TenantRequest) (Applied, error) {
	var applied Applied
	return applied, c.do(ctx, http.MethodPost, "/v1/tenants", request, &applied)
}

// Configure replaces a tenant's app and overrides; Applied.Changed is false
// if nothing changed.
func (c *Client) Configure(ctx context.Context, name string, request TenantRequest) (Applied, error) {
	var applied Applied
	return applied, c.do(ctx, http.MethodPut, tenantPath(name), request, &applied)
}

// Events reads the audit log, newest first.
func (c *Client) Events(ctx context.Context, query store.EventQuery) ([]store.Event, error) {
	values := url.Values{}
	if query.Tenant != "" {
		values.Set("tenant", query.Tenant)
	}
	if query.App != "" {
		values.Set("app", query.App)
	}
	if query.Before > 0 {
		values.Set("before", strconv.FormatInt(query.Before, 10))
	}
	if query.Limit > 0 {
		values.Set("limit", strconv.Itoa(query.Limit))
	}
	var events []store.Event
	return events, c.do(ctx, http.MethodGet, "/v1/events?"+values.Encode(), nil, &events)
}

func (c *Client) Suspend(ctx context.Context, name string, suspended bool) (Tenant, error) {
	action := "/resume"
	if suspended {
		action = "/suspend"
	}
	var tenant Tenant
	return tenant, c.do(ctx, http.MethodPost, tenantPath(name)+action, nil, &tenant)
}

// Archive takes a tenant out of service, keeping its data, secrets and overrides.
func (c *Client) Archive(ctx context.Context, name string) error {
	return c.do(ctx, http.MethodPost, tenantPath(name)+"/archive", nil, nil)
}

// Restore brings an archived tenant back on its app's current spec.
func (c *Client) Restore(ctx context.Context, name string) (Tenant, error) {
	var tenant Tenant
	return tenant, c.do(ctx, http.MethodPost, tenantPath(name)+"/restore", nil, &tenant)
}

// Purge deletes an archived tenant and its data for good.
func (c *Client) Purge(ctx context.Context, name string) error {
	return c.do(ctx, http.MethodDelete, tenantPath(name), nil, nil)
}

// Secret reads one of a tenant's secret values, recorded in the access
// register with proof.
func (c *Client) Secret(ctx context.Context, name, key string, proof access.Proof) (string, error) {
	var secret SecretValue
	err := c.do(ctx, http.MethodGet, withProof(tenantPath(name)+"/secrets/"+url.PathEscape(key), url.Values{}, proof), nil, &secret)
	return secret.Value, err
}

// withProof is path with query and a proof that the read is recorded.
func withProof(path string, query url.Values, proof access.Proof) string {
	proof.AddTo(query)
	if len(query) == 0 {
		return path
	}
	return path + "?" + query.Encode()
}

// RecordAccess records an access to a tenant's data in the register.
func (c *Client) RecordAccess(ctx context.Context, request access.Request) (access.Entry, error) {
	var entry access.Entry
	return entry, c.do(ctx, http.MethodPost, "/v1/access", request, &entry)
}

// Accesses lists the register, oldest first: a tenant's accesses and
// those of every tenant (all for ""), recorded since since (all for zero).
func (c *Client) Accesses(ctx context.Context, tenant string, since time.Time) ([]access.Entry, error) {
	values := url.Values{}
	if tenant != "" {
		values.Set("tenant", tenant)
	}
	if !since.IsZero() {
		values.Set("since", since.UTC().Format(time.RFC3339Nano))
	}
	path := "/v1/access"
	if len(values) > 0 {
		path += "?" + values.Encode()
	}
	var entries []access.Entry
	return entries, c.do(ctx, http.MethodGet, path, nil, &entries)
}

// Rotate gives one of a tenant's secrets a new value.
// Roll has a tenant's container replaced as it is.
func (c *Client) Roll(ctx context.Context, name string) (Tenant, error) {
	var tenant Tenant
	return tenant, c.do(ctx, http.MethodPost, tenantPath(name)+"/roll", nil, &tenant)
}

func (c *Client) Rotate(ctx context.Context, name, key string) (Tenant, error) {
	var tenant Tenant
	return tenant, c.do(ctx, http.MethodPost, tenantPath(name)+"/secrets/"+url.PathEscape(key)+"/rotate", nil, &tenant)
}

// Logs are the last lines of a tenant's container, recorded in the access
// register with proof.
func (c *Client) Logs(ctx context.Context, name string, tail int, proof access.Proof) ([]byte, error) {
	var output []byte
	return output, c.do(ctx, http.MethodGet, withProof(tenantPath(name)+"/logs", url.Values{"tail": {strconv.Itoa(tail)}}, proof), nil, &output)
}

// PreviousLogs are the last lines of a tenant's previous container; a 404
// when its node kept none.
func (c *Client) PreviousLogs(ctx context.Context, name string, proof access.Proof) (PreviousLogs, error) {
	var previous PreviousLogs
	return previous, c.do(ctx, http.MethodGet, withProof(tenantPath(name)+"/logs", url.Values{"previous": {"1"}}, proof), nil, &previous)
}

func (c *Client) Reconcile(ctx context.Context) error {
	return c.do(ctx, http.MethodPost, "/v1/reconcile", nil, nil)
}

// Backup writes a copy of the agent's database to w: every tenant's sealed
// secrets are in it, so it is recorded in the access register with proof.
func (c *Client) Backup(ctx context.Context, w io.Writer, proof access.Proof) error {
	return c.do(ctx, http.MethodGet, withProof("/v1/backup", url.Values{}, proof), nil, w)
}

// LocalBackup is a copy of the database the agent keeps on its node.
type LocalBackup struct {
	Path    string `json:"path"`
	Node    string `json:"node"`
	Tenants int    `json:"tenants"`
}

// ScheduledBackup has the agent keep a copy of its database in its node's
// backup directory, root's alone, and all but the newest keep removed (0
// keeps all): the system's own keeping, which no one is handed.
func (c *Client) ScheduledBackup(ctx context.Context, keep int) (LocalBackup, error) {
	var kept LocalBackup
	return kept, c.do(ctx, http.MethodPost, "/v1/backup/scheduled?keep="+strconv.Itoa(keep), nil, &kept)
}

// responseError is the agent's refusal, if it refused.
func responseError(response *http.Response) error {
	if response.StatusCode < 300 {
		return nil
	}
	var failure ErrorBody
	data, _ := io.ReadAll(io.LimitReader(response.Body, 1<<16))
	if json.Unmarshal(data, &failure) != nil || failure.Error == "" {
		failure.Error = string(bytes.TrimSpace(data))
	}
	return &Error{Status: response.StatusCode, Message: failure.Error}
}
