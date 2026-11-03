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
	"strconv"
	"time"

	"github.com/PowerPenguini/murder/internal/spec"
	"github.com/PowerPenguini/murder/internal/store"
)

// Client calls the agent's API over its unix socket.
type Client struct {
	http *http.Client
	base string
	// Actor, if set, names the person the client acts for in the audit log.
	Actor string
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
	return applied, c.do(ctx, http.MethodPut, appPath(name), app, &applied)
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
	return applied, c.do(ctx, http.MethodPut, path, app, &applied)
}

// PutAppNewest is PutApp, pinning a tag to its newest image even when the
// app was given that tag before: PutApp keeps the image the app has.
func (c *Client) PutAppNewest(ctx context.Context, name string, app spec.Tenant) (AppApplied, error) {
	var applied AppApplied
	return applied, c.do(ctx, http.MethodPut, appPath(name)+"?update=true", app, &applied)
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
	return applied, c.do(ctx, http.MethodPost, path, nil, &applied)
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
	return applied, c.do(ctx, http.MethodPost, path, nil, &applied)
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

func (c *Client) Secret(ctx context.Context, name, key string) (string, error) {
	var secret SecretValue
	err := c.do(ctx, http.MethodGet, tenantPath(name)+"/secrets/"+url.PathEscape(key), nil, &secret)
	return secret.Value, err
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

func (c *Client) Logs(ctx context.Context, name string, tail int) ([]byte, error) {
	var output []byte
	return output, c.do(ctx, http.MethodGet, tenantPath(name)+"/logs?tail="+strconv.Itoa(tail), nil, &output)
}

func (c *Client) Reconcile(ctx context.Context) error {
	return c.do(ctx, http.MethodPost, "/v1/reconcile", nil, nil)
}

// Backup writes a copy of the agent's database to w.
func (c *Client) Backup(ctx context.Context, w io.Writer) error {
	return c.do(ctx, http.MethodGet, "/v1/backup", nil, w)
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
