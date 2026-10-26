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

	"github.com/PowerPenguini/corral/internal/spec"
	"github.com/PowerPenguini/corral/internal/store"
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
	return &Client{http: &http.Client{Transport: transport}, base: "http://corral"}
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
		return fmt.Errorf("reach the corral agent: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode >= 300 {
		var failure errorBody
		data, _ := io.ReadAll(io.LimitReader(response.Body, 1<<16))
		if json.Unmarshal(data, &failure) != nil || failure.Error == "" {
			failure.Error = string(bytes.TrimSpace(data))
		}
		return &Error{Status: response.StatusCode, Message: failure.Error}
	}
	switch out := out.(type) {
	case nil:
		return nil
	case *[]byte:
		*out, err = io.ReadAll(response.Body)
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

// Archive takes a tenant out of service, keeping its data, secrets and spec.
func (c *Client) Archive(ctx context.Context, name string) error {
	return c.do(ctx, http.MethodPost, tenantPath(name)+"/archive", nil, nil)
}

func (c *Client) ListArchived(ctx context.Context) ([]ArchivedTenant, error) {
	var archived []ArchivedTenant
	return archived, c.do(ctx, http.MethodGet, "/v1/archive", nil, &archived)
}

// Restore brings an archived tenant back as it was.
func (c *Client) Restore(ctx context.Context, name string) (Tenant, error) {
	var tenant Tenant
	return tenant, c.do(ctx, http.MethodPost, "/v1/archive/"+url.PathEscape(name)+"/restore", nil, &tenant)
}

// Purge deletes an archived tenant and its data for good.
func (c *Client) Purge(ctx context.Context, name string) error {
	return c.do(ctx, http.MethodDelete, "/v1/archive/"+url.PathEscape(name), nil, nil)
}

func (c *Client) Secret(ctx context.Context, name, key string) (string, error) {
	var secret SecretValue
	err := c.do(ctx, http.MethodGet, tenantPath(name)+"/secrets/"+url.PathEscape(key), nil, &secret)
	return secret.Value, err
}

// Rotate gives one of a tenant's secrets a new value.
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
