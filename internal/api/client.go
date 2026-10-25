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
)

// Client calls the agent's API over its unix socket.
type Client struct {
	http *http.Client
	base string
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

// Create adds a tenant. adoptData lets it take over data a deleted tenant
// of the same name left on the node.
func (c *Client) Create(ctx context.Context, tenant spec.Tenant, adoptData bool) (Tenant, error) {
	var created Tenant
	return created, c.do(ctx, http.MethodPost, "/v1/tenants?adoptData="+strconv.FormatBool(adoptData), tenant, &created)
}

func (c *Client) Replace(ctx context.Context, tenant spec.Tenant) (Tenant, error) {
	var replaced Tenant
	return replaced, c.do(ctx, http.MethodPut, tenantPath(tenant.Name), tenant, &replaced)
}

func (c *Client) SetImage(ctx context.Context, name, image string) (Tenant, error) {
	var tenant Tenant
	return tenant, c.do(ctx, http.MethodPost, tenantPath(name)+"/image", ImageChange{Image: image}, &tenant)
}

func (c *Client) Suspend(ctx context.Context, name string, suspended bool) (Tenant, error) {
	action := "/resume"
	if suspended {
		action = "/suspend"
	}
	var tenant Tenant
	return tenant, c.do(ctx, http.MethodPost, tenantPath(name)+action, nil, &tenant)
}

func (c *Client) Delete(ctx context.Context, name string, purge bool) error {
	return c.do(ctx, http.MethodDelete, tenantPath(name)+"?purge="+strconv.FormatBool(purge), nil, nil)
}

// PurgeData deletes data a deleted tenant left behind.
func (c *Client) PurgeData(ctx context.Context, name string) error {
	return c.do(ctx, http.MethodDelete, "/v1/data/"+url.PathEscape(name), nil, nil)
}

func (c *Client) Secret(ctx context.Context, name, key string) (string, error) {
	var secret SecretValue
	err := c.do(ctx, http.MethodGet, tenantPath(name)+"/secrets/"+url.PathEscape(key), nil, &secret)
	return secret.Value, err
}

func (c *Client) Logs(ctx context.Context, name string, tail int) ([]byte, error) {
	var output []byte
	return output, c.do(ctx, http.MethodGet, tenantPath(name)+"/logs?tail="+strconv.Itoa(tail), nil, &output)
}

func (c *Client) Reconcile(ctx context.Context) error {
	return c.do(ctx, http.MethodPost, "/v1/reconcile", nil, nil)
}
