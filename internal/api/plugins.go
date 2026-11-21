package api

import (
	"context"
	"net/http"
	"net/url"
	"time"

	"github.com/PowerPenguini/murder/internal/plugins"
)

// Plugin is a plugin installed in the cluster.
type Plugin struct {
	Name string       `json:"name"`
	Spec plugins.Spec `json:"spec"`
	// Manifest is what a plugin of kind container says of itself: its
	// image's label.
	Manifest *plugins.Manifest `json:"manifest,omitempty"`
	// HasKey says whether it has the operator's key; the key itself is
	// never shown.
	HasKey bool `json:"hasKey"`
	// Apps are the apps that list it, whose tenants may use it.
	Apps      []string  `json:"apps,omitempty"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}

// PluginRequest installs a plugin or changes its spec. The image of a
// plugin of kind container may be a tag: the agent pins it.
type PluginRequest struct {
	Spec plugins.Spec `json:"spec"`
}

// PluginChange is what installing a plugin did; Note says how its image
// was pinned, when it was not to a registry's digest.
type PluginChange struct {
	Plugin  Plugin `json:"plugin"`
	Changed bool   `json:"changed"`
	Note    string `json:"note,omitempty"`
}

// PluginKeyRequest gives a plugin the operator's key, or with Tenant a
// tenant's own; an empty Key removes it.
type PluginKeyRequest struct {
	Key    string `json:"key"`
	Tenant string `json:"tenant,omitempty"`
}

// PluginLimitsRequest gives a tenant limits of its own (none at all for an
// empty list), or with Set false the plugin's again.
type PluginLimitsRequest struct {
	Limits []plugins.Limit `json:"limits"`
	Set    bool            `json:"set"`
}

// PluginUse is a tenant's use of a plugin: whether it uses a key of its
// own, and how far it is into each limit that holds for it.
type PluginUse struct {
	Tenant string `json:"tenant"`
	Plugin string `json:"plugin"`
	// Granted says whether the tenant's app lists the plugin.
	Granted bool `json:"granted"`
	// OwnKey says whether the tenant has a key of its own; OwnLimits
	// whether its limits are its own rather than the plugin's.
	OwnKey    bool         `json:"ownKey,omitempty"`
	OwnLimits bool         `json:"ownLimits,omitempty"`
	Limits    []LimitUse   `json:"limits,omitempty"`
	Today     []MetricUsed `json:"today,omitempty"`
	Month     []MetricUsed `json:"month,omitempty"`
}

// LimitUse is a limit and what was used of it in its period now.
type LimitUse struct {
	plugins.Limit
	Period string `json:"period"`
	Used   int64  `json:"used"`
	// Reached says the tenant used it all: a hard limit refuses its
	// requests.
	Reached bool `json:"reached,omitempty"`
}

// MetricUsed is an amount of a metric used.
type MetricUsed struct {
	Metric string `json:"metric"`
	Amount int64  `json:"amount"`
}

// PluginUsage is what tenants used of plugins from day From to day To,
// a day, metric and detail at a time.
type PluginUsage struct {
	From   string          `json:"from"`
	To     string          `json:"to"`
	Counts []plugins.Count `json:"counts"`
}

func pluginPath(name string) string { return "/v1/plugins/" + url.PathEscape(name) }

// Plugins lists the plugins installed.
func (c *Client) Plugins(ctx context.Context) ([]Plugin, error) {
	var list []Plugin
	return list, c.do(ctx, http.MethodGet, "/v1/plugins", nil, &list)
}

// Plugin returns one plugin.
func (c *Client) Plugin(ctx context.Context, name string) (Plugin, error) {
	var plugin Plugin
	return plugin, c.do(ctx, http.MethodGet, pluginPath(name), nil, &plugin)
}

// PutPlugin installs a plugin or changes its spec.
func (c *Client) PutPlugin(ctx context.Context, name string, spec plugins.Spec) (PluginChange, error) {
	var change PluginChange
	return change, c.do(ctx, http.MethodPut, pluginPath(name), PluginRequest{Spec: spec}, &change)
}

// DeletePlugin removes a plugin no app lists.
func (c *Client) DeletePlugin(ctx context.Context, name string) error {
	return c.do(ctx, http.MethodDelete, pluginPath(name), nil, nil)
}

// SetPluginKey gives a plugin a key: the operator's, or a tenant's own;
// an empty key removes it.
func (c *Client) SetPluginKey(ctx context.Context, name, tenant, key string) error {
	return c.do(ctx, http.MethodPut, pluginPath(name)+"/key", PluginKeyRequest{Key: key, Tenant: tenant}, nil)
}

// SetPluginLimits gives a tenant limits of a plugin of its own, or with
// set false the plugin's again.
func (c *Client) SetPluginLimits(ctx context.Context, name, tenant string, limits []plugins.Limit, set bool) error {
	return c.do(ctx, http.MethodPut, pluginPath(name)+"/tenants/"+url.PathEscape(tenant)+"/limits", PluginLimitsRequest{Limits: limits, Set: set}, nil)
}

// TenantPlugins is a tenant's use of the plugins its app lists or it has
// settings of.
func (c *Client) TenantPlugins(ctx context.Context, tenant string) ([]PluginUse, error) {
	var uses []PluginUse
	return uses, c.do(ctx, http.MethodGet, tenantPath(tenant)+"/plugins", nil, &uses)
}

// PluginUsage is what tenants used of plugins from day from to day to
// (2006-01-02, both included): a tenant's or all, a plugin's or all.
func (c *Client) PluginUsage(ctx context.Context, tenant, plugin, from, to string) (PluginUsage, error) {
	query := url.Values{"from": {from}, "to": {to}}
	if tenant != "" {
		query.Set("tenant", tenant)
	}
	if plugin != "" {
		query.Set("plugin", plugin)
	}
	var usage PluginUsage
	return usage, c.do(ctx, http.MethodGet, "/v1/plugin-usage?"+query.Encode(), nil, &usage)
}

// PluginUses returns every live tenant's use of plugins.
func (c *Client) PluginUses(ctx context.Context) ([]PluginUse, error) {
	var uses []PluginUse
	return uses, c.do(ctx, http.MethodGet, "/v1/plugin-uses", nil, &uses)
}

// EdgePluginUsage gives the agent a report of what the edge's tenants
// used of plugins, and returns what they used, cluster wide. A report
// without counts (Seq 0) only asks for that.
func (c *Client) EdgePluginUsage(ctx context.Context, report plugins.Report) (plugins.Totals, error) {
	var totals plugins.Totals
	return totals, c.do(ctx, http.MethodPost, "/v1/edge/plugin-usage", report, &totals)
}
