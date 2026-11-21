package plugins

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/netip"
	"slices"
	"strings"
)

// Config is what an edge needs to serve plugins: the plugins, and the
// tenants its node runs that may use them. The agent makes it, as part of
// the edge's routes.
type Config struct {
	Plugins map[string]Plugin `json:"plugins,omitempty"`
	Tenants map[string]Tenant `json:"tenants,omitempty"`
}

// Plugin is a plugin as the edge serves it: its spec, the operator's key
// sealed with the master key (see KeyContext), empty when it has none, and
// for a container, its manifest.
type Plugin struct {
	Spec     Spec      `json:"spec"`
	Key      string    `json:"key,omitempty"`
	Manifest *Manifest `json:"manifest,omitempty"`
}

// Tenant is a tenant this node runs: the address of its container, which
// its requests come from, and the plugins it may use.
type Tenant struct {
	Address string           `json:"address"`
	Grants  map[string]Grant `json:"grants"`
}

// Grant is a tenant's use of a plugin: its own key, sealed (see
// TenantKeyContext), if it has one, and the limits that hold for it.
type Grant struct {
	Key    string  `json:"key,omitempty"`
	Limits []Limit `json:"limits,omitempty"`
}

// KeyContext names the sealing context of a plugin's key. No tenant
// secret's context is like it: tenant names have no spaces.
func KeyContext(plugin string) string { return "plugin key " + plugin }

// TenantKeyContext names the sealing context of a tenant's own key for a
// plugin.
func TenantKeyContext(plugin, tenant string) string { return "plugin key " + plugin + " " + tenant }

// Version sums up the config, for the routes' version.
func (c Config) Version(w io.Writer) {
	for _, name := range slices.Sorted(maps.Keys(c.Plugins)) {
		plugin := c.Plugins[name]
		// The image, pinned, says its manifest too.
		config, _ := json.Marshal(plugin.Spec.Config)
		fmt.Fprintf(w, "plugin\x00%s\x00%s\x00%s\x00%s\x00%s\x00%s\x00%s\x00%s\x00", name, plugin.Spec.Kind, plugin.Spec.Upstream, strings.Join(plugin.Spec.Hosts, " "), strings.Join(plugin.Spec.Allow, " "), plugin.Key, plugin.Spec.Image, config)
		writeLimits(w, plugin.Spec.Limits)
	}
	for _, name := range slices.Sorted(maps.Keys(c.Tenants)) {
		tenant := c.Tenants[name]
		fmt.Fprintf(w, "plugin tenant\x00%s\x00%s\x00", name, tenant.Address)
		for _, plugin := range slices.Sorted(maps.Keys(tenant.Grants)) {
			grant := tenant.Grants[plugin]
			fmt.Fprintf(w, "grant\x00%s\x00%x\x00", plugin, sha256.Sum256([]byte(grant.Key)))
			writeLimits(w, grant.Limits)
		}
	}
}

func writeLimits(w io.Writer, limits []Limit) {
	for _, limit := range limits {
		fmt.Fprintf(w, "limit\x00%s\x00", limit)
	}
}

// Empty reports whether there is nothing to serve.
func (c Config) Empty() bool { return len(c.Plugins) == 0 && len(c.Tenants) == 0 }

// tenantAt finds the tenant whose container is at addr: its own fixed
// address, not its network's gateway, which is the host's.
func (c Config) tenantAt(addr netip.Addr) (string, Tenant, bool) {
	for name, tenant := range c.Tenants {
		if own, err := netip.ParseAddr(tenant.Address); err == nil && own == addr {
			return name, tenant, true
		}
	}
	return "", Tenant{}, false
}
