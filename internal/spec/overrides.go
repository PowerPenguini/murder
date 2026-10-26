package spec

import (
	"encoding/json"
	"maps"
)

// Overrides are what one tenant sets differently from its app. Everything
// else comes from the app, so a change to the app reaches every tenant.
type Overrides struct {
	// Image replaces the app's image, for example for a canary. Empty means
	// the app's image.
	Image string `json:"image,omitempty"`
	// Env adds to the app's env and replaces values of the same name.
	Env map[string]string `json:"env,omitempty"`
	// Secrets adds secrets to the app's.
	Secrets Secrets `json:"secrets,omitempty"`
	// Limits replace the app's limits field by field; zero keeps the app's.
	Limits Limits `json:"limits,omitzero"`
}

// Normalized returns o with secret defaults filled in and empty maps
// dropped, so equal overrides compare and store alike.
func (o Overrides) Normalized() Overrides {
	normalized := Overrides{Image: o.Image, Limits: o.Limits}
	if len(o.Env) > 0 {
		normalized.Env = maps.Clone(o.Env)
	}
	if len(o.Secrets) > 0 {
		normalized.Secrets = Tenant{Secrets: o.Secrets}.WithDefaults().Secrets
	}
	return normalized
}

// Equal reports whether two overrides are the same once normalized.
func (o Overrides) Equal(other Overrides) bool {
	a, _ := json.Marshal(o.Normalized())
	b, _ := json.Marshal(other.Normalized())
	return string(a) == string(b)
}

// Compose returns the spec tenant name runs: the app's spec with the
// tenant's overrides and every default filled in. It does not validate.
func Compose(app Tenant, name string, overrides Overrides) Tenant {
	tenant := app.WithDefaults()
	tenant.Name = name
	tenant.Suspended = false
	if overrides.Image != "" {
		tenant.Image = overrides.Image
	}
	if len(overrides.Env) > 0 {
		if tenant.Env == nil {
			tenant.Env = map[string]string{}
		}
		maps.Copy(tenant.Env, overrides.Env)
	}
	if len(overrides.Secrets) > 0 {
		if tenant.Secrets == nil {
			tenant.Secrets = Secrets{}
		}
		maps.Copy(tenant.Secrets, overrides.Secrets)
	}
	if overrides.Limits.MemoryMB != 0 {
		tenant.Limits.MemoryMB = overrides.Limits.MemoryMB
	}
	if overrides.Limits.CPUs != 0 {
		tenant.Limits.CPUs = overrides.Limits.CPUs
	}
	if overrides.Limits.PIDs != 0 {
		tenant.Limits.PIDs = overrides.Limits.PIDs
	}
	return tenant.WithDefaults()
}
