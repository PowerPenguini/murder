// Package fleet reads a fleet file: the apps a node runs and the tenants of
// each, with the few things a tenant may set differently.
//
//	apps:
//	  addhoc: apps/addhoc.yaml          # an app spec file, relative to this file
//	  demo: {image: nginx:1.27, user: "101:101"}   # or an inline app spec
//	tenants:
//	  acme: {app: addhoc}
//	  beta: {app: addhoc, env: {TZ: Europe/Berlin}, limits: {memoryMB: 768}}
package fleet

import (
	"bytes"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/PowerPenguini/corral/internal/spec"
)

// File is a parsed fleet file.
type File struct {
	Apps    map[string]App    `yaml:"apps"`
	Tenants map[string]Tenant `yaml:"tenants"`
}

// App is an app spec, given inline or as the path of a spec file.
type App struct {
	Path   string
	Inline *spec.Tenant
}

func (a *App) UnmarshalYAML(node *yaml.Node) error {
	switch node.Kind {
	case yaml.ScalarNode:
		a.Path = node.Value
		return nil
	case yaml.MappingNode:
		var inline spec.Tenant
		if err := decodeStrict(node, &inline); err != nil {
			return err
		}
		a.Inline = &inline
		return nil
	default:
		return errors.New("an app is a spec file path or an inline spec")
	}
}

// Tenant is one tenant entry: its app and what it overrides.
type Tenant struct {
	App string `yaml:"app"`
	// Image replaces the app's image, for example for a canary.
	Image string `yaml:"image"`
	// Env and Secrets add to (and Env replaces values of) the app's.
	Env     map[string]string `yaml:"env"`
	Secrets spec.Secrets      `yaml:"secrets"`
	// Limits replace the app's limits field by field.
	Limits spec.Limits `yaml:"limits"`
}

// Load reads the fleet file at path and returns one spec per tenant, sorted
// by name. App spec paths are relative to the fleet file.
func Load(path string) ([]spec.Tenant, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var file File
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(&file); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	apps := map[string]spec.Tenant{}
	for name, app := range file.Apps {
		loaded, err := app.load(filepath.Dir(path))
		if err != nil {
			return nil, fmt.Errorf("app %s: %w", name, err)
		}
		apps[name] = loaded
	}
	var problems []string
	tenants := make([]spec.Tenant, 0, len(file.Tenants))
	for _, name := range slices.Sorted(maps.Keys(file.Tenants)) {
		tenant, err := compose(name, file.Tenants[name], apps)
		if err != nil {
			problems = append(problems, err.Error())
			continue
		}
		tenants = append(tenants, tenant)
	}
	if len(problems) > 0 {
		return nil, fmt.Errorf("%s: %s", path, strings.Join(problems, "; "))
	}
	return tenants, nil
}

func (a App) load(dir string) (spec.Tenant, error) {
	var app spec.Tenant
	if a.Inline != nil {
		app = *a.Inline
	} else {
		path := a.Path
		if !filepath.IsAbs(path) {
			path = filepath.Join(dir, path)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return spec.Tenant{}, err
		}
		decoder := yaml.NewDecoder(bytes.NewReader(data))
		decoder.KnownFields(true)
		if err := decoder.Decode(&app); err != nil {
			return spec.Tenant{}, fmt.Errorf("parse %s: %w", path, err)
		}
	}
	if app.Name != "" {
		return spec.Tenant{}, errors.New("an app spec names no tenant; tenants are listed in the fleet file")
	}
	return app, nil
}

func compose(name string, entry Tenant, apps map[string]spec.Tenant) (spec.Tenant, error) {
	if err := spec.ValidName(name); err != nil {
		return spec.Tenant{}, err
	}
	app, ok := apps[entry.App]
	if !ok {
		return spec.Tenant{}, fmt.Errorf("tenant %s runs unknown app %q", name, entry.App)
	}
	tenant := app
	tenant.Name = name
	if entry.Image != "" {
		tenant.Image = entry.Image
	}
	tenant.Env = maps.Clone(app.Env)
	if len(entry.Env) > 0 {
		if tenant.Env == nil {
			tenant.Env = map[string]string{}
		}
		maps.Copy(tenant.Env, entry.Env)
	}
	tenant.Secrets = maps.Clone(app.Secrets)
	if len(entry.Secrets) > 0 {
		if tenant.Secrets == nil {
			tenant.Secrets = spec.Secrets{}
		}
		maps.Copy(tenant.Secrets, entry.Secrets)
	}
	tenant.Tmpfs = maps.Clone(app.Tmpfs)
	if entry.Limits.MemoryMB != 0 {
		tenant.Limits.MemoryMB = entry.Limits.MemoryMB
	}
	if entry.Limits.CPUs != 0 {
		tenant.Limits.CPUs = entry.Limits.CPUs
	}
	if entry.Limits.PIDs != 0 {
		tenant.Limits.PIDs = entry.Limits.PIDs
	}
	return tenant, nil
}

// decodeStrict decodes node into out, refusing unknown fields as the file
// decoder does.
func decodeStrict(node *yaml.Node, out any) error {
	encoded, err := yaml.Marshal(node)
	if err != nil {
		return err
	}
	decoder := yaml.NewDecoder(bytes.NewReader(encoded))
	decoder.KnownFields(true)
	return decoder.Decode(out)
}
