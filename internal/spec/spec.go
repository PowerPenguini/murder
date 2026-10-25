// Package spec defines what a tenant runs and checks that a definition is
// complete and safe to hand to the container runtime.
package spec

import (
	"fmt"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// Tenant is the desired definition of one tenant: the app it runs and how.
// Corral treats the app as configuration; nothing here is app specific.
type Tenant struct {
	// Name is the tenant's DNS label: its subdomain and the suffix of its
	// container, network and volume names.
	Name string `json:"name" yaml:"name"`
	// Image is pinned by digest ("repo@sha256:…") or is a local image ID
	// ("sha256:…"), so every node runs the same bytes.
	Image string `json:"image" yaml:"image"`
	// Port is the HTTP port the app listens on inside the container.
	Port int `json:"port" yaml:"port"`
	// HealthPath is requested with GET to decide whether the tenant is up.
	HealthPath string `json:"healthPath" yaml:"healthPath"`
	// DataPath is where the tenant's data directory is mounted.
	DataPath string `json:"dataPath" yaml:"dataPath"`
	// DataUID and DataGID own the data directory, matching the app's user.
	DataUID int `json:"dataUID" yaml:"dataUID"`
	DataGID int `json:"dataGID" yaml:"dataGID"`
	// User is the numeric "uid:gid" the app runs as. It is required, so an
	// image that defaults to root never runs as root, and neither id may be 0.
	User string `json:"user" yaml:"user"`
	// Env holds plain environment values. Values may use the placeholders
	// {{name}}, {{host}}, {{url}} and {{network}}; see Render.
	Env map[string]string `json:"env,omitempty" yaml:"env,omitempty"`
	// Secrets names environment variables whose values Corral generates once
	// and stores encrypted.
	Secrets map[string]Secret `json:"secrets,omitempty" yaml:"secrets,omitempty"`
	Limits  Limits            `json:"limits" yaml:"limits"`
	// ReadOnlyRoot mounts the image read-only; Tmpfs adds writable scratch
	// mounts (path → mount options).
	ReadOnlyRoot bool              `json:"readOnlyRoot,omitempty" yaml:"readOnlyRoot,omitempty"`
	Tmpfs        map[string]string `json:"tmpfs,omitempty" yaml:"tmpfs,omitempty"`
	// Suspended keeps the data but runs no container.
	Suspended bool `json:"suspended,omitempty" yaml:"suspended,omitempty"`
}

// Secret describes how a secret value is generated.
type Secret struct {
	// Kind is "hex" (Bytes random bytes, hex encoded), "base64" (Bytes random
	// bytes, standard base64) or "password" (Bytes characters from an
	// unambiguous alphabet).
	Kind  string `json:"kind" yaml:"kind"`
	Bytes int    `json:"bytes" yaml:"bytes"`
}

// Limits caps a tenant's resources. Zero means the agent's default.
type Limits struct {
	MemoryMB int     `json:"memoryMB,omitempty" yaml:"memoryMB,omitempty"`
	CPUs     float64 `json:"cpus,omitempty" yaml:"cpus,omitempty"`
	PIDs     int     `json:"pids,omitempty" yaml:"pids,omitempty"`
}

const maxNameLength = 40

var (
	namePattern = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]*[a-z0-9])?$`)
	envPattern  = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
	userPattern = regexp.MustCompile(`^(0|[1-9][0-9]{0,9}):(0|[1-9][0-9]{0,9})$`)
	maxID       = int64(1)<<31 - 1
	digestRef   = regexp.MustCompile(`^[a-z0-9]+(?:[._/:-][a-z0-9]+)*@sha256:[a-f0-9]{64}$`)
	imageID     = regexp.MustCompile(`^sha256:[a-f0-9]{64}$`)
	placeholder = regexp.MustCompile(`\{\{[^}]*\}\}`)
	knownHolder = map[string]bool{"{{name}}": true, "{{host}}": true, "{{url}}": true, "{{network}}": true}
	reserved    = map[string]bool{"www": true, "api": true, "admin": true, "corral": true, "mail": true}
	secretKinds = map[string]bool{"hex": true, "base64": true, "password": true}
)

// ValidLabel reports whether name has the shape of a tenant name: safe as a
// DNS label and as part of container, network, path and dataset names.
func ValidLabel(name string) error {
	if len(name) == 0 || len(name) > maxNameLength || !namePattern.MatchString(name) {
		return fmt.Errorf("tenant name %q must be 1-%d lowercase letters, digits or inner hyphens", name, maxNameLength)
	}
	return nil
}

// ValidName reports whether name can be given to a new tenant: a valid label
// that is not reserved for the platform's own host names.
func ValidName(name string) error {
	if err := ValidLabel(name); err != nil {
		return err
	}
	if reserved[name] {
		return fmt.Errorf("tenant name %q is reserved", name)
	}
	return nil
}

// ValidImage reports whether image is pinned to exact content.
func ValidImage(image string) error {
	if digestRef.MatchString(image) || imageID.MatchString(image) {
		return nil
	}
	return fmt.Errorf("image %q must be pinned: repository@sha256:<digest> or a local sha256:<id>", image)
}

// Validate checks t and returns every problem found.
func (t Tenant) Validate() error {
	var problems []string
	add := func(err error) {
		if err != nil {
			problems = append(problems, err.Error())
		}
	}
	add(ValidName(t.Name))
	add(ValidImage(t.Image))
	if t.Port < 1 || t.Port > 65535 {
		problems = append(problems, fmt.Sprintf("port %d is out of range", t.Port))
	}
	if !strings.HasPrefix(t.HealthPath, "/") {
		problems = append(problems, fmt.Sprintf("health path %q must start with /", t.HealthPath))
	}
	if !absoluteClean(t.DataPath) || t.DataPath == "/" {
		problems = append(problems, fmt.Sprintf("data path %q must be an absolute path below /", t.DataPath))
	}
	// Tenants never run as root: root in the container owns the data
	// directory and is a step from root on the host.
	if t.DataUID <= 0 || t.DataGID <= 0 || int64(t.DataUID) > maxID || int64(t.DataGID) > maxID {
		problems = append(problems, "data owner ids must be positive (tenants never run as root)")
	}
	if err := checkUser(t.User); err != nil {
		problems = append(problems, err.Error())
	}
	for key, value := range t.Env {
		if !envPattern.MatchString(key) {
			problems = append(problems, fmt.Sprintf("env name %q is invalid", key))
		}
		for _, holder := range placeholder.FindAllString(value, -1) {
			if !knownHolder[holder] {
				problems = append(problems, fmt.Sprintf("env %s uses unknown placeholder %s", key, holder))
			}
		}
	}
	for key, secret := range t.Secrets {
		if !envPattern.MatchString(key) {
			problems = append(problems, fmt.Sprintf("secret name %q is invalid", key))
		}
		if _, clash := t.Env[key]; clash {
			problems = append(problems, fmt.Sprintf("%s is both env and secret", key))
		}
		if !secretKinds[secret.Kind] {
			problems = append(problems, fmt.Sprintf("secret %s has unknown kind %q", key, secret.Kind))
		}
		if secret.Bytes < 16 || secret.Bytes > 128 {
			problems = append(problems, fmt.Sprintf("secret %s needs 16-128 bytes, got %d", key, secret.Bytes))
		}
	}
	for mount := range t.Tmpfs {
		if !absoluteClean(mount) || mount == "/" || mount == t.DataPath {
			problems = append(problems, fmt.Sprintf("tmpfs path %q is invalid", mount))
		}
	}
	if t.Limits.MemoryMB < 0 || t.Limits.CPUs < 0 || t.Limits.PIDs < 0 {
		problems = append(problems, "limits must not be negative")
	}
	if len(problems) == 0 {
		return nil
	}
	sort.Strings(problems)
	return &ValidationError{Problems: problems}
}

// ValidationError lists everything wrong with a tenant spec.
type ValidationError struct {
	Problems []string
}

func (e *ValidationError) Error() string {
	return "invalid tenant: " + strings.Join(e.Problems, "; ")
}

// checkUser accepts only "uid:gid" in plain decimal, both above zero. Names
// and other spellings are refused: the engine parses "00" or "+0" as root.
func checkUser(user string) error {
	if !userPattern.MatchString(user) {
		return fmt.Errorf("user %q must be a numeric uid:gid such as 10001:10001", user)
	}
	uid, gid, _ := strings.Cut(user, ":")
	if uid == "0" || gid == "0" {
		return fmt.Errorf("user %q is root; tenants never run as root", user)
	}
	for _, id := range []string{uid, gid} {
		if value, err := strconv.ParseInt(id, 10, 64); err != nil || value > maxID {
			return fmt.Errorf("user %q: ids must be at most %d", user, maxID)
		}
	}
	return nil
}

func absoluteClean(p string) bool {
	return strings.HasPrefix(p, "/") && path.Clean(p) == p
}

// Values are the facts substituted into env placeholders.
type Values struct {
	// Host is the tenant's public host name, URL its public base URL and
	// Network the CIDR of its container network (the edge proxy connects from
	// it, so an app that trusts proxy headers should trust this range).
	Host, URL, Network string
}

// Render returns the tenant's plain environment with placeholders replaced.
func (t Tenant) Render(values Values) map[string]string {
	replacer := strings.NewReplacer("{{name}}", t.Name, "{{host}}", values.Host, "{{url}}", values.URL, "{{network}}", values.Network)
	rendered := make(map[string]string, len(t.Env))
	for key, value := range t.Env {
		rendered[key] = replacer.Replace(value)
	}
	return rendered
}
