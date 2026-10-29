// Package spec defines what a tenant runs and checks that a definition is
// complete and safe to hand to the container runtime.
package spec

import (
	"encoding/json"
	"fmt"
	"maps"
	"path"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// Tenant is the desired definition of one tenant: the app it runs and how.
// Murder treats the app as configuration; nothing here is app specific.
// WithDefaults fills in everything an app spec may leave out.
type Tenant struct {
	// Name is the tenant's DNS label: its subdomain and the suffix of its
	// container, network and volume names.
	Name string `json:"name" yaml:"name"`
	// Image may be given as a tag ("repo:tag"); the agent pins it to a digest
	// ("repo@sha256:…") or a local image ID ("sha256:…") before storing it,
	// so every node runs the same bytes.
	Image string `json:"image" yaml:"image"`
	// Port is the HTTP port the app listens on inside the container (8080).
	Port int `json:"port,omitempty" yaml:"port,omitempty"`
	// HealthPath is requested with GET to decide whether the tenant is up ("/").
	HealthPath string `json:"healthPath,omitempty" yaml:"healthPath,omitempty"`
	// DataPath is where the tenant's data directory is mounted ("/data").
	DataPath string `json:"dataPath,omitempty" yaml:"dataPath,omitempty"`
	// User is the numeric "uid:gid" the app runs as; it also owns the data
	// directory. It is required, so an image that defaults to root never runs
	// as root, and neither id may be 0.
	User string `json:"user" yaml:"user"`
	// Env holds plain environment values. Values may use the placeholders
	// {{name}}, {{host}}, {{url}} and {{network}}; see Render.
	Env map[string]string `json:"env,omitempty" yaml:"env,omitempty"`
	// Secrets names environment variables whose values Murder generates once
	// and stores encrypted. In YAML a list of names gives 32-byte hex values;
	// a mapping may give a kind ("password") or a full {kind, bytes}.
	Secrets Secrets `json:"secrets,omitempty" yaml:"secrets,omitempty"`
	Limits  Limits  `json:"limits,omitzero" yaml:"limits,omitempty"`
	// The image's filesystem is read-only unless WritableRoot is set; Tmpfs
	// adds writable scratch mounts (path → mount options), by default /tmp.
	WritableRoot bool              `json:"writableRoot,omitempty" yaml:"writableRoot,omitempty"`
	Tmpfs        map[string]string `json:"tmpfs" yaml:"tmpfs,omitempty"`
	// Suspended keeps the data but runs no container. It is operational
	// state, set by suspend and resume, never by an app spec.
	Suspended bool `json:"suspended,omitempty" yaml:"-"`
}

// Secret describes how a secret value is generated.
type Secret struct {
	// Kind is "hex" (Bytes random bytes, hex encoded), "base64" (Bytes random
	// bytes, standard base64) or "password" (Bytes characters from an
	// unambiguous alphabet).
	Kind  string `json:"kind" yaml:"kind"`
	Bytes int    `json:"bytes,omitempty" yaml:"bytes,omitempty"`
}

// Secrets maps environment variable names to how their values are made.
type Secrets map[string]Secret

// UnmarshalYAML accepts a list of names, or a mapping of names to a kind or
// to a full Secret.
func (s *Secrets) UnmarshalYAML(node *yaml.Node) error {
	secrets := Secrets{}
	switch node.Kind {
	case yaml.SequenceNode:
		var names []string
		if err := node.Decode(&names); err != nil {
			return err
		}
		for _, name := range names {
			if _, duplicate := secrets[name]; duplicate {
				return fmt.Errorf("secret %s is listed twice", name)
			}
			secrets[name] = Secret{Kind: "hex"}
		}
	case yaml.MappingNode:
		for i := 0; i+1 < len(node.Content); i += 2 {
			name, value := node.Content[i].Value, node.Content[i+1]
			if _, duplicate := secrets[name]; duplicate {
				return fmt.Errorf("secret %s is listed twice", name)
			}
			for value.Kind == yaml.AliasNode {
				value = value.Alias
			}
			var secret Secret
			if value.Kind == yaml.ScalarNode {
				secret.Kind = value.Value
			} else if err := decodeSecret(value, &secret); err != nil {
				return fmt.Errorf("secret %s: %w", name, err)
			}
			secrets[name] = secret
		}
	default:
		return fmt.Errorf("secrets must be a list of names or a mapping")
	}
	*s = secrets
	return nil
}

// decodeSecret decodes the full {kind, bytes} form, refusing unknown fields:
// Node.Decode does not inherit the file decoder's strictness. yaml.v3 itself
// refuses an alias that contains itself and excessive aliasing.
func decodeSecret(node *yaml.Node, secret *Secret) error {
	var strict struct {
		Secret  `yaml:",inline"`
		Unknown map[string]yaml.Node `yaml:",inline"`
	}
	if err := node.Decode(&strict); err != nil {
		return err
	}
	if len(strict.Unknown) > 0 {
		field := slices.Min(slices.Collect(maps.Keys(strict.Unknown)))
		return fmt.Errorf("line %d: unknown field %q", strict.Unknown[field].Line, field)
	}
	*secret = strict.Secret
	return nil
}

// Limits caps a tenant's resources. Zero means the agent's default.
type Limits struct {
	MemoryMB int     `json:"memoryMB,omitempty" yaml:"memoryMB,omitempty"`
	CPUs     float64 `json:"cpus,omitempty" yaml:"cpus,omitempty"`
	PIDs     int     `json:"pids,omitempty" yaml:"pids,omitempty"`
	// DiskGB caps the tenant's data, not counting snapshots. Only the zfs
	// volume driver enforces it.
	DiskGB int `json:"diskGB,omitempty" yaml:"diskGB,omitempty"`
}

// Or returns l with every field left at zero taken from defaults.
func (l Limits) Or(defaults Limits) Limits {
	if l.MemoryMB == 0 {
		l.MemoryMB = defaults.MemoryMB
	}
	if l.CPUs == 0 {
		l.CPUs = defaults.CPUs
	}
	if l.PIDs == 0 {
		l.PIDs = defaults.PIDs
	}
	if l.DiskGB == 0 {
		l.DiskGB = defaults.DiskGB
	}
	return l
}

// Upper bounds of limits, far above any server, so the byte counts made of
// them cannot overflow.
const (
	maxMemoryMB = 1 << 24 // 16 TB
	maxCPUs     = 4096
	maxPIDs     = 1 << 22
	maxDiskGB   = 1 << 20 // 1 PB
)

// Defaults applied by WithDefaults.
const (
	DefaultPort       = 8080
	DefaultHealthPath = "/"
	DefaultDataPath   = "/data"
	DefaultTmpfs      = "rw,noexec,nosuid,nodev,size=256m"
)

var defaultSecretBytes = map[string]int{"hex": 32, "base64": 32, "password": 20}

// WithDefaults returns a copy of t with everything left out filled in. A nil
// Tmpfs gets /tmp; an empty one stays empty.
func (t Tenant) WithDefaults() Tenant {
	if t.Port == 0 {
		t.Port = DefaultPort
	}
	if t.HealthPath == "" {
		t.HealthPath = DefaultHealthPath
	}
	if t.DataPath == "" {
		t.DataPath = DefaultDataPath
	}
	if t.Tmpfs == nil {
		t.Tmpfs = map[string]string{"/tmp": DefaultTmpfs}
	} else {
		t.Tmpfs = maps.Clone(t.Tmpfs)
	}
	t.Env = maps.Clone(t.Env)
	if t.Secrets != nil {
		secrets := make(Secrets, len(t.Secrets))
		for name, secret := range t.Secrets {
			if secret.Kind == "" {
				secret.Kind = "hex"
			}
			if secret.Bytes == 0 {
				secret.Bytes = defaultSecretBytes[secret.Kind]
			}
			secrets[name] = secret
		}
		t.Secrets = secrets
	}
	return t
}

// Owner returns the uid and gid of a valid User.
func (t Tenant) Owner() (uid, gid int) {
	uidText, gidText, _ := strings.Cut(t.User, ":")
	uid, _ = strconv.Atoi(uidText)
	gid, _ = strconv.Atoi(gidText)
	return uid, gid
}

const maxNameLength = 40

// repositoryName follows Docker's reference grammar: lowercase path
// components joined by "/", optionally after a registry host. A first
// component is a host only if it has a dot or a port, or is localhost.
const repositoryName = `(?:` + registryHost + `/)?` + pathComponent + `(?:/` + pathComponent + `)*`

const (
	imageTag      = `:[A-Za-z0-9_][A-Za-z0-9._-]{0,127}`
	pathComponent = `[a-z0-9]+(?:(?:[._]|__|-+)[a-z0-9]+)*`
	hostComponent = `(?:[a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9-]*[a-zA-Z0-9])`
	registryHost  = `(?:(?:` + hostComponent + `(?:\.` + hostComponent + `)+|localhost)(?::[0-9]+)?|` + hostComponent + `:[0-9]+)`
)

var (
	namePattern = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]*[a-z0-9])?$`)
	envPattern  = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
	userPattern = regexp.MustCompile(`^(0|[1-9][0-9]{0,9}):(0|[1-9][0-9]{0,9})$`)
	maxID       = int64(1)<<31 - 1
	digestRef   = regexp.MustCompile(`^` + repositoryName + `(?:` + imageTag + `)?@sha256:[a-f0-9]{64}$`)
	imageID     = regexp.MustCompile(`^sha256:[a-f0-9]{64}$`)
	tagRef      = regexp.MustCompile(`^` + repositoryName + `(?:` + imageTag + `)?$`)
	placeholder = regexp.MustCompile(`\{\{[^}]*\}\}`)
	knownHolder = map[string]bool{"{{name}}": true, "{{host}}": true, "{{url}}": true, "{{network}}": true}
	reserved    = map[string]bool{"www": true, "api": true, "admin": true, "murder": true, "mail": true}
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

// Pinned reports whether image names exact content.
func Pinned(image string) bool {
	return digestRef.MatchString(image) || imageID.MatchString(image)
}

// ValidImage reports whether image is pinned to exact content.
func ValidImage(image string) error {
	if Pinned(image) {
		return nil
	}
	return fmt.Errorf("image %q must be pinned: repository@sha256:<digest> or a local sha256:<id>", image)
}

// ValidReference reports whether image is pinned or a tag reference the
// agent can pin.
func ValidReference(image string) error {
	if Pinned(image) || tagRef.MatchString(image) {
		return nil
	}
	return fmt.Errorf("image %q is not a valid image reference", image)
}

// Validate checks a tenant with defaults applied and its image pinned, and
// returns every problem found.
func (t Tenant) Validate() error { return t.validate(ValidImage) }

// ValidateUnpinned is Validate for a spec whose image may still be a tag:
// everything that can be checked before the image is pinned.
func (t Tenant) ValidateUnpinned() error { return t.validate(ValidReference) }

func (t Tenant) validate(checkImage func(string) error) error {
	var problems []string
	add := func(err error) {
		if err != nil {
			problems = append(problems, err.Error())
		}
	}
	add(ValidName(t.Name))
	add(checkImage(t.Image))
	if t.Port < 1 || t.Port > 65535 {
		problems = append(problems, fmt.Sprintf("port %d is out of range", t.Port))
	}
	if !strings.HasPrefix(t.HealthPath, "/") {
		problems = append(problems, fmt.Sprintf("health path %q must start with /", t.HealthPath))
	}
	if !absoluteClean(t.DataPath) || t.DataPath == "/" {
		problems = append(problems, fmt.Sprintf("data path %q must be an absolute path below /", t.DataPath))
	}
	add(checkUser(t.User))
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
		if _, known := defaultSecretBytes[secret.Kind]; !known {
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
	if t.Limits.MemoryMB < 0 || t.Limits.CPUs < 0 || t.Limits.PIDs < 0 || t.Limits.DiskGB < 0 {
		problems = append(problems, "limits must not be negative")
	}
	if t.Limits.MemoryMB > maxMemoryMB || t.Limits.CPUs > maxCPUs || t.Limits.PIDs > maxPIDs || t.Limits.DiskGB > maxDiskGB {
		problems = append(problems, fmt.Sprintf("limits must be at most %d MB memory, %d CPUs, %d processes and %d GB disk", maxMemoryMB, maxCPUs, maxPIDs, maxDiskGB))
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

// Equal reports whether two tenants define the same thing.
func (t Tenant) Equal(other Tenant) bool {
	a, _ := json.Marshal(t)
	b, _ := json.Marshal(other)
	return string(a) == string(b)
}
