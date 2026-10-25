// Package config loads the agent's configuration file.
package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/PowerPenguini/corral/internal/edge"
	"github.com/PowerPenguini/corral/internal/spec"
)

// Agent configures one node's agent.
type Agent struct {
	// BaseDomain is the domain tenants live below: <tenant>.<BaseDomain>.
	BaseDomain    string `yaml:"baseDomain"`
	StateFile     string `yaml:"stateFile"`
	MasterKeyFile string `yaml:"masterKeyFile"`
	// Socket is the local API the CLI talks to. SocketGroup, if set, may use
	// it besides root.
	Socket       string  `yaml:"socket"`
	SocketGroup  string  `yaml:"socketGroup"`
	DockerSocket string  `yaml:"dockerSocket"`
	Volumes      Volumes `yaml:"volumes"`
	Ports        Ports   `yaml:"ports"`
	Edge         Edge    `yaml:"edge"`
	// Defaults fill in tenant limits left at zero.
	Defaults          spec.Limits `yaml:"defaults"`
	ReconcileInterval Duration    `yaml:"reconcileInterval"`
}

// Volumes selects the data volume driver.
type Volumes struct {
	// Driver is "dir" (Root/<tenant>) or "zfs" (Parent/<tenant> mounted at
	// MountRoot/<tenant>).
	Driver    string `yaml:"driver"`
	Root      string `yaml:"root"`
	Parent    string `yaml:"parent"`
	MountRoot string `yaml:"mountRoot"`
}

// Ports is the loopback port range tenant containers are published on.
type Ports struct {
	From int `yaml:"from"`
	To   int `yaml:"to"`
}

// Edge configures the public listeners.
type Edge struct {
	HTTP  string         `yaml:"http"`
	HTTPS string         `yaml:"https"`
	TLS   edge.TLSConfig `yaml:"tls"`
}

// Duration reads Go duration strings such as "10s".
type Duration struct{ time.Duration }

func (d *Duration) UnmarshalYAML(node *yaml.Node) error {
	parsed, err := time.ParseDuration(node.Value)
	if err != nil {
		return fmt.Errorf("duration %q: %w", node.Value, err)
	}
	d.Duration = parsed
	return nil
}

// Load reads the file at path and fills in defaults.
func Load(path string) (Agent, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Agent{}, err
	}
	return Parse(data)
}

// Parse reads a configuration and fills in defaults.
func Parse(data []byte) (Agent, error) {
	agent := Agent{
		StateFile:         "/var/lib/corral/state.json",
		MasterKeyFile:     "/etc/corral/master.key",
		Socket:            "/run/corral/agent.sock",
		DockerSocket:      "/var/run/docker.sock",
		Volumes:           Volumes{Driver: "dir", Root: "/srv/corral/tenants"},
		Ports:             Ports{From: 20000, To: 29999},
		Edge:              Edge{HTTP: ":80", HTTPS: ":443", TLS: edge.TLSConfig{Mode: edge.TLSOff, Storage: "/var/lib/corral/certs"}},
		Defaults:          spec.Limits{MemoryMB: 512, CPUs: 1, PIDs: 512},
		ReconcileInterval: Duration{10 * time.Second},
	}
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	// An empty document keeps every default; Validate then reports the
	// missing base domain.
	if err := decoder.Decode(&agent); err != nil && !errors.Is(err, io.EOF) {
		return Agent{}, fmt.Errorf("parse config: %w", err)
	}
	return agent, agent.Validate()
}

// Validate reports settings that cannot work.
func (a Agent) Validate() error {
	var problems []string
	if a.BaseDomain == "" || strings.HasPrefix(a.BaseDomain, ".") || strings.Contains(a.BaseDomain, "/") {
		problems = append(problems, "baseDomain must be a domain name")
	}
	switch a.Volumes.Driver {
	case "dir":
		if !strings.HasPrefix(a.Volumes.Root, "/") {
			problems = append(problems, "volumes.root must be an absolute path")
		}
	case "zfs":
		if a.Volumes.Parent == "" || !strings.HasPrefix(a.Volumes.MountRoot, "/") {
			problems = append(problems, "zfs volumes need parent and an absolute mountRoot")
		}
	default:
		problems = append(problems, fmt.Sprintf("unknown volume driver %q", a.Volumes.Driver))
	}
	if a.Ports.From < 1024 || a.Ports.To > 65535 || a.Ports.From > a.Ports.To {
		problems = append(problems, "ports must be a range within 1024-65535")
	}
	switch a.Edge.TLS.Mode {
	case edge.TLSOff, edge.TLSInternal, edge.TLSACME:
	default:
		problems = append(problems, fmt.Sprintf("unknown edge.tls.mode %q", a.Edge.TLS.Mode))
	}
	if a.Edge.TLS.Mode == edge.TLSACME && (a.Edge.TLS.Email == "" || a.Edge.TLS.DNSProvider == "" || a.Edge.TLS.DNSTokenFile == "") {
		problems = append(problems, "acme TLS needs email, dnsProvider and dnsTokenFile")
	}
	if a.ReconcileInterval.Duration < time.Second {
		problems = append(problems, "reconcileInterval must be at least 1s")
	}
	if len(problems) > 0 {
		return fmt.Errorf("invalid config: %s", strings.Join(problems, "; "))
	}
	return nil
}

// Scheme is the public URL scheme tenants are served with.
func (a Agent) Scheme() string {
	if a.Edge.TLS.Mode == edge.TLSOff {
		return "http"
	}
	return "https"
}
