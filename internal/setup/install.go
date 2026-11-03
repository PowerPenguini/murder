package setup

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"path"
	"slices"
	"strings"
	"text/template"

	"github.com/PowerPenguini/murder/deploy"
)

// Config is the agent's config for the settings: only what differs from
// the defaults, with a word on each.
func Config(s Settings) []byte {
	var out bytes.Buffer
	if err := configTemplate.Execute(&out, s); err != nil {
		panic(err) // the template and its data are fixed
	}
	return out.Bytes()
}

var configTemplate = template.Must(template.New("agent.yaml").Funcs(template.FuncMap{
	"list": func(items []string) string { return "[" + strings.Join(items, ", ") + "]" },
}).Parse(`# /etc/murder/agent.yaml, written by "murder setup"; everything not here
# has a default (see deploy/agent.example.yaml for every setting).
node: {{.Node}}
baseDomain: {{.Domain}}  # tenants live at <tenant>.{{.Domain}}
{{- if .ZFSPool}}

volumes:
  driver: zfs                      # a dataset per tenant, with a disk limit
  parent: {{.ZFSPool}}/murder/tenants
{{- else}}

volumes:
  driver: dir                      # plain directories: no disk limits, no copies on other nodes
{{- end}}

edge:
  tls:
    mode: {{.TLS}}
{{- if eq .TLS "acme"}}
    email: {{.Email}}
    dnsProvider: {{.DNSProvider}}
    dnsTokenFile: {{if eq .DNSProvider "hetzner"}}` + HetznerTokenFile + `{{else}}` + DNSTokenFile + `{{end}}
{{- end}}
{{- if .ClusterAddress}}

cluster:
  address: {{.ClusterAddress}}  # this node on the private network
{{- if .Public}}
  publicAddresses: {{list .Public}}
{{- end}}
{{- if .Failover}}
  failover:                        # power a dead node off, run its tenants from their copies
    hetzner:
      tokenFile: ` + HetznerTokenFile + `
{{- end}}
{{- if .ManageDNS}}
  dns:                             # the leader keeps *.{{.Domain}} on the nodes that serve
    provider: {{.DNSProvider}}
    tokenFile: {{if eq .DNSProvider "hetzner"}}` + HetznerTokenFile + `{{else}}` + DNSTokenFile + `{{end}}
{{- end}}
{{- end}}
{{- if .Panel}}

panel:
  tunnel:
    address: {{.TunnelAddress}}  # this node in the panel's WireGuard tunnel
{{- end}}
`))

// Step is one thing setup does.
type Step struct {
	Say string
	Do  func(ctx context.Context) error
}

// Install is what setup puts on a host.
type Install struct {
	// Config is the agent's config.
	Config []byte
	// Secrets are files only root reads, such as tokens and the master
	// key, by path.
	Secrets map[string][]byte
	// ZFSPool holds the tenants' datasets; empty for plain directories.
	ZFSPool string
	// Panel starts the panel too.
	Panel bool
	// LiveRestore turns on Docker's live-restore.
	LiveRestore bool
}

// Install is what setup installs for the settings.
func (s Settings) Install() Install {
	in := Install{Config: Config(s), Secrets: map[string][]byte{}, ZFSPool: s.ZFSPool, Panel: s.Panel}
	if s.HetznerToken != "" {
		in.Secrets[HetznerTokenFile] = []byte(s.HetznerToken + "\n")
	}
	if s.DNSToken != "" {
		file := DNSTokenFile
		if s.DNSProvider == "hetzner" {
			file = HetznerTokenFile
		}
		in.Secrets[file] = []byte(s.DNSToken + "\n")
	}
	return in
}

// liveRestore sets "live-restore" in Docker's config, keeping the rest,
// and has Docker read it again: a reload, which restarts no container.
func liveRestore(ctx context.Context, host Host) error {
	settings := map[string]any{}
	if data, err := host.ReadFile(DaemonConfig); err == nil {
		if err := json.Unmarshal(data, &settings); err != nil {
			return fmt.Errorf("%s is not JSON setup can change (%v): add \"live-restore\": true to it by hand, then `systemctl reload docker`", DaemonConfig, err)
		}
		if err := host.WriteFile(DaemonConfig+".before-murder", data, 0o644); err != nil {
			return err
		}
	} else if !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	settings["live-restore"] = true
	data, err := json.MarshalIndent(settings, "", "  ")
	if err != nil {
		return err
	}
	if err := host.MkdirAll(path.Dir(DaemonConfig), 0o755); err != nil {
		return err
	}
	if err := host.WriteFile(DaemonConfig, append(data, '\n'), 0o644); err != nil {
		return err
	}
	if _, err := host.Run(ctx, "systemctl", "reload", "docker"); err != nil {
		return fmt.Errorf("have Docker read %s again: %w", DaemonConfig, err)
	}
	return nil
}

// Units are the services setup enables and starts, in order.
func (in Install) Units() []string {
	units := []string{"murder-firewall.service", "murder-agent.service", "murder-edge.service", "murder-backup.timer"}
	if in.Panel {
		units = append(units, "murder-panel.service")
	}
	return units
}

// Steps are what setup does to install in, in order. initialize makes
// the master key and the database, as "murder init" does; it keeps a
// master key in.Secrets.
func Steps(host Host, in Install, force bool, initialize func() error) ([]Step, error) {
	executable, err := host.Executable()
	if err != nil {
		return nil, err
	}
	var steps []Step
	add := func(say string, do func(ctx context.Context) error) { steps = append(steps, Step{say, do}) }
	if executable != Binary {
		add("install "+executable+" as "+Binary, func(context.Context) error {
			data, err := host.ReadFile(executable)
			if err != nil {
				return err
			}
			return host.WriteFile(Binary, data, 0o755)
		})
	}
	files := "install the systemd units and the sysctl settings"
	if in.ZFSPool != "" {
		files += ", and have Docker start after ZFS has mounted"
	}
	add(files, func(ctx context.Context) error {
		files, err := fs.ReadDir(deploy.Files, ".")
		if err != nil {
			return err
		}
		for _, file := range files {
			name := file.Name()
			target := path.Join(UnitDir, name)
			switch {
			case name == "murder-sysctl.conf":
				target = SysctlFile
			case name == "docker-after-zfs.conf":
				if in.ZFSPool == "" {
					continue
				}
				target = DockerDropIn
			}
			data, err := deploy.Files.ReadFile(name)
			if err != nil {
				return err
			}
			if err := host.MkdirAll(path.Dir(target), 0o755); err != nil {
				return err
			}
			if err := host.WriteFile(target, data, 0o644); err != nil {
				return err
			}
		}
		if _, err := host.Run(ctx, "sysctl", "-q", "-p", SysctlFile); err != nil {
			return fmt.Errorf("apply %s: %w", SysctlFile, err)
		}
		_, err = host.Run(ctx, "systemctl", "daemon-reload")
		return err
	})
	if in.LiveRestore {
		add("have Docker keep containers running while it restarts (live-restore)", func(ctx context.Context) error {
			return liveRestore(ctx, host)
		})
	}
	if in.ZFSPool != "" {
		parent := in.ZFSPool + "/murder/tenants"
		add("create the ZFS dataset "+parent, func(ctx context.Context) error {
			if _, err := host.Run(ctx, "zfs", "list", "-H", "-o", "name", parent); err == nil {
				return nil
			}
			_, err := host.Run(ctx, "zfs", "create", "-p", parent)
			return err
		})
	}
	add("write "+ConfigFile+" and the files it names", func(context.Context) error {
		if host.Exists(ConfigFile) && !force {
			return fmt.Errorf("%s exists: this node is set up already (edit it, or run setup again with -force to write it anew)", ConfigFile)
		}
		if err := host.MkdirAll(ConfigDir, 0o755); err != nil {
			return err
		}
		for _, file := range slices.Sorted(maps.Keys(in.Secrets)) {
			if err := host.MkdirAll(path.Dir(file), 0o700); err != nil {
				return err
			}
			if err := host.WriteFile(file, in.Secrets[file], 0o600); err != nil {
				return err
			}
		}
		return host.WriteFile(ConfigFile, in.Config, 0o644)
	})
	add("make the master key and the database", func(context.Context) error { return initialize() })
	add("start "+strings.Join(in.Units(), ", "), func(ctx context.Context) error {
		if _, err := host.Run(ctx, "systemctl", append([]string{"enable", "--now"}, in.Units()...)...); err != nil {
			return err
		}
		if !force {
			return nil
		}
		// Services that ran already take the new binary and config: the
		// edge hands its listeners over, the agent leaves tenants running.
		if _, err := host.Run(ctx, "systemctl", "try-reload-or-restart", "murder-edge.service"); err != nil {
			return err
		}
		var others []string
		for _, unit := range in.Units() {
			if strings.HasSuffix(unit, ".service") && unit != "murder-edge.service" {
				others = append(others, unit)
			}
		}
		_, err := host.Run(ctx, "systemctl", append([]string{"try-restart"}, others...)...)
		return err
	})
	return steps, nil
}
