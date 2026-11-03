package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"text/tabwriter"
	"time"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/cluster"
	"github.com/PowerPenguini/murder/internal/config"
	"github.com/PowerPenguini/murder/internal/rotation"
	"github.com/PowerPenguini/murder/internal/secrets"
	"github.com/PowerPenguini/murder/internal/store"
)

// keyRing is this node's master key ring: its file, and the key every
// part of the agent holds, read again whenever the file changes.
type keyRing struct {
	path   string
	key    *secrets.Key
	member func() *cluster.Cluster
	log    *slog.Logger
	// mu keeps changes, reads of the file and what follows them one at a
	// time: a read from before a change never wins over the change.
	mu sync.Mutex
}

func (k *keyRing) Ring() []secrets.Entry { return k.key.Ring() }

func (k *keyRing) Add(raw []byte) error {
	return k.change(func() error { return secrets.AddNext(k.path, raw) })
}

func (k *keyRing) Use(id string) error {
	return k.change(func() error { return secrets.Use(k.path, id) })
}

func (k *keyRing) Drop(id string) error {
	return k.change(func() error { return secrets.Drop(k.path, id) })
}

func (k *keyRing) change(edit func() error) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	if err := edit(); err != nil {
		return err
	}
	return k.reloadLocked()
}

// reload reads the file again, as the agent does every few seconds.
func (k *keyRing) reload() error {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.reloadLocked()
}

func (k *keyRing) reloadLocked() error {
	changed, err := k.key.Reload()
	if err != nil {
		return err
	}
	if changed {
		k.log.Info("master key ring changed", "keys", k.key.Ring())
	}
	return k.rekey()
}

// rekey has the cluster port present and trust what the ring makes; the
// same ring again changes nothing.
func (k *keyRing) rekey() error {
	if member := k.member(); member != nil {
		if err := member.Rekey(k.key.DeriveAll(clusterSeed)); err != nil {
			k.log.Error("follow the master key ring", "error", err)
			return err
		}
	}
	return nil
}

// keyRotator runs rotations from this node.
type keyRotator struct {
	ring   *keyRing
	store  *store.SQLite
	member func() *cluster.Cluster
	node   string
	log    *slog.Logger
	mu     sync.Mutex
	// settle overrides how long a step waits for the edges; tests only.
	settle time.Duration
}

// localRing and remoteRing are a node's ring as rotations see it.
type localRing struct{ ring *keyRing }

func (l localRing) Ring(context.Context) ([]secrets.Entry, error) { return l.ring.Ring(), nil }
func (l localRing) Add(_ context.Context, raw []byte) error       { return l.ring.Add(raw) }
func (l localRing) Use(_ context.Context, id string) error        { return l.ring.Use(id) }
func (l localRing) Drop(_ context.Context, id string) error       { return l.ring.Drop(id) }

type remoteRing struct {
	member *cluster.Cluster
	node   string
}

func (r remoteRing) Ring(ctx context.Context) ([]secrets.Entry, error) {
	return r.member.KeyRingOn(ctx, r.node)
}

func (r remoteRing) Add(ctx context.Context, raw []byte) error {
	_, err := r.member.AddKeyOn(ctx, r.node, raw)
	return err
}

func (r remoteRing) Use(ctx context.Context, id string) error {
	_, err := r.member.UseKeyOn(ctx, r.node, id)
	return err
}

func (r remoteRing) Drop(ctx context.Context, id string) error {
	_, err := r.member.DropKeyOn(ctx, r.node, id)
	return err
}

func (k *keyRotator) nodes() []string {
	if k.member() == nil {
		return []string{k.node}
	}
	var names []string
	for _, node := range k.store.Nodes() {
		names = append(names, node.Name)
	}
	if len(names) == 0 {
		return []string{k.node}
	}
	return names
}

func (k *keyRotator) on(node string) rotation.Ring {
	if node == k.node {
		return localRing{k.ring}
	}
	return remoteRing{member: k.member(), node: node}
}

func (k *keyRotator) Rings(ctx context.Context) []api.NodeKeys {
	var out []api.NodeKeys
	for _, node := range k.nodes() {
		ask, cancel := context.WithTimeout(ctx, 5*time.Second)
		ring, err := k.on(node).Ring(ask)
		cancel()
		entry := api.NodeKeys{Node: node, Keys: ring}
		if err != nil {
			entry.Error = err.Error()
		}
		out = append(out, entry)
	}
	return out
}

// rotationFreeze begins the reason of a rotation's freeze.
const rotationFreeze = "master key rotation from node "

func (k *keyRotator) Rotate(ctx context.Context, actor string) (rotation.Result, error) {
	if !k.mu.TryLock() {
		return rotation.Result{}, errors.New("a rotation runs on this node already")
	}
	defer k.mu.Unlock()
	// The cluster takes no change meanwhile: no second rotation from
	// another node, no node joining, no secret sealed with a key on its
	// way out. A rotation that stopped on this node takes its own freeze
	// over.
	reason := rotationFreeze + k.node
	if err := k.store.Freeze(reason, actor); err != nil {
		return rotation.Result{}, fmt.Errorf("%w (a rotation from another node runs there to its end; `murder upgrade -thaw` ends a freeze no one ends)", err)
	}
	defer func() {
		if err := k.store.Thaw(actor); err != nil {
			k.log.Warn("take changes again after the rotation", "error", err)
		}
	}()
	key := k.ring.key
	r := &rotation.Rotation{
		Nodes:   k.nodes(),
		Members: k.nodes,
		Settle:  k.settle,
		On:      k.on,
		Raw:     key.Raw,
		Reseal: func(target string) (int, error) {
			if current := key.Current(); current != target {
				return 0, fmt.Errorf("this node uses key %s, not %s", current, target)
			}
			return k.store.ResealSecrets(func(tenant, name, sealed string) (string, bool, error) {
				return key.Reseal(secrets.Context(tenant, name), sealed)
			}, actor)
		},
		Report: func(step string) { k.log.Info("master key rotation", "step", step) },
	}
	result, err := r.Run(ctx)
	detail := fmt.Sprintf("key %s", result.Key)
	if len(result.Dropped) > 0 {
		detail += fmt.Sprintf(", dropped %s", strings.Join(result.Dropped, ", "))
	}
	if err != nil {
		detail += ": stopped: " + err.Error()
	}
	if result.Key != "" {
		if noteErr := k.store.Note("master-key.rotated", detail, actor); noteErr != nil {
			k.log.Warn("note the rotation in the audit log", "error", noteErr)
		}
	}
	return result, err
}

const keyUsage = `Usage: murder key <command> (as root)

  status    every node's master key ring: the key in use, older and next ones
  rotate    rotate the master key of every node, without stopping one

A rotation makes a new key, has every node trust it, then use it (its
cluster certificates and every new secret), seals every secret again with
it, and takes the old key out of every node. Every node must answer. A
rotation stopped part way goes on where it was when run again, from the
node that made the key. Copy the new master key file to your password
manager afterwards; backups of the database made before need the old key.
`

func runKey(args []string) error {
	if len(args) == 0 || args[0] == "-h" || args[0] == "help" {
		fmt.Fprint(os.Stderr, keyUsage)
		return flag.ErrHelp
	}
	flags := flag.NewFlagSet("key "+args[0], flag.ContinueOnError)
	socket := socketPath(flags)
	configPath := flags.String("config", defaultConfig, "agent configuration file, to name the master key file")
	asJSON := flags.Bool("json", false, "print JSON")
	rest, err := parseInterspersed(flags, args[1:])
	if err != nil {
		return err
	}
	if len(rest) != 0 {
		fmt.Fprint(os.Stderr, keyUsage)
		return fmt.Errorf("unknown key arguments: %v", rest)
	}
	client := api.NewClient(*socket)
	switch args[0] {
	case "status":
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		rings, err := client.KeyRings(ctx)
		if err != nil {
			return err
		}
		if *asJSON {
			return printJSON(rings)
		}
		table := tabwriter.NewWriter(stdout, 0, 4, 2, ' ', 0)
		fmt.Fprintln(table, "NODE\tIN USE\tOLDER\tNEXT")
		for _, ring := range rings {
			if ring.Error != "" {
				fmt.Fprintf(table, "%s\t%s\t\t\n", ring.Node, ring.Error)
				continue
			}
			var current, older, next []string
			for _, entry := range ring.Keys {
				switch entry.State {
				case secrets.StateCurrent:
					current = append(current, entry.ID)
				case secrets.StateNext:
					next = append(next, entry.ID)
				default:
					older = append(older, entry.ID)
				}
			}
			fmt.Fprintf(table, "%s\t%s\t%s\t%s\n", ring.Node, strings.Join(current, " "), dash(older), dash(next))
		}
		return table.Flush()
	case "rotate":
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
		defer cancel()
		result, err := client.RotateKey(ctx)
		for _, step := range result.Steps {
			fmt.Fprintln(stdout, step)
		}
		if err != nil {
			return err
		}
		if *asJSON {
			return printJSON(result)
		}
		path := "the master key file"
		if cfg, err := config.Load(*configPath); err == nil {
			path = cfg.MasterKeyFile
		}
		fmt.Fprintf(stdout, "every node uses master key %s now: copy %s to your password manager, in place of the old key\n", result.Key, path)
		if len(result.Dropped) > 0 {
			fmt.Fprintf(stdout, "keep the old key (%s) only for backups of the database made before\n", strings.Join(result.Dropped, ", "))
		}
		return nil
	default:
		fmt.Fprint(os.Stderr, keyUsage)
		return fmt.Errorf("unknown key command %q", args[0])
	}
}

func dash(ids []string) string {
	if len(ids) == 0 {
		return "-"
	}
	return strings.Join(ids, " ")
}
