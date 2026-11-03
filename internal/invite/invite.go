// Package invite keeps a node's invitations: one-time secrets, valid for a
// while, with which a host gets what it needs to join the cluster as a
// node like this one: the master key, this node's config and the files it
// names.
package invite

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/netip"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sync"
	"time"

	"github.com/PowerPenguini/murder/internal/cluster"
	"github.com/PowerPenguini/murder/internal/config"
	"github.com/PowerPenguini/murder/internal/store"
)

// Store is what invitations need of the store.
type Store interface {
	Cluster() string
	Nodes() []store.Node
	Note(action, detail, actor string) error
	// Frozen says why the cluster takes no changes now, if it does not.
	Frozen() string
}

// Book is a node's invitations, kept in memory: a restarted agent forgets
// them, and they are made again.
type Book struct {
	// ConfigPath is this node's agent config, which a host gets.
	ConfigPath string
	Store      Store
	// Invitation makes an invitation to this node for a secret; nil until
	// the node runs its part of a cluster.
	Invitation func(secret []byte) cluster.Invite
	Now        func() time.Time

	mu      sync.Mutex
	pending map[[sha256.Size]byte]time.Time
	// handed are tunnel addresses given to hosts that have not joined
	// yet, with when.
	handed map[netip.Addr]time.Time
}

// MaxValid bounds how long an invitation is valid.
const MaxValid = 24 * time.Hour

func (b *Book) now() time.Time {
	if b.Now != nil {
		return b.Now()
	}
	return time.Now()
}

// Create makes an invitation, valid for valid.
func (b *Book) Create(valid time.Duration, actor string) (cluster.Invite, time.Time, error) {
	if b.Invitation == nil || b.Store.Cluster() == "" {
		return cluster.Invite{}, time.Time{}, errors.New("this node is not part of a cluster: start one with `murder cluster bootstrap`")
	}
	if valid <= 0 || valid > MaxValid {
		return cluster.Invite{}, time.Time{}, fmt.Errorf("an invitation is valid for up to %s", MaxValid)
	}
	if frozen := b.Store.Frozen(); frozen != "" {
		return cluster.Invite{}, time.Time{}, fmt.Errorf("the cluster takes no new node now (%s): invite it once that ends", frozen)
	}
	// A host would refuse what this node cannot send whole: say so now,
	// not once the invitation is spent.
	b.mu.Lock()
	_, err := b.bundle()
	b.mu.Unlock()
	if err != nil {
		return cluster.Invite{}, time.Time{}, fmt.Errorf("this node cannot invite: %w", err)
	}
	secret, err := cluster.InviteSecret()
	if err != nil {
		return cluster.Invite{}, time.Time{}, err
	}
	expires := b.now().Add(valid).UTC()
	b.mu.Lock()
	if b.pending == nil {
		b.pending = map[[sha256.Size]byte]time.Time{}
	}
	b.forgetExpired()
	b.pending[sha256.Sum256(secret)] = expires
	b.mu.Unlock()
	_ = b.Store.Note("cluster.invited", "a host may join until "+expires.Format(time.RFC3339), actor)
	return b.Invitation(secret), expires, nil
}

// forgetExpired drops invitations past their time; the caller holds mu.
func (b *Book) forgetExpired() {
	now := b.now()
	for hash, expires := range b.pending {
		if now.After(expires) {
			delete(b.pending, hash)
		}
	}
	for address, at := range b.handed {
		if now.Sub(at) > MaxValid {
			delete(b.handed, address)
		}
	}
}

var nodePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9.-]{0,62}$`)

// Redeem takes an invitation's secret once.
func (b *Book) Redeem(_ context.Context, secret []byte, node string) (cluster.Bundle, error) {
	hash := sha256.Sum256(secret)
	b.mu.Lock()
	defer b.mu.Unlock()
	b.forgetExpired()
	if _, ok := b.pending[hash]; !ok {
		return cluster.Bundle{}, cluster.ErrInvite
	}
	if !nodePattern.MatchString(node) {
		return cluster.Bundle{}, fmt.Errorf("node name %q: letters, digits, dots and hyphens", node)
	}
	// During a master key rotation, the key ring sent now would be out of
	// date by the time the host joins.
	if frozen := b.Store.Frozen(); frozen != "" {
		return cluster.Bundle{}, fmt.Errorf("the cluster takes no new node now (%s): try again once that ends", frozen)
	}
	for _, member := range b.Store.Nodes() {
		if member.Name == node {
			return cluster.Bundle{}, fmt.Errorf("a node named %s is in the cluster already: give this one another name with -node", node)
		}
	}
	bundle, err := b.bundle()
	if err != nil {
		return cluster.Bundle{}, err
	}
	delete(b.pending, hash)
	if bundle.TunnelAddress != "" {
		if b.handed == nil {
			b.handed = map[netip.Addr]time.Time{}
		}
		b.handed[netip.MustParseAddr(bundle.TunnelAddress)] = b.now()
	}
	_ = b.Store.Note("cluster.invitation-used", "by "+node, "murder")
	return bundle, nil
}

// bundle is what a host gets: the master key, this node's config and
// the files it names, and an address in the panel's tunnel.
func (b *Book) bundle() (cluster.Bundle, error) {
	data, err := os.ReadFile(b.ConfigPath)
	if err != nil {
		return cluster.Bundle{}, err
	}
	agent, err := config.Parse(data)
	if err != nil {
		return cluster.Bundle{}, err
	}
	dir := filepath.Dir(b.ConfigPath)
	if filepath.Dir(agent.MasterKeyFile) != dir {
		return cluster.Bundle{}, fmt.Errorf("the master key %s is not in %s: a host would not know where to keep it", agent.MasterKeyFile, dir)
	}
	key, err := os.ReadFile(agent.MasterKeyFile)
	if err != nil {
		return cluster.Bundle{}, fmt.Errorf("read the master key: %w", err)
	}
	bundle := cluster.Bundle{Cluster: b.Store.Cluster(), Key: key, Config: data, Files: map[string][]byte{}}
	for _, path := range Shared(agent) {
		// Only files beside the config: a host writes them where this
		// node keeps them.
		if filepath.Dir(path) != dir {
			return cluster.Bundle{}, fmt.Errorf("%s is not in %s: a host would not know where to keep it", path, dir)
		}
		file, err := os.ReadFile(path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return cluster.Bundle{}, err
		}
		bundle.Files[path] = file
	}
	if tunnel := agent.Panel.Tunnel; tunnel != nil {
		address, err := b.freeTunnelAddress(tunnel.Network, tunnel.Address)
		if err != nil {
			return cluster.Bundle{}, err
		}
		bundle.TunnelAddress = address.String()
	}
	return bundle, nil
}

// Shared are the files a config names that every node of a cluster
// keeps alike: tokens and keys, not the node's own tunnel key.
func Shared(agent config.Agent) []string {
	var paths []string
	add := func(path string) {
		if path != "" && !slices.Contains(paths, path) {
			paths = append(paths, path)
		}
	}
	add(agent.Edge.TLS.DNSTokenFile)
	add(agent.RegistryAuthFile)
	for _, registry := range agent.Registries {
		add(registry.TokenFile)
	}
	if backups := agent.Backups; backups != nil {
		add(backups.AccessKeyFile)
		add(backups.SecretKeyFile)
		add(backups.IdentityFile)
	}
	if c := agent.Cluster; c != nil {
		if c.Failover != nil && c.Failover.Hetzner != nil {
			add(c.Failover.Hetzner.TokenFile)
		}
		if c.DNS != nil {
			add(c.DNS.TokenFile)
		}
	}
	return paths
}

// freeTunnelAddress is an address of network below devices' that no node
// uses, nor was given to a host; the caller holds mu. It is one of the free
// ones at random: another node inviting at the same moment, which cannot
// see what this one handed out, rarely picks the same.
func (b *Book) freeTunnelAddress(network, own string) (netip.Addr, error) {
	prefix, err := netip.ParsePrefix(network)
	if err != nil {
		return netip.Addr{}, err
	}
	used := map[netip.Addr]bool{}
	if address, err := netip.ParseAddr(own); err == nil {
		used[address] = true
	}
	for _, node := range b.Store.Nodes() {
		if node.Tunnel != nil {
			used[node.Tunnel.Address] = true
		}
	}
	for address := range b.handed {
		used[address] = true
	}
	var free []netip.Addr
	address := prefix.Masked().Addr().Next()
	for i := 1; i < 100 && prefix.Contains(address); i++ {
		if !used[address] {
			free = append(free, address)
		}
		address = address.Next()
	}
	if len(free) == 0 {
		return netip.Addr{}, fmt.Errorf("no address left for a node in the panel's tunnel %s", network)
	}
	return free[rand.IntN(len(free))], nil
}
