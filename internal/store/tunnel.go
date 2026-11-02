package store

import (
	"encoding/base64"
	"errors"
	"fmt"
	"maps"
	"net/netip"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
)

// parseTunnel reads a node's tunnel column; nil when it has none.
func parseTunnel(value string) *Tunnel {
	fields := strings.Fields(value)
	if len(fields) != 3 {
		return nil
	}
	address, err := netip.ParseAddr(fields[0])
	port, portErr := strconv.Atoi(fields[2])
	if err != nil || portErr != nil {
		return nil
	}
	return &Tunnel{Address: address, Key: fields[1], Port: port}
}

// validKey reports whether key is a WireGuard public key: 32 bytes in
// standard base64.
func validKey(key string) bool {
	raw, err := base64.StdEncoding.DecodeString(key)
	return err == nil && len(raw) == 32
}

var deviceName = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,30}[a-z0-9])?$`)

func (s *SQLite) SetTunnel(node string, tunnel Tunnel) error {
	if !tunnel.Address.IsValid() || !validKey(tunnel.Key) || tunnel.Port < 1 || tunnel.Port > 65535 {
		return fmt.Errorf("%w: a tunnel needs an address, a WireGuard public key and a port", ErrInvalid)
	}
	tunnel.Address = tunnel.Address.Unmap()
	value := tunnel.Address.String() + " " + tunnel.Key + " " + strconv.Itoa(tunnel.Port)
	_, err := s.write(false, func(tx *recorder, current *snapshot, now time.Time) error {
		existing, member := current.nodes[node]
		if !member {
			return fmt.Errorf("node %s: %w", node, ErrNotFound)
		}
		if existing.Tunnel != nil && *existing.Tunnel == tunnel {
			return errUnchanged
		}
		for _, other := range current.nodes {
			if other.Name != node && other.Tunnel != nil && other.Tunnel.Address == tunnel.Address {
				return fmt.Errorf("%w: node %s has the tunnel address %s already", ErrConflict, other.Name, tunnel.Address)
			}
		}
		for _, device := range current.devices {
			if device.Address == tunnel.Address {
				return fmt.Errorf("%w: device %s has the tunnel address %s", ErrConflict, device.Name, tunnel.Address)
			}
		}
		if err := tx.Exec(`UPDATE nodes SET tunnel = ? WHERE name = ?`, value, node); err != nil {
			return err
		}
		return event(tx, now, "murder", "node.tunnel", "", "", node+" serves the panel at "+tunnel.Address.String())
	})
	if errors.Is(err, errUnchanged) {
		return nil
	}
	return err
}

func (s *SQLite) Devices() []Device {
	current := s.state.Load()
	devices := make([]Device, 0, len(current.devices))
	for _, name := range slices.Sorted(maps.Keys(current.devices)) {
		devices = append(devices, current.devices[name])
	}
	return devices
}

// firstDeviceHost is where devices' addresses begin in the tunnel's
// network; the nodes take addresses below it.
const firstDeviceHost = 100

func (s *SQLite) AddDevice(name, key string, network netip.Prefix, actor string) (Device, error) {
	switch {
	case !deviceName.MatchString(name):
		return Device{}, fmt.Errorf("%w: a device's name is 1 to 32 lowercase letters, digits and dashes", ErrInvalid)
	case !validKey(key):
		return Device{}, fmt.Errorf("%w: a device's key is a WireGuard public key in base64", ErrInvalid)
	case !network.IsValid() || !network.Addr().Is4() || network.Bits() > 24:
		return Device{}, fmt.Errorf("%w: the tunnel's network must be IPv4, a /24 or larger", ErrInvalid)
	}
	network = network.Masked()
	var added Device
	_, err := s.write(false, func(tx *recorder, current *snapshot, now time.Time) error {
		taken := map[netip.Addr]bool{}
		for _, device := range current.devices {
			switch {
			case device.Name == name:
				return fmt.Errorf("%w: device %s exists already", ErrConflict, name)
			case device.Key == key:
				return fmt.Errorf("%w: device %s has this key already", ErrConflict, device.Name)
			}
			taken[device.Address] = true
		}
		for _, node := range current.nodes {
			if node.Tunnel != nil {
				taken[node.Tunnel.Address] = true
			}
		}
		address := network.Addr()
		for range firstDeviceHost {
			address = address.Next()
		}
		for network.Contains(address) && taken[address] {
			address = address.Next()
		}
		// The network's last address is its broadcast address.
		if !network.Contains(address) || !network.Contains(address.Next()) {
			return fmt.Errorf("%w: no address is left for a device in %s", ErrConflict, network)
		}
		added = Device{Name: name, Key: key, Address: address, CreatedAt: now, CreatedBy: actor}
		if err := tx.Exec(`INSERT INTO devices (name, key, address, created_at, created_by) VALUES (?, ?, ?, ?, ?)`,
			name, key, address.String(), formatTime(now), actor); err != nil {
			return err
		}
		return event(tx, now, actor, "device.added", "", "", name+" at "+address.String())
	})
	return added, err
}

func (s *SQLite) RemoveDevice(name, actor string) error {
	_, err := s.write(false, func(tx *recorder, current *snapshot, now time.Time) error {
		device, ok := current.devices[name]
		if !ok {
			return fmt.Errorf("device %s: %w", name, ErrNotFound)
		}
		if err := tx.Exec(`DELETE FROM devices WHERE name = ?`, name); err != nil {
			return err
		}
		return event(tx, now, actor, "device.removed", "", "", name+" at "+device.Address.String())
	})
	return err
}
