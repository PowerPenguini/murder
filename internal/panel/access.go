package panel

import (
	"context"
	"errors"
	"html/template"
	"net/http"
	"net/netip"
	"strings"
	"time"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/store"
	"github.com/PowerPenguini/murder/internal/tunnel"
)

// AccessAgent is what the access page asks of the agent, in the name of
// the person signed in.
type AccessAgent interface {
	Nodes(ctx context.Context) (api.Nodes, error)
	PanelUsers(ctx context.Context) ([]store.PanelUser, error)
	InvitePanelUser(ctx context.Context, name string, request api.InviteRequest) (store.PanelUser, error)
	RemovePanelUser(ctx context.Context, name string) error
	RemovePasskey(ctx context.Context, id string) error
	Devices(ctx context.Context) ([]store.Device, error)
	AddDevice(ctx context.Context, request api.DeviceRequest) (store.Device, error)
	RemoveDevice(ctx context.Context, name string) error
}

// Ends are the nodes' ends of the tunnel, as a device's configuration
// names them, and the nodes left out: those whose panel never ran, or
// without a public IPv4 address.
func Ends(nodes []api.NodeState) (ends []tunnel.Node, left []string) {
	for _, node := range nodes {
		var endpoint netip.Addr
		for _, address := range node.Public {
			if address.Unmap().Is4() {
				endpoint = address.Unmap()
				break
			}
		}
		if node.Tunnel == nil || !endpoint.IsValid() {
			left = append(left, node.Name)
			continue
		}
		ends = append(ends, tunnel.Node{Name: node.Name, Key: node.Tunnel.Key, Address: node.Tunnel.Address, Endpoint: netip.AddrPortFrom(endpoint, uint16(node.Tunnel.Port))})
	}
	return ends, left
}

type accessData struct {
	People  []store.PanelUser
	Devices []store.Device
	Me      string
	// CanCodes says recovery codes can be made here: someone signs in.
	CanCodes bool
	// Shown once, after an action: an invitation's link, recovery codes,
	// a device's QR code and configuration, or what went wrong.
	Invite  string
	Codes   []string
	Device  *deviceShown
	Problem string
	Done    string
}

type deviceShown struct {
	Name, Config string
	QR           template.HTML
}

// access is the people and devices page; a POST does one thing and shows
// the page with what it gave, once.
func (s *Server) access(w http.ResponseWriter, r *http.Request) {
	user := userOf(r)
	agent := s.Access(s.actor(user))
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	data := accessData{Me: user, CanCodes: s.Auth != nil}
	if r.Method == http.MethodPost {
		s.accessAction(ctx, r, agent, &data)
	}
	var err error
	if data.People, err = agent.PanelUsers(ctx); err != nil {
		s.fail(w, r, err)
		return
	}
	if data.Devices, err = agent.Devices(ctx); err != nil {
		s.fail(w, r, err)
		return
	}
	s.render(w, r, "access", "Access", "access", data)
}

func (s *Server) accessAction(ctx context.Context, r *http.Request, agent AccessAgent, data *accessData) {
	fail := func(err error) { data.Problem = err.Error() }
	name := strings.TrimSpace(r.FormValue("name"))
	confirmed := strings.TrimSpace(r.FormValue("confirm"))
	switch r.FormValue("do") {
	case "invite":
		// Another person who has passkeys adds the next one themselves:
		// an invitation for them would let whoever uses it act as them.
		if name != data.Me {
			people, err := agent.PanelUsers(ctx)
			if err != nil {
				fail(err)
				return
			}
			for _, person := range people {
				if person.Name == name && len(person.Passkeys) > 0 {
					fail(errors.New(name + " has passkeys: they add another themselves, signed in; root can invite them on a node (murder admin invite)"))
					return
				}
			}
		}
		token := Token(32)
		if _, err := agent.InvitePanelUser(ctx, name, api.InviteRequest{Hash: TokenHash(token), Expires: s.now().Add(InviteLength)}); err != nil {
			fail(err)
			return
		}
		data.Invite = s.origin() + "/enroll#" + token
		data.Done = "an invitation for " + name + ", valid for 24 hours, once"
	case "remove-person":
		if name == data.Me {
			fail(errors.New("you cannot remove yourself: ask another person"))
			return
		}
		if confirmed != name {
			fail(errors.New("type the person's name to remove them"))
			return
		}
		if err := agent.RemovePanelUser(ctx, name); err != nil {
			fail(err)
			return
		}
		data.Done = name + " no longer signs in"
	case "remove-passkey":
		id := r.FormValue("id")
		people, err := agent.PanelUsers(ctx)
		if err != nil {
			fail(err)
			return
		}
		label, owner := "", ""
		for _, person := range people {
			for _, key := range person.Passkeys {
				if key.ID == id {
					label, owner = key.Label, person.Name
				}
			}
		}
		if owner == "" || confirmed != passkeyName(label, owner) {
			fail(errors.New("type the passkey's name to remove it"))
			return
		}
		if err := agent.RemovePasskey(ctx, id); err != nil {
			fail(err)
			return
		}
		data.Done = "passkey " + passkeyName(label, owner) + " of " + owner + " was removed: it signs in no more"
	case "codes":
		if s.Auth == nil {
			fail(errors.New("no one signs in here"))
			return
		}
		codes, err := s.Auth.newRecoveryCodes(ctx, data.Me)
		if err != nil {
			fail(err)
			return
		}
		data.Codes = codes
		data.Done = "your new recovery codes: the old ones no longer work"
	case "add-device":
		if !s.TunnelNetwork.IsValid() {
			fail(errors.New("the panel serves in no tunnel here"))
			return
		}
		shown, err := s.addDevice(ctx, agent, name)
		if err != nil {
			fail(err)
			return
		}
		data.Device = shown
		data.Done = "device " + name + " may reach the panel"
	case "remove-device":
		if confirmed != name {
			fail(errors.New("type the device's name to remove it"))
			return
		}
		if err := agent.RemoveDevice(ctx, name); err != nil {
			fail(err)
			return
		}
		data.Done = "device " + name + " no longer reaches the panel"
	default:
		fail(errors.New("unknown action"))
	}
}

// addDevice adds a device and makes its configuration, shown once.
func (s *Server) addDevice(ctx context.Context, agent AccessAgent, name string) (*deviceShown, error) {
	nodes, err := agent.Nodes(ctx)
	if err != nil {
		return nil, err
	}
	ends, _ := Ends(nodes.Nodes)
	if len(ends) == 0 {
		return nil, errors.New("no node serves the panel in its tunnel yet")
	}
	private, public, err := tunnel.GenerateKey()
	if err != nil {
		return nil, err
	}
	device, err := agent.AddDevice(ctx, api.DeviceRequest{Name: name, Key: public, Network: s.TunnelNetwork})
	if err != nil {
		return nil, err
	}
	config := tunnel.DeviceConfig{PrivateKey: private, Address: device.Address, Panel: s.origin(), Nodes: ends}
	svg, err := tunnel.QRSVG(config.Compact())
	if err != nil {
		svg = ""
	}
	return &deviceShown{Name: name, Config: config.String(), QR: template.HTML(svg)}, nil
}

// origin is where the panel's pages are.
func (s *Server) origin() string {
	if s.Auth != nil {
		return s.Auth.origin
	}
	return "https://" + s.Name
}

// actor names who acts through the panel, in the audit log.
func (s *Server) actor(user string) string {
	if user == "" {
		return "murder-panel socket"
	}
	return user + " (panel)"
}

// passkeyName is what removing a passkey asks to be typed: its label, or
// its owner's name when it has none.
func passkeyName(label, owner string) string {
	if label == "" {
		return owner
	}
	return label
}
