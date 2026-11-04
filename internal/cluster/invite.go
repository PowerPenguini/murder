package cluster

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"strings"
	"time"
)

// protocolJoin is negotiated by a host that is not a node yet and brings
// an invitation: it presents no certificate, and the node answers nothing
// but the invitation.
const protocolJoin = "murder-join/1"

// inviteSecretSize is the invitation secret's size in bytes: 160 bits.
const inviteSecretSize = 20

// Invite is what a host needs to join a cluster: where a node is, its
// name and the public half of its identity key, and a secret it accepts
// once. The host talks only to the node that holds that key, so whoever
// holds the cluster's master key cannot stand in for it, take the secret
// or hand the host members of their own.
type Invite struct {
	Address string
	Node    string
	Key     ed25519.PublicKey
	Secret  []byte
	// CA, if set, is the public key of the cluster's authority the node's
	// certificate must be signed with, checked before the secret goes out:
	// a node that holds a master key already checks that it is the
	// cluster's. It is not part of the invitation.
	CA ed25519.PublicKey
}

// String is the invitation as one word: address/node/secret.key.
func (i Invite) String() string {
	encode := base64.RawURLEncoding.EncodeToString
	return i.Address + "/" + i.Node + "/" + encode(i.Secret) + "." + encode(i.Key)
}

// ErrOldInvite refuses an invitation of a release before node identity
// keys, which names the cluster's authority but not the inviting node.
var ErrOldInvite = errors.New("this invitation was made by a release before node identity keys, which does not name the inviting node: make a new one with `murder cluster invite` on a node of this release")

// ParseInvite reads an invitation String wrote.
func ParseInvite(text string) (Invite, error) {
	parts := strings.Split(strings.TrimSpace(text), "/")
	if len(parts) == 2 && strings.Contains(parts[1], ".") {
		return Invite{}, ErrOldInvite
	}
	if len(parts) != 3 {
		return Invite{}, errors.New("an invitation reads address/node/secret.key, as `murder cluster invite` prints it")
	}
	address, node, rest := parts[0], parts[1], parts[2]
	secret, key, ok := strings.Cut(rest, ".")
	if !ok {
		return Invite{}, errors.New("an invitation reads address/node/secret.key, as `murder cluster invite` prints it")
	}
	if _, err := netip.ParseAddrPort(address); err != nil {
		return Invite{}, fmt.Errorf("the invitation's address %q: %w", address, err)
	}
	if node == "" {
		return Invite{}, errors.New("the invitation names no node: copy it whole")
	}
	invite := Invite{Address: address, Node: node}
	var err error
	if invite.Secret, err = base64.RawURLEncoding.DecodeString(secret); err != nil || len(invite.Secret) != inviteSecretSize {
		return Invite{}, errors.New("the invitation's secret is damaged: copy it whole")
	}
	raw, err := base64.RawURLEncoding.DecodeString(key)
	if err != nil || len(raw) != ed25519.PublicKeySize {
		return Invite{}, errors.New("the invitation's key is damaged: copy it whole")
	}
	invite.Key = raw
	return invite, nil
}

// Bundle is what a node hands a host it invited: everything the host
// needs to become a node like it.
type Bundle struct {
	// Cluster names the cluster.
	Cluster string `json:"cluster"`
	// Key is the master key file's contents.
	Key []byte `json:"key"`
	// Config is the inviting node's agent config, which the host makes
	// its own.
	Config []byte `json:"config"`
	// Files are the files the config names, such as tokens, by path.
	Files map[string][]byte `json:"files"`
	// TunnelAddress is the host's address in the panel's tunnel, unused
	// by any node; empty without the panel's tunnel.
	TunnelAddress string `json:"tunnelAddress,omitempty"`
	// Members are the identity keys of the cluster's nodes, by name, which
	// the host trusts until it holds the cluster's state; empty for a node
	// of a release before keys that has not registered its own.
	Members map[string]string `json:"members,omitempty"`
}

// Invites lets hosts join with an invitation.
type Invites interface {
	// Redeem takes an invitation's secret, once, for a host that will be
	// node with identity key key: it admits them, and returns what the
	// host needs.
	Redeem(ctx context.Context, secret []byte, node string, key ed25519.PublicKey) (Bundle, error)
}

// ErrInvite refuses an invitation that is unknown, used or expired; it
// says no more, so a guess learns nothing.
var ErrInvite = errors.New("the invitation is unknown, used or expired: ask a node for a new one with `murder cluster invite`")

// inviteRequest is what a host sends with its invitation: the secret, the
// name it will have and the public key of its identity.
type inviteRequest struct {
	Secret []byte `json:"secret"`
	Node   string `json:"node"`
	Key    []byte `json:"key,omitempty"`
}

// joinHandler answers invitations, and nothing else.
func joinHandler(invites Invites) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /cluster/v1/invitation", func(w http.ResponseWriter, r *http.Request) {
		var request inviteRequest
		if err := json.NewDecoder(io.LimitReader(r.Body, 4096)).Decode(&request); err != nil {
			http.Error(w, "a malformed request", http.StatusBadRequest)
			return
		}
		if invites == nil {
			http.Error(w, ErrInvite.Error(), http.StatusForbidden)
			return
		}
		bundle, err := invites.Redeem(r.Context(), request.Secret, request.Node, request.Key)
		switch {
		case errors.Is(err, ErrInvite):
			http.Error(w, ErrInvite.Error(), http.StatusForbidden)
			return
		case err != nil:
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(bundle)
	})
	return mux
}

// CAKey is the public key of the cluster's authority.
func (c *Credentials) CAKey() ed25519.PublicKey { return c.state.Load().caKey }

// Redeem asks the node an invitation names to admit a host as node, with
// the identity key key, and for what the host needs to become that node.
// It talks to that node only once its certificate is signed by the
// authority the invitation names.
func Redeem(ctx context.Context, invite Invite, node string, key ed25519.PublicKey) (Bundle, error) {
	config := &tls.Config{
		MinVersion: tls.VersionTLS13,
		NextProtos: []string{protocolJoin},
		// The host holds no authority to check the node's certificate
		// against: it checks it against the key the invitation pins.
		InsecureSkipVerify: true,
		VerifyConnection: func(state tls.ConnectionState) error {
			return pinned(state, invite, time.Now())
		},
	}
	client := &http.Client{Transport: &http.Transport{
		DialTLSContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			conn, err := (&tls.Dialer{NetDialer: netDialer(), Config: config}).DialContext(ctx, "tcp", invite.Address)
			if err != nil {
				return nil, err
			}
			if conn.(*tls.Conn).ConnectionState().NegotiatedProtocol != protocolJoin {
				_ = conn.Close()
				return nil, errors.New("the node does not take invitations: is it a node of this release?")
			}
			return conn, nil
		},
	}}
	defer client.CloseIdleConnections()
	body, _ := json.Marshal(inviteRequest{Secret: invite.Secret, Node: node, Key: key})
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://"+serverName+"/cluster/v1/invitation", bytes.NewReader(body))
	if err != nil {
		return Bundle{}, err
	}
	response, err := client.Do(request)
	if err != nil {
		return Bundle{}, fmt.Errorf("reach %s: %w", invite.Address, err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, 16<<20))
	if err != nil {
		return Bundle{}, err
	}
	if response.StatusCode != http.StatusOK {
		return Bundle{}, fmt.Errorf("%s refused: %s", invite.Address, strings.TrimSpace(string(data)))
	}
	var bundle Bundle
	if err := json.Unmarshal(data, &bundle); err != nil {
		return Bundle{}, fmt.Errorf("a malformed answer from %s: %w", invite.Address, err)
	}
	return bundle, nil
}

// pinned checks that the peer is the node the invitation names: its
// certificate names it and carries its identity key, which the node proved
// it holds in the handshake; and, if the invitation says, that it is signed
// by the cluster's authority.
func pinned(state tls.ConnectionState, invite Invite, now time.Time) error {
	if len(state.PeerCertificates) == 0 {
		return errors.New("the node presented no certificate")
	}
	leaf := state.PeerCertificates[0]
	key, _ := leaf.PublicKey.(ed25519.PublicKey)
	if leaf.Subject.CommonName != invite.Node || !key.Equal(invite.Key) {
		return fmt.Errorf("the node at %s is not %s, the node that made the invitation: is the address right?", invite.Address, invite.Node)
	}
	if invite.CA != nil && (leaf.SignatureAlgorithm != x509.PureEd25519 || !ed25519.Verify(invite.CA, leaf.RawTBSCertificate, leaf.Signature)) {
		return errOtherAuthority
	}
	if now.Before(leaf.NotBefore) || now.After(leaf.NotAfter) {
		return errors.New("the node's certificate is not valid now: check the clocks")
	}
	return leaf.VerifyHostname(serverName)
}

// errOtherAuthority reports an inviting node whose certificate is not of
// the authority the host trusts.
var errOtherAuthority = errors.New("the invitation is to a cluster of another master key than this node's: copy the cluster's master key here first, or set the host up with `murder setup -join`")
