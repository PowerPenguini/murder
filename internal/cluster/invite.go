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

// Invite is what a host needs to join a cluster: where a node is, a
// secret it accepts once, and the public key of the cluster's authority,
// which every node's certificate is signed with, so the host knows it
// talks to the cluster before it holds any of its keys.
type Invite struct {
	Address string
	Secret  []byte
	CA      ed25519.PublicKey
}

// String is the invitation as one word: address/secret.authority.
func (i Invite) String() string {
	encode := base64.RawURLEncoding.EncodeToString
	return i.Address + "/" + encode(i.Secret) + "." + encode(i.CA)
}

// ParseInvite reads an invitation String wrote.
func ParseInvite(text string) (Invite, error) {
	address, rest, ok := strings.Cut(strings.TrimSpace(text), "/")
	secret, ca, ok2 := strings.Cut(rest, ".")
	if !ok || !ok2 {
		return Invite{}, errors.New("an invitation reads address/secret.key, as `murder cluster invite` prints it")
	}
	if _, err := netip.ParseAddrPort(address); err != nil {
		return Invite{}, fmt.Errorf("the invitation's address %q: %w", address, err)
	}
	invite := Invite{Address: address}
	var err error
	if invite.Secret, err = base64.RawURLEncoding.DecodeString(secret); err != nil || len(invite.Secret) != inviteSecretSize {
		return Invite{}, errors.New("the invitation's secret is damaged: copy it whole")
	}
	key, err := base64.RawURLEncoding.DecodeString(ca)
	if err != nil || len(key) != ed25519.PublicKeySize {
		return Invite{}, errors.New("the invitation's key is damaged: copy it whole")
	}
	invite.CA = key
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
}

// Invites lets hosts join with an invitation.
type Invites interface {
	// Redeem takes an invitation's secret, once, for a host that will be
	// node, and returns what it needs.
	Redeem(ctx context.Context, secret []byte, node string) (Bundle, error)
}

// ErrInvite refuses an invitation that is unknown, used or expired; it
// says no more, so a guess learns nothing.
var ErrInvite = errors.New("the invitation is unknown, used or expired: ask a node for a new one with `murder cluster invite`")

// inviteRequest is what a host sends with its invitation.
type inviteRequest struct {
	Secret []byte `json:"secret"`
	Node   string `json:"node"`
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
		bundle, err := invites.Redeem(r.Context(), request.Secret, request.Node)
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
func (c *Credentials) CAKey() ed25519.PublicKey { return c.caKey }

// Join asks the node an invitation names for what a host needs to become
// node. It talks to that node only once its certificate is signed by the
// authority the invitation names.
func Join(ctx context.Context, invite Invite, node string) (Bundle, error) {
	config := &tls.Config{
		MinVersion: tls.VersionTLS13,
		NextProtos: []string{protocolJoin},
		// The host holds no authority to check the node's certificate
		// against: it checks it against the key the invitation pins.
		InsecureSkipVerify: true,
		VerifyConnection: func(state tls.ConnectionState) error {
			return pinned(state, invite.CA, time.Now())
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
	body, _ := json.Marshal(inviteRequest{Secret: invite.Secret, Node: node})
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

// pinned checks that the peer's certificate is a node's of the cluster
// whose authority's key is ca.
func pinned(state tls.ConnectionState, ca ed25519.PublicKey, now time.Time) error {
	if len(state.PeerCertificates) == 0 {
		return errors.New("the node presented no certificate")
	}
	leaf := state.PeerCertificates[0]
	if leaf.SignatureAlgorithm != x509.PureEd25519 || !ed25519.Verify(ca, leaf.RawTBSCertificate, leaf.Signature) {
		return errors.New("the node's certificate is not the cluster's the invitation names: is the address right?")
	}
	if now.Before(leaf.NotBefore) || now.After(leaf.NotAfter) {
		return errors.New("the node's certificate is not valid now: check the clocks")
	}
	if err := leaf.VerifyHostname(serverName); err != nil {
		return err
	}
	return nil
}
