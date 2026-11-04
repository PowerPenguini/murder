package cluster

import (
	"bytes"
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"fmt"
	"math/big"
	"slices"
	"sync/atomic"
	"time"
)

// serverName is the name every node's certificate carries and every node
// dials: any node of the cluster may answer for any other.
const serverName = "murder-cluster"

// Protocols negotiated on the cluster port: Raft's own, and HTTP for
// everything else.
const (
	protocolRaft = "murder-raft/1"
	protocolHTTP = "http/1.1"
)

// Credentials are what a node presents to, and expects of, other nodes.
// They follow the master key ring: the node presents a certificate of the
// authority derived from the key in use and trusts the authorities of
// every key in the ring, so nodes part way through a rotation still know
// each other. Update changes them as they run. A node's certificate is
// issued over its identity key, which stays as the authorities change.
type Credentials struct {
	node string
	// identity is the node's identity key; nil for an edge, whose
	// certificate has a key of its own for each run.
	identity ed25519.PrivateKey
	state    atomic.Pointer[credentialState]
	// servers, once set, checks the node a connection this node made
	// reached: its name and identity key.
	servers atomic.Pointer[func(name string, key ed25519.PublicKey) error]
}

type credentialState struct {
	seeds [][]byte
	caKey ed25519.PublicKey
	cert  tls.Certificate
	pool  *x509.CertPool
	// nodes, invited and relay are what the cluster port serves nodes and
	// invited hosts, and what the relay port serves edges.
	nodes, invited, relay *tls.Config
}

// NewCredentials derives the cluster's certificate authorities from seeds,
// the one in use first, so every node with the same master key has the
// same authorities, and issues node a certificate over its identity key.
// Without one, as for an edge, the certificate's key is made for this run
// and never leaves memory.
func NewCredentials(seeds [][]byte, node string, identity ed25519.PrivateKey, now time.Time) (*Credentials, error) {
	c := &Credentials{node: node, identity: identity}
	if err := c.Update(seeds, now); err != nil {
		return nil, err
	}
	return c, nil
}

// Update has the credentials follow seeds, the one in use first. A
// connection made before stays as it is; every new one uses them.
func (c *Credentials) Update(seeds [][]byte, now time.Time) error {
	if len(seeds) == 0 {
		return errors.New("no cluster seed")
	}
	if old := c.state.Load(); old != nil && slices.EqualFunc(old.seeds, seeds, bytes.Equal) {
		return nil
	}
	pool := x509.NewCertPool()
	var current *x509.Certificate
	var currentKey ed25519.PrivateKey
	for i, seed := range seeds {
		ca, key, err := authority(seed)
		if err != nil {
			return err
		}
		pool.AddCert(ca)
		if i == 0 {
			current, currentKey = ca, key
		}
	}
	var key crypto.Signer = c.identity
	if c.identity == nil {
		ephemeral, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			return err
		}
		key = ephemeral
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 127))
	if err != nil {
		return err
	}
	template := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: c.node},
		DNSNames:     []string{serverName},
		NotBefore:    now.Add(-time.Hour),
		NotAfter:     now.AddDate(10, 0, 0),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, current, key.Public(), currentKey)
	if err != nil {
		return fmt.Errorf("node certificate: %w", err)
	}
	state := &credentialState{
		seeds: slices.Clone(seeds), caKey: currentKey.Public().(ed25519.PublicKey), pool: pool,
		cert: tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key},
	}
	state.nodes = state.nodesOnly(protocolRaft, protocolHTTP)
	state.relay = state.nodesOnly("h2", protocolHTTP)
	state.invited = &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{state.cert},
		NextProtos:   []string{protocolJoin},
	}
	c.state.Store(state)
	return nil
}

// authority is the cluster's certificate authority derived from seed.
func authority(seed []byte) (*x509.Certificate, ed25519.PrivateKey, error) {
	if len(seed) != ed25519.SeedSize {
		return nil, nil, fmt.Errorf("cluster seed of %d bytes", len(seed))
	}
	caKey := ed25519.NewKeyFromSeed(seed)
	// Fixed fields: every node makes the same authority.
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "Murder cluster"},
		NotBefore:             time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		NotAfter:              time.Date(2126, 1, 1, 0, 0, 0, 0, time.UTC),
		IsCA:                  true,
		BasicConstraintsValid: true,
		MaxPathLenZero:        true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, caKey.Public(), caKey)
	if err != nil {
		return nil, nil, fmt.Errorf("cluster authority: %w", err)
	}
	ca, err := x509.ParseCertificate(caDER)
	if err != nil {
		return nil, nil, err
	}
	return ca, caKey, nil
}

// nodesOnly accepts nodes of the cluster only, for protocols.
func (s *credentialState) nodesOnly(protocols ...string) *tls.Config {
	return &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{s.cert},
		ClientCAs:    s.pool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		NextProtos:   protocols,
	}
}

// server accepts, on the cluster port, nodes of the cluster, and hosts
// that bring an invitation, for the invitation alone.
func (c *Credentials) server() *tls.Config {
	return &tls.Config{
		MinVersion: tls.VersionTLS13,
		GetConfigForClient: func(hello *tls.ClientHelloInfo) (*tls.Config, error) {
			state := c.state.Load()
			if len(hello.SupportedProtos) == 1 && hello.SupportedProtos[0] == protocolJoin {
				return state.invited, nil
			}
			return state.nodes, nil
		},
	}
}

// client dials nodes of the cluster only, for one protocol: certificates
// of its authorities, of the nodes that servers, once set, accepts.
func (c *Credentials) client(protocol string) *tls.Config {
	state := c.state.Load()
	return &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{state.cert},
		RootCAs:      state.pool,
		ServerName:   serverName,
		NextProtos:   []string{protocol},
		VerifyConnection: func(cs tls.ConnectionState) error {
			check := c.servers.Load()
			if check == nil {
				return nil
			}
			name, key, err := peerIdentity(&cs)
			if err != nil {
				return err
			}
			return (*check)(name, key)
		},
	}
}

// checkServers has every connection this node makes check the node it
// reached with check.
func (c *Credentials) checkServers(check func(name string, key ed25519.PublicKey) error) {
	c.servers.Store(&check)
}

// RelayServer accepts other nodes' edges passing requests on to this
// node's tenants: over HTTP/2, or HTTP/1.1 for protocol switches.
func (c *Credentials) RelayServer() *tls.Config {
	return &tls.Config{
		MinVersion: tls.VersionTLS13,
		NextProtos: []string{"h2", protocolHTTP},
		GetConfigForClient: func(*tls.ClientHelloInfo) (*tls.Config, error) {
			return c.state.Load().relay, nil
		},
	}
}

// RelayClient passes requests on to another node's edge. It is one
// configuration for as long as the edge runs, so it checks the other edge
// against the authorities as they are at each connection.
func (c *Credentials) RelayClient() *tls.Config {
	return &tls.Config{
		MinVersion: tls.VersionTLS13,
		NextProtos: []string{protocolHTTP},
		GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			return &c.state.Load().cert, nil
		},
		// Checked below, against the authorities of the moment: the
		// standard check would hold those of the start.
		InsecureSkipVerify: true,
		VerifyConnection: func(cs tls.ConnectionState) error {
			if len(cs.PeerCertificates) == 0 {
				return errors.New("the other edge presented no certificate")
			}
			intermediates := x509.NewCertPool()
			for _, cert := range cs.PeerCertificates[1:] {
				intermediates.AddCert(cert)
			}
			_, err := cs.PeerCertificates[0].Verify(x509.VerifyOptions{
				Roots: c.state.Load().pool, Intermediates: intermediates, DNSName: serverName,
				KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
			})
			return err
		},
	}
}
