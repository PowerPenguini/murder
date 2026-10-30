package cluster

import (
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
type Credentials struct {
	ca   *x509.CertPool
	cert tls.Certificate
}

// NewCredentials derives the cluster's certificate authority from seed, so
// every node with the same master key has the same authority and nothing
// but the master key is shared, and issues node a certificate for this run
// of the agent; its key never leaves memory.
func NewCredentials(seed []byte, node string, now time.Time) (*Credentials, error) {
	if len(seed) != ed25519.SeedSize {
		return nil, fmt.Errorf("cluster seed of %d bytes", len(seed))
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
		return nil, fmt.Errorf("cluster authority: %w", err)
	}
	ca, err := x509.ParseCertificate(caDER)
	if err != nil {
		return nil, err
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 127))
	if err != nil {
		return nil, err
	}
	template := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: node},
		DNSNames:     []string{serverName},
		NotBefore:    now.Add(-time.Hour),
		NotAfter:     now.AddDate(10, 0, 0),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca, key.Public(), caKey)
	if err != nil {
		return nil, fmt.Errorf("node certificate: %w", err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(ca)
	return &Credentials{ca: pool, cert: tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}}, nil
}

// server accepts nodes of the cluster only.
func (c *Credentials) server() *tls.Config {
	return &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{c.cert},
		ClientCAs:    c.ca,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		NextProtos:   []string{protocolRaft, protocolHTTP},
	}
}

// client dials nodes of the cluster only, for one protocol.
func (c *Credentials) client(protocol string) *tls.Config {
	return &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{c.cert},
		RootCAs:      c.ca,
		ServerName:   serverName,
		NextProtos:   []string{protocol},
	}
}

// RelayServer accepts other nodes' edges passing requests on to this
// node's tenants.
func (c *Credentials) RelayServer() *tls.Config {
	config := c.server()
	config.NextProtos = []string{protocolHTTP}
	return config
}

// RelayClient passes requests on to another node's edge.
func (c *Credentials) RelayClient() *tls.Config { return c.client(protocolHTTP) }

// peerName is the node name in the certificate a peer presented.
func peerName(state *tls.ConnectionState) (string, error) {
	if state == nil || len(state.VerifiedChains) == 0 || len(state.VerifiedChains[0]) == 0 {
		return "", errors.New("no verified cluster certificate")
	}
	return state.VerifiedChains[0][0].Subject.CommonName, nil
}
