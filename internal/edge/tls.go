package edge

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/caddyserver/certmagic"
	"github.com/libdns/hetzner/v2"
	"github.com/libdns/namecheap"
	"go.uber.org/zap"
)

// TLS modes.
const (
	TLSOff      = "off"
	TLSInternal = "internal"
	TLSACME     = "acme"
)

// TLSConfig chooses how the edge gets its certificate.
type TLSConfig struct {
	// Mode is "off" (plain HTTP only), "internal" (a local CA kept in
	// Storage, for testing) or "acme" (a wildcard certificate through the
	// ACME DNS-01 challenge).
	Mode    string `yaml:"mode"`
	Storage string `yaml:"storage"`
	// ACME settings. CA defaults to Let's Encrypt production.
	Email string `yaml:"email"`
	CA    string `yaml:"ca"`
	// DNSProvider is "hetzner" (Hetzner Cloud DNS) or "namecheap".
	DNSProvider string `yaml:"dnsProvider"`
	// DNSTokenFile holds the provider's API token; for Namecheap it holds
	// "user:apikey".
	DNSTokenFile string `yaml:"dnsTokenFile"`
}

// NewTLS returns the TLS configuration for the wildcard and apex names of
// baseDomain, or nil when TLS is off.
func NewTLS(ctx context.Context, config TLSConfig, baseDomain string, log *slog.Logger) (*tls.Config, error) {
	names := []string{"*." + baseDomain, baseDomain}
	switch config.Mode {
	case TLSOff, "":
		return nil, nil
	case TLSInternal:
		certificate, err := internalCertificate(config.Storage, names)
		if err != nil {
			return nil, err
		}
		return &tls.Config{Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS12, NextProtos: []string{"h2", "http/1.1"}}, nil
	case TLSACME:
		return acmeTLS(ctx, config, names, log)
	default:
		return nil, fmt.Errorf("unknown TLS mode %q", config.Mode)
	}
}

func acmeTLS(ctx context.Context, config TLSConfig, names []string, log *slog.Logger) (*tls.Config, error) {
	if config.Email == "" || config.Storage == "" {
		return nil, errors.New("acme TLS needs email and storage")
	}
	provider, err := dnsProvider(config.DNSProvider, config.DNSTokenFile)
	if err != nil {
		return nil, err
	}
	ca := config.CA
	if ca == "" {
		ca = certmagic.LetsEncryptProductionCA
	}
	var magic *certmagic.Config
	cache := certmagic.NewCache(certmagic.CacheOptions{
		GetConfigForCert: func(certmagic.Certificate) (*certmagic.Config, error) { return magic, nil },
	})
	logger := zap.NewNop()
	magic = certmagic.New(cache, certmagic.Config{Storage: &certmagic.FileStorage{Path: config.Storage}, Logger: logger})
	issuer := certmagic.NewACMEIssuer(magic, certmagic.ACMEIssuer{
		CA:                      ca,
		Email:                   config.Email,
		Agreed:                  true,
		DisableHTTPChallenge:    true,
		DisableTLSALPNChallenge: true,
		DNS01Solver:             &certmagic.DNS01Solver{DNSManager: certmagic.DNSManager{DNSProvider: provider}},
		Logger:                  logger,
	})
	magic.Issuers = []certmagic.Issuer{issuer}
	log.Info("obtaining wildcard certificate", "names", names, "ca", ca)
	if err := magic.ManageSync(ctx, names); err != nil {
		return nil, fmt.Errorf("obtain certificate for %v: %w", names, err)
	}
	tlsConfig := magic.TLSConfig()
	tlsConfig.NextProtos = []string{"h2", "http/1.1"}
	tlsConfig.MinVersion = tls.VersionTLS12
	return tlsConfig, nil
}

// dnsProvider builds the DNS-01 solver's provider from a token file.
func dnsProvider(name, tokenFile string) (certmagic.DNSProvider, error) {
	data, err := os.ReadFile(tokenFile)
	if err != nil {
		return nil, fmt.Errorf("read DNS token: %w", err)
	}
	token := strings.TrimSpace(string(data))
	switch name {
	case "hetzner":
		return &hetzner.Provider{APIToken: token}, nil
	case "namecheap":
		user, key, ok := strings.Cut(token, ":")
		if !ok {
			return nil, errors.New("namecheap token file must hold user:apikey")
		}
		return &namecheap.Provider{User: user, APIKey: key}, nil
	default:
		return nil, fmt.Errorf("unknown DNS provider %q", name)
	}
}

// internalCertificate signs the names with a local CA kept in dir, creating
// the CA on first use. Clients must trust dir/ca.pem.
func internalCertificate(dir string, names []string) (tls.Certificate, error) {
	if dir == "" {
		return tls.Certificate{}, errors.New("internal TLS needs storage")
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return tls.Certificate{}, err
	}
	caCert, caKey, err := loadOrCreateCA(dir)
	if err != nil {
		return tls.Certificate{}, err
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, err
	}
	template := &x509.Certificate{
		SerialNumber: randomSerial(),
		Subject:      pkix.Name{CommonName: names[0]},
		DNSNames:     names,
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(90 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, caCert, &key.PublicKey, caKey)
	if err != nil {
		return tls.Certificate{}, err
	}
	return tls.Certificate{Certificate: [][]byte{der, caCert.Raw}, PrivateKey: key}, nil
}

func loadOrCreateCA(dir string) (*x509.Certificate, *ecdsa.PrivateKey, error) {
	certPath, keyPath := filepath.Join(dir, "ca.pem"), filepath.Join(dir, "ca-key.pem")
	if certPEM, err := os.ReadFile(certPath); err == nil {
		keyPEM, err := os.ReadFile(keyPath)
		if err != nil {
			return nil, nil, err
		}
		certBlock, keyBlock := firstPEM(certPEM), firstPEM(keyPEM)
		if certBlock == nil || keyBlock == nil {
			return nil, nil, fmt.Errorf("internal CA in %s is malformed", dir)
		}
		cert, err := x509.ParseCertificate(certBlock.Bytes)
		if err != nil {
			return nil, nil, err
		}
		key, err := x509.ParseECPrivateKey(keyBlock.Bytes)
		return cert, key, err
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	template := &x509.Certificate{
		SerialNumber:          randomSerial(),
		Subject:               pkix.Name{CommonName: "Corral internal CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(10 * 365 * 24 * time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return nil, nil, err
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return nil, nil, err
	}
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		return nil, nil, err
	}
	if err := os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o644); err != nil {
		return nil, nil, err
	}
	cert, err := x509.ParseCertificate(der)
	return cert, key, err
}

func firstPEM(data []byte) *pem.Block {
	block, _ := pem.Decode(data)
	return block
}

func randomSerial() *big.Int {
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 127))
	if err != nil {
		panic(err)
	}
	return serial
}
