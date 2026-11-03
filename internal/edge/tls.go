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
	"maps"
	"math/big"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/caddyserver/certmagic"
	"github.com/libdns/hetzner/v2"
	"github.com/libdns/namecheap"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// DNSProviders are the DNS-01 providers acme mode supports.
var DNSProviders = []string{"hetzner", "namecheap"}

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

// Certificates is the edge's TLS setup.
type Certificates struct {
	Config *tls.Config
	// NotAfter returns when the first of the certificates served now
	// expires; zero if none is known.
	NotAfter func() time.Time
	// stop ends what keeps the certificates renewed, if anything does.
	stop func()
}

// Close stops renewing the certificates, once nothing serves them.
func (c *Certificates) Close() {
	if c != nil && c.stop != nil {
		c.stop()
	}
}

// NewTLS returns the TLS setup for the wildcard and apex names of
// baseDomain, or nil when TLS is off.
func NewTLS(ctx context.Context, config TLSConfig, baseDomain string, log *slog.Logger) (*Certificates, error) {
	return newTLS(ctx, config, baseDomain, true, log)
}

// SharedTLS is NewTLS for a process beside the edge, such as the panel: it
// serves the same certificate, and never creates the internal CA, which
// the edge does; until then it fails.
func SharedTLS(ctx context.Context, config TLSConfig, baseDomain string, log *slog.Logger) (*Certificates, error) {
	return newTLS(ctx, config, baseDomain, false, log)
}

func newTLS(ctx context.Context, config TLSConfig, baseDomain string, createCA bool, log *slog.Logger) (*Certificates, error) {
	names := []string{"*." + baseDomain, baseDomain}
	switch config.Mode {
	case TLSOff, "":
		return nil, nil
	case TLSInternal:
		if config.Storage == "" {
			return nil, errors.New("internal TLS needs storage")
		}
		issuer := &internalIssuer{dir: config.Storage, names: names, now: time.Now, create: createCA}
		if _, err := issuer.certificate(); err != nil {
			return nil, err
		}
		return &Certificates{
			Config: &tls.Config{
				GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) { return issuer.certificate() },
				MinVersion:     tls.VersionTLS12, NextProtos: []string{"h2", "http/1.1"},
			},
			NotAfter: issuer.notAfter,
		}, nil
	case TLSACME:
		return acmeTLS(ctx, config, names, log)
	default:
		return nil, fmt.Errorf("unknown TLS mode %q", config.Mode)
	}
}

func acmeTLS(ctx context.Context, config TLSConfig, names []string, log *slog.Logger) (*Certificates, error) {
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
	// certmagic logs renewals and their failures; keep them, with the
	// agent's other logs.
	logger := zap.New(slogCore{log: log.With("component", "certmagic")})
	var magic *certmagic.Config
	cache := certmagic.NewCache(certmagic.CacheOptions{
		GetConfigForCert: func(certmagic.Certificate) (*certmagic.Config, error) { return magic, nil },
		Logger:           logger,
	})
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
		// The cache renews in the background from the start; the next
		// attempt makes its own.
		cache.Stop()
		return nil, fmt.Errorf("obtain certificate for %v: %w", names, err)
	}
	tlsConfig := magic.TLSConfig()
	tlsConfig.NextProtos = []string{"h2", "http/1.1"}
	tlsConfig.MinVersion = tls.VersionTLS12
	notAfter := func() time.Time {
		// certmagic renews a name's certificate a while before it expires;
		// the newest one of each name is the one served.
		var first time.Time
		for _, name := range names {
			var newest time.Time
			for _, certificate := range cache.AllMatchingCertificates(name) {
				if leaf, err := leafOf(certificate.Certificate); err == nil && leaf.NotAfter.After(newest) {
					newest = leaf.NotAfter
				}
			}
			if newest.IsZero() {
				return time.Time{}
			}
			if first.IsZero() || newest.Before(first) {
				first = newest
			}
		}
		return first
	}
	var once sync.Once
	return &Certificates{Config: tlsConfig, NotAfter: notAfter, stop: func() { once.Do(cache.Stop) }}, nil
}

func leafOf(certificate tls.Certificate) (*x509.Certificate, error) {
	if certificate.Leaf != nil {
		return certificate.Leaf, nil
	}
	if len(certificate.Certificate) == 0 {
		return nil, errors.New("empty certificate")
	}
	return x509.ParseCertificate(certificate.Certificate[0])
}

// internalIssuer signs the edge's certificate with the internal CA and signs
// a new one when less than a third of its 90 days is left, so an agent that
// runs for months keeps serving a valid one.
type internalIssuer struct {
	dir   string
	names []string
	now   func() time.Time

	mu      sync.Mutex
	current *tls.Certificate
	expires time.Time
	// create lets the first certificate create the CA; a process other
	// than the edge never does, so two do not make two CAs at once.
	create bool
	// retryAt holds back the next renewal after one failed.
	retryAt time.Time
}

// renewRetry is how long a failed renewal waits before the next attempt.
const renewRetry = time.Minute

func (i *internalIssuer) certificate() (*tls.Certificate, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	now := i.now()
	if i.current != nil && (now.Before(i.expires.Add(-internalLifetime/3)) || now.Before(i.retryAt)) {
		return i.current, nil
	}
	// The CA is created on the first start only: a new one later would not
	// be trusted by the clients that trust the first.
	certificate, err := internalCertificate(i.dir, i.names, now, i.current == nil && i.create)
	if err != nil {
		i.retryAt = now.Add(renewRetry)
		if i.current != nil && now.Before(i.expires) {
			return i.current, nil
		}
		return nil, err
	}
	leaf, err := leafOf(certificate)
	if err != nil {
		return nil, err
	}
	i.current, i.expires = &certificate, leaf.NotAfter
	return i.current, nil
}

func (i *internalIssuer) notAfter() time.Time {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.expires
}

// internalLifetime is how long an internal certificate is valid.
const internalLifetime = 90 * 24 * time.Hour

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
func internalCertificate(dir string, names []string, now time.Time, createCA bool) (tls.Certificate, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return tls.Certificate{}, err
	}
	caCert, caKey, err := loadOrCreateCA(dir, createCA)
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
		NotBefore:    now.Add(-time.Hour),
		NotAfter:     now.Add(internalLifetime),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, caCert, &key.PublicKey, caKey)
	if err != nil {
		return tls.Certificate{}, err
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		return tls.Certificate{}, err
	}
	return tls.Certificate{Certificate: [][]byte{der, caCert.Raw}, PrivateKey: key, Leaf: leaf}, nil
}

func loadOrCreateCA(dir string, create bool) (*x509.Certificate, *ecdsa.PrivateKey, error) {
	certPath, keyPath := filepath.Join(dir, "ca.pem"), filepath.Join(dir, "ca-key.pem")
	certPEM, err := os.ReadFile(certPath)
	if err != nil && (!create || !errors.Is(err, os.ErrNotExist)) {
		// Never replace a CA clients already trust because of a read error.
		return nil, nil, err
	}
	if err == nil {
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
		Subject:               pkix.Name{CommonName: "Murder internal CA"},
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

// slogCore passes certmagic's zap logs on to the agent's logger.
type slogCore struct {
	log    *slog.Logger
	fields []zapcore.Field
}

func (c slogCore) Enabled(level zapcore.Level) bool {
	return c.log.Enabled(context.Background(), slogLevel(level))
}

func (c slogCore) With(fields []zapcore.Field) zapcore.Core {
	return slogCore{log: c.log, fields: append(slices.Clip(c.fields), fields...)}
}

func (c slogCore) Check(entry zapcore.Entry, checked *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if c.Enabled(entry.Level) {
		return checked.AddCore(entry, c)
	}
	return checked
}

func (c slogCore) Write(entry zapcore.Entry, fields []zapcore.Field) error {
	encoder := zapcore.NewMapObjectEncoder()
	for _, field := range append(slices.Clip(c.fields), fields...) {
		field.AddTo(encoder)
	}
	attrs := make([]any, 0, 2*len(encoder.Fields)+2)
	if entry.LoggerName != "" {
		attrs = append(attrs, "logger", entry.LoggerName)
	}
	for _, key := range slices.Sorted(maps.Keys(encoder.Fields)) {
		attrs = append(attrs, key, encoder.Fields[key])
	}
	c.log.Log(context.Background(), slogLevel(entry.Level), entry.Message, attrs...)
	return nil
}

func (slogCore) Sync() error { return nil }

func slogLevel(level zapcore.Level) slog.Level {
	switch {
	case level <= zapcore.DebugLevel:
		return slog.LevelDebug
	case level == zapcore.InfoLevel:
		return slog.LevelInfo
	case level == zapcore.WarnLevel:
		return slog.LevelWarn
	default:
		return slog.LevelError
	}
}
