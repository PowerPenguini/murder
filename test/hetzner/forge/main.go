// Command forge tests what the master key alone gets on a cluster port.
// It derives the cluster's authority from the master key, as every node
// does, and makes a certificate naming a node, over a fresh key (or over
// a real node.key, with -identity, as a control). Then it tries what only
// members may do: member requests, the state's snapshot, a join, and a
// Raft connection that sends a heartbeat of term 0, which a member answers
// and ignores. It changes nothing on a cluster that refuses it.
//
//	go build ./test/hetzner/forge
//	forge -master /etc/murder/master.key -name murder-test-n1 -cluster NAME -target 10.0.1.2:7400
package main

import (
	"bytes"
	"context"
	"crypto"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"flag"
	"fmt"
	"io"
	"log"
	"math/big"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/hashicorp/raft"

	"github.com/PowerPenguini/murder/internal/secrets"
)

func main() {
	master := flag.String("master", "/etc/murder/master.key", "the master key")
	name := flag.String("name", "", "the node the certificate names")
	identity := flag.String("identity", "", "a node.key to issue the certificate over (a control); default a fresh key")
	clusterName := flag.String("cluster", "", "the cluster name to send")
	target := flag.String("target", "", "host:port of a node's cluster port")
	joinBody := flag.String("join-body", "{}", "the body of the join request (POST /cluster/v1/nodes)")
	only := flag.String("only", "", "comma-separated tests to run (default all): node,index,commit,register,admit,binary,snapshot,join,raft")
	flag.Parse()
	if *name == "" || *target == "" {
		log.Fatal("-name and -target are needed")
	}
	key, err := secrets.LoadKey(*master)
	if err != nil {
		log.Fatal(err)
	}
	seed := key.DeriveAll("cluster ca")[0]
	caKey := ed25519.NewKeyFromSeed(seed)
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
		log.Fatal(err)
	}
	ca, _ := x509.ParseCertificate(caDER)

	var leafKey crypto.Signer
	if *identity != "" {
		data, err := os.ReadFile(*identity)
		if err != nil {
			log.Fatal(err)
		}
		block, _ := pem.Decode(data)
		parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
		if err != nil {
			log.Fatal(err)
		}
		leafKey = parsed.(ed25519.PrivateKey)
		fmt.Printf("certificate for %q over the node.key at %s (control)\n", *name, *identity)
	} else {
		_, fresh, _ := ed25519.GenerateKey(rand.Reader)
		leafKey = fresh
		fmt.Printf("certificate for %q over a FRESH key (forged)\n", *name)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: *name},
		DNSNames:     []string{"murder-cluster"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().AddDate(1, 0, 0),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca, leafKey.Public(), caKey)
	if err != nil {
		log.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(ca)
	cert := tls.Certificate{Certificate: [][]byte{der}, PrivateKey: leafKey}
	tlsFor := func(protocol string) *tls.Config {
		return &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{cert}, RootCAs: pool, ServerName: "murder-cluster", NextProtos: []string{protocol}}
	}
	fmt.Printf("target %s, cluster %q, at %s\n\n", *target, *clusterName, time.Now().UTC().Format(time.RFC3339Nano))

	selected := func(test string) bool {
		if *only == "" {
			return true
		}
		for _, t := range strings.Split(*only, ",") {
			if t == test {
				return true
			}
		}
		return false
	}
	client := &http.Client{Timeout: 20 * time.Second, Transport: &http.Transport{TLSClientConfig: tlsFor("http/1.1"), ForceAttemptHTTP2: false}}
	request := func(test, method, path, body string) {
		if !selected(test) {
			return
		}
		req, _ := http.NewRequest(method, "https://"+*target+path, strings.NewReader(body))
		req.Header.Set("X-Murder-Cluster", *clusterName)
		if body != "" {
			req.Header.Set("Content-Type", "application/json")
		}
		started := time.Now()
		resp, err := client.Do(req)
		if err != nil {
			fmt.Printf("%-6s %-32s -> error: %v\n", method, path, err)
			return
		}
		defer resp.Body.Close()
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		shown := strings.TrimSpace(string(data))
		if resp.Header.Get("Content-Type") == "application/vnd.sqlite3" || bytes.HasPrefix(data, []byte("SQLite format")) {
			shown = fmt.Sprintf("<%d bytes of SQLite database>", len(data))
		}
		if len(shown) > 300 {
			shown = shown[:300] + "..."
		}
		fmt.Printf("%-6s %-32s -> %d in %s: %s\n", method, path, resp.StatusCode, time.Since(started).Round(time.Millisecond), shown)
	}
	request("node", "GET", "/cluster/v1/node", "")
	request("index", "GET", "/cluster/v1/index", "")
	request("commit", "POST", "/cluster/v1/commit", "{}")
	request("register", "POST", "/cluster/v1/node/key", "{}")
	request("admit", "POST", "/cluster/v1/admissions", "{}")
	request("binary", "PUT", "/cluster/v1/node/binary", "")
	request("snapshot", "GET", "/cluster/v1/snapshot", "")
	request("join", "POST", "/cluster/v1/nodes", *joinBody)

	if selected("raft") {
		layer := &layer{config: tlsFor("murder-raft/1"), cluster: *clusterName}
		transport := raft.NewNetworkTransportWithConfig(&raft.NetworkTransportConfig{Stream: layer, MaxPool: 1, Timeout: 10 * time.Second, Logger: nil})
		var resp raft.AppendEntriesResponse
		started := time.Now()
		err := transport.AppendEntries(raft.ServerID(*name), raft.ServerAddress(*target), &raft.AppendEntriesRequest{
			RPCHeader: raft.RPCHeader{ProtocolVersion: raft.ProtocolVersionMax, ID: []byte(*name), Addr: []byte(*target)},
			Term:      0,
		}, &resp)
		if err != nil {
			fmt.Printf("RAFT   AppendEntries(term 0)          -> refused after %s: %v (handshake: %s)\n", time.Since(started).Round(time.Millisecond), err, layer.handshake)
		} else {
			fmt.Printf("RAFT   AppendEntries(term 0)          -> ANSWERED in %s: term=%d success=%v (handshake: %s)\n", time.Since(started).Round(time.Millisecond), resp.Term, resp.Success, layer.handshake)
		}
		_ = transport.Close()
	}
}

// layer dials a node's cluster port for Raft, as a node does: TLS with the
// Raft protocol, then the cluster's name.
type layer struct {
	config    *tls.Config
	cluster   string
	handshake string
}

func (l *layer) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	conn, err := (&tls.Dialer{Config: l.config}).DialContext(ctx, "tcp", string(address))
	if err != nil {
		l.handshake = "failed: " + err.Error()
		return nil, err
	}
	l.handshake = "ok, ALPN " + conn.(*tls.Conn).ConnectionState().NegotiatedProtocol
	if _, err := io.WriteString(conn, "murder-cluster "+l.cluster+"\n"); err != nil {
		return nil, err
	}
	return conn, nil
}

func (l *layer) Accept() (net.Conn, error) { select {} }
func (l *layer) Close() error              { return nil }
func (l *layer) Addr() net.Addr            { return &net.TCPAddr{IP: net.IPv4zero} }
