package domains

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/PowerPenguini/murder/internal/store"
)

// probeWait is how long an order waits for the edges to answer its
// challenge before it asks the CA to check it all the same.
var probeWait = 30 * time.Second

// ProbeEdges returns a Probe that waits until the edge at each of the
// addresses (host:port, its plain HTTP listener) answers a challenge as a
// CA would ask it, or probeWait passed: the CA may ask any node the domain
// leads to. An edge that never answers, say of a node that is down, is
// logged, and the order goes on.
func ProbeEdges(addresses func() []string, log *slog.Logger) func(ctx context.Context, challenge store.Challenge) error {
	client := &http.Client{
		Timeout: 2 * time.Second,
		// The answer comes on the port asked: a redirect is no answer.
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		Transport:     &http.Transport{Proxy: nil, DisableKeepAlives: true},
	}
	return func(ctx context.Context, challenge store.Challenge) error {
		waiting := slices.Clone(addresses())
		deadline := time.Now().Add(probeWait)
		for {
			waiting = slices.DeleteFunc(waiting, func(address string) bool { return answers(ctx, client, address, challenge) })
			if len(waiting) == 0 {
				return nil
			}
			if !time.Now().Before(deadline) {
				log.Warn("edges did not answer an ACME challenge in time; the CA is asked to check it all the same", "domain", challenge.Domain, "edges", strings.Join(waiting, ", "))
				return nil
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(250 * time.Millisecond):
			}
		}
	}
}

// answers reports whether the edge at address answers challenge.
func answers(ctx context.Context, client *http.Client, address string, challenge store.Challenge) bool {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+address+challengePath+challenge.Token, nil)
	if err != nil {
		return false
	}
	request.Host = challenge.Domain
	response, err := client.Do(request)
	if err != nil {
		return false
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 4096))
	return err == nil && response.StatusCode == http.StatusOK && strings.TrimSpace(string(body)) == challenge.KeyAuth
}

// challengePath is where a CA asks for an HTTP-01 challenge's answer.
const challengePath = "/.well-known/acme-challenge/"

// EdgeAddresses are the plain HTTP listeners of the nodes' edges, as the
// leader reaches them: each node's private address, at port, or this
// host's loopback outside a cluster. Nodes that failed over are left out.
func EdgeAddresses(nodes []store.Node, port string) []string {
	var addresses []string
	for _, node := range nodes {
		if node.Failed() {
			continue
		}
		if host, _, err := net.SplitHostPort(node.Address); err == nil {
			addresses = append(addresses, net.JoinHostPort(host, port))
		}
	}
	if len(nodes) == 0 {
		addresses = append(addresses, net.JoinHostPort("127.0.0.1", port))
	}
	return addresses
}
