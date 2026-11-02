// Command demo serves the panel over a made-up cluster, on loopback only,
// for trying its pages: go run ./internal/panel/demo.
package main

import (
	"flag"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"os"

	"github.com/PowerPenguini/murder/internal/panel"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:7480", "address to serve on")
	signIn := flag.Bool("signin", false, "sign in with passkeys, at http://localhost:<port>; an invitation for jan is printed")
	flag.Parse()
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	cluster := panel.NewDemoCluster()
	server, err := panel.New(cluster, log)
	if err != nil {
		log.Error("panel", "error", err)
		os.Exit(1)
	}
	server.Actions = cluster.As
	server.Cluster = cluster.AsCluster
	url := "http://" + *listen
	if *signIn {
		_, port, _ := net.SplitHostPort(*listen)
		people := panel.NewMemoryAuth()
		url = "http://localhost:" + port
		if server.Auth, err = panel.NewAuthAt(people, "localhost", url, log); err != nil {
			log.Error("panel", "error", err)
			os.Exit(1)
		}
		access := panel.NewDemoAccess(people)
		server.Access = func(string) panel.AccessAgent { return access }
		server.TunnelNetwork = netip.MustParsePrefix("10.77.0.0/24")
		log.Info("an invitation", "url", url+"/enroll#"+people.Invite("jan"))
	}
	log.Info("the demo panel serves", "url", url)
	if err := http.ListenAndServe(*listen, server.Handler()); err != nil {
		log.Error("serve", "error", err)
		os.Exit(1)
	}
}
