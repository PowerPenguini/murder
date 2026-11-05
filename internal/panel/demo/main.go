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
	"time"

	"github.com/PowerPenguini/murder/internal/panel"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:7480", "address to serve on")
	signIn := flag.Bool("signin", false, "sign in with passkeys, at http://localhost:<port>; an invitation for jan is printed")
	name := flag.String("name", "", "a name the panel answers as besides localhost, such as the address it listens on in a private network")
	jobs := flag.String("jobs", "", "a file to keep the jobs' records in across restarts of the demo")
	flag.Parse()
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	cluster := panel.NewDemoCluster()
	server, err := panel.New(cluster, log)
	if err != nil {
		log.Error("panel", "error", err)
		os.Exit(1)
	}
	server.Name = *name
	if *jobs != "" {
		if err := panel.SeedDemoJobs(*jobs, time.Now()); err != nil {
			log.Warn("the jobs' records", "error", err)
		}
		if err := server.Jobs.Persist(*jobs, time.Now()); err != nil {
			log.Warn("the jobs' records", "error", err)
		}
	}
	server.Actions = cluster.As
	server.Cluster = cluster.AsCluster
	server.Apps = cluster.AsApps
	url := "http://" + *listen
	people := panel.NewMemoryAuth()
	access := panel.NewDemoAccess(people)
	access.Seed()
	server.Access = func(string) panel.AccessAgent { return access }
	server.TunnelNetwork = netip.MustParsePrefix("10.77.0.0/24")
	if *signIn {
		_, port, _ := net.SplitHostPort(*listen)
		url = "http://localhost:" + port
		if server.Auth, err = panel.NewAuthAt(people, "localhost", url, log); err != nil {
			log.Error("panel", "error", err)
			os.Exit(1)
		}
		log.Info("an invitation", "url", url+"/enroll#"+people.Invite("ewa"))
	}
	log.Info("the demo panel serves", "url", url, "connection lost", url+"/?demo=offline", "signed out", url+"/?demo=signedout", "a full disk", "POST "+url+"/demo/fill?tenant=NAME")
	if err := http.ListenAndServe(*listen, demoStates(server.Handler(), cluster)); err != nil {
		log.Error("serve", "error", err)
		os.Exit(1)
	}
}

// demoStates lets a page show what it does when the node it follows stops
// answering, or its session ends: with ?demo=offline (or signedout) in its
// address, the page loads, and its following of the cluster then fails so.
//
// POST /demo/fill?tenant=NAME has a tenant's data grow to its disk limit,
// as data grows into a limit set long ago: the demo of a tenant that
// cannot write.
func demoStates(next http.Handler, cluster *panel.DemoCluster) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/demo/fill" && r.Method == http.MethodPost {
			if err := cluster.FillDisk(r.URL.Query().Get("tenant")); err != nil {
				http.Error(w, err.Error(), http.StatusConflict)
				return
			}
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if r.Header.Get("X-Murder-Live") != "" {
			switch r.URL.Query().Get("demo") {
			case "offline":
				w.Header().Set("Content-Type", "text/html; charset=utf-8")
				w.WriteHeader(http.StatusGatewayTimeout)
				_, _ = w.Write([]byte("<!doctype html><title>murder</title><h1>The agent took too long to answer</h1>"))
				return
			case "signedout":
				http.Error(w, "sign in first", http.StatusUnauthorized)
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}
