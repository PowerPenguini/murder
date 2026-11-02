// Command demo serves the panel over a made-up cluster, on loopback only,
// for trying its pages: go run ./internal/panel/demo.
package main

import (
	"flag"
	"log/slog"
	"net/http"
	"os"

	"github.com/PowerPenguini/murder/internal/panel"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:7480", "address to serve on")
	flag.Parse()
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	server, err := panel.New(panel.Demo{}, log)
	if err != nil {
		log.Error("panel", "error", err)
		os.Exit(1)
	}
	log.Info("the demo panel serves", "url", "http://"+*listen)
	if err := http.ListenAndServe(*listen, server.Handler()); err != nil {
		log.Error("serve", "error", err)
		os.Exit(1)
	}
}
