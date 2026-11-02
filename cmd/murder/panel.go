package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/config"
	"github.com/PowerPenguini/murder/internal/panel"
)

// runPanel serves the administration panel, asking the local agent.
func runPanel(args []string) error {
	flags := flag.NewFlagSet("panel", flag.ContinueOnError)
	configPath := flags.String("config", defaultConfig, "agent configuration file")
	if err := flags.Parse(args); err != nil {
		return err
	}
	cfg, err := config.Load(*configPath)
	if err != nil {
		return err
	}
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("process", "panel")
	client := api.NewClient(cfg.Socket)
	client.Actor = "murder-panel"
	server, err := panel.New(client, log)
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", cfg.Panel.Listen)
	if err != nil {
		return err
	}
	httpServer := &http.Server{Handler: server.Handler(), ReadHeaderTimeout: 10 * time.Second}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = httpServer.Shutdown(shutdown)
	}()
	log.Info("the panel serves", "address", listener.Addr().String())
	if err := httpServer.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
