package failover

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Hetzner powers servers off through the Hetzner Cloud API.
type Hetzner struct {
	// Token is a read and write token of the project the servers are in.
	Token string
	// Servers names each node's server; a node not in it is the server of
	// its own name.
	Servers map[string]string
	// Endpoint is the API's base URL; Hetzner's if empty.
	Endpoint string
	Client   *http.Client
	// Poll is how often the server's status is read until it is off; 2 s
	// if zero.
	Poll time.Duration
}

const hetznerAPI = "https://api.hetzner.cloud/v1"

type hetznerServer struct {
	ID     int64  `json:"id"`
	Name   string `json:"name"`
	Status string `json:"status"`
}

// PowerOff finds the node's server by name, asks for a hard power-off
// unless it is off already, and reads its status until it is "off".
func (h *Hetzner) PowerOff(ctx context.Context, node string) error {
	name := node
	if server, ok := h.Servers[node]; ok {
		name = server
	}
	var found struct {
		Servers []hetznerServer `json:"servers"`
	}
	if err := h.call(ctx, http.MethodGet, "/servers?name="+url.QueryEscape(name), &found); err != nil {
		return err
	}
	if len(found.Servers) != 1 || found.Servers[0].Name != name {
		return fmt.Errorf("hetzner: no single server named %q for node %s (%d found)", name, node, len(found.Servers))
	}
	server := found.Servers[0]
	path := "/servers/" + strconv.FormatInt(server.ID, 10)
	if server.Status != "off" {
		if err := h.call(ctx, http.MethodPost, path+"/actions/poweroff", nil); err != nil {
			return err
		}
	}
	poll := h.Poll
	if poll == 0 {
		poll = 2 * time.Second
	}
	for {
		var current struct {
			Server hetznerServer `json:"server"`
		}
		if err := h.call(ctx, http.MethodGet, path, &current); err != nil {
			return err
		}
		if current.Server.Status == "off" {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("hetzner: server %s is %s, not off: %w", name, current.Server.Status, ctx.Err())
		case <-time.After(poll):
		}
	}
}

func (h *Hetzner) call(ctx context.Context, method, path string, out any) error {
	endpoint := h.Endpoint
	if endpoint == "" {
		endpoint = hetznerAPI
	}
	request, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(endpoint, "/")+path, nil)
	if err != nil {
		return err
	}
	request.Header.Set("Authorization", "Bearer "+h.Token)
	client := h.Client
	if client == nil {
		client = &http.Client{Timeout: 20 * time.Second}
	}
	response, err := client.Do(request)
	if err != nil {
		return fmt.Errorf("hetzner: %w", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return fmt.Errorf("hetzner: %w", err)
	}
	if response.StatusCode < 200 || response.StatusCode > 299 {
		var failure struct {
			Error struct {
				Code    string `json:"code"`
				Message string `json:"message"`
			} `json:"error"`
		}
		_ = json.Unmarshal(body, &failure)
		return fmt.Errorf("hetzner: %s %s: %d %s %s", method, strings.SplitN(path, "?", 2)[0], response.StatusCode, failure.Error.Code, failure.Error.Message)
	}
	if out == nil {
		return nil
	}
	return json.Unmarshal(body, out)
}
