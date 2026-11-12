package failover

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/PowerPenguini/murder/internal/store"
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
	// Poll is how often the server's status is read until it is off; 1 s
	// if zero. Hetzner allows a project 3600 calls an hour, refilled one a
	// second: a power-off confirmed within 10 s takes about 10 of them.
	Poll time.Duration
}

const hetznerAPI = "https://api.hetzner.cloud/v1"

type hetznerServer struct {
	ID         int64  `json:"id"`
	Name       string `json:"name"`
	Status     string `json:"status"`
	PrivateNet []struct {
		IP string `json:"ip"`
	} `json:"private_net"`
}

// server finds the node's server by name, and checks that it has the
// node's address on its private network: a wrong name in the config must
// never power off, and confirm off, another machine.
func (h *Hetzner) server(ctx context.Context, node store.Node) (hetznerServer, error) {
	name := node.Name
	if server, ok := h.Servers[node.Name]; ok {
		name = server
	}
	var found struct {
		Servers []hetznerServer `json:"servers"`
	}
	if err := h.call(ctx, http.MethodGet, "/servers?name="+url.QueryEscape(name), &found); err != nil {
		return hetznerServer{}, err
	}
	if len(found.Servers) != 1 || found.Servers[0].Name != name {
		return hetznerServer{}, fmt.Errorf("hetzner: no single server named %q for node %s (%d found)", name, node.Name, len(found.Servers))
	}
	server := found.Servers[0]
	host, _, err := net.SplitHostPort(node.Address)
	if err != nil {
		host = node.Address
	}
	if !slices.ContainsFunc(server.PrivateNet, func(network struct {
		IP string `json:"ip"`
	}) bool {
		return network.IP == host
	}) {
		return hetznerServer{}, fmt.Errorf("hetzner: server %s does not have node %s's address %s on a private network: is it the node's server?", name, node.Name, host)
	}
	return server, nil
}

// PowerOff asks for a hard power-off of the node's server unless it is off
// already, and reads its status until it is "off". A failed read is tried
// again until ctx ends: one error of the API does not stop it.
func (h *Hetzner) PowerOff(ctx context.Context, node store.Node) error {
	server, err := h.server(ctx, node)
	if err != nil {
		return err
	}
	path := "/servers/" + strconv.FormatInt(server.ID, 10)
	if server.Status != "off" {
		if err := h.call(ctx, http.MethodPost, path+"/actions/poweroff", nil); err != nil {
			return err
		}
	}
	poll := h.Poll
	if poll == 0 {
		poll = time.Second
	}
	last := server.Status
	for {
		var current struct {
			Server hetznerServer `json:"server"`
		}
		if err := h.call(ctx, http.MethodGet, path, &current); err != nil {
			last = err.Error()
		} else if current.Server.Status == "off" {
			return nil
		} else {
			last = current.Server.Status
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("hetzner: server %s is not confirmed off (last: %s): %w", server.Name, last, ctx.Err())
		case <-time.After(poll):
		}
	}
}

// Off reports whether the node's server is off now.
func (h *Hetzner) Off(ctx context.Context, node store.Node) (bool, error) {
	server, err := h.server(ctx, node)
	if err != nil {
		return false, err
	}
	return server.Status == "off", nil
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
