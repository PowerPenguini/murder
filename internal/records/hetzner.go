package records

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/netip"
	"slices"
	"strconv"
	"strings"
	"time"
)

// Hetzner writes the records through the Hetzner Cloud DNS API. The
// records of a name and type are replaced in one action (set_records),
// so the name never resolves to nothing meanwhile.
type Hetzner struct {
	Token string
	// Zone is the zone's name; Name the records' name within it.
	Zone, Name string
	TTL        time.Duration
	// Endpoint is the API's base URL; Hetzner's if empty.
	Endpoint string
	Client   *http.Client
	// Poll is how often a running action is read; 1 s if zero.
	Poll time.Duration
}

const hetznerAPI = "https://api.hetzner.cloud/v1"

type hetznerRecord struct {
	Value string `json:"value"`
}

type hetznerAction struct {
	ID     int64  `json:"id"`
	Status string `json:"status"`
	Error  *struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func (h *Hetzner) Publish(ctx context.Context, addresses []netip.Addr) error {
	v4, v6 := families(addresses)
	if len(v4) > 0 {
		if err := h.set(ctx, "A", v4); err != nil {
			return err
		}
	}
	if len(v6) > 0 {
		if err := h.set(ctx, "AAAA", v6); err != nil {
			return err
		}
	}
	return nil
}

// set makes the records of type name values: created if there are none,
// replaced if they differ, untouched if they match.
func (h *Hetzner) set(ctx context.Context, kind string, values []string) error {
	path := "/zones/" + h.Zone + "/rrsets/" + h.Name + "/" + kind
	var current struct {
		RRSet struct {
			Records []hetznerRecord `json:"records"`
			TTL     *int            `json:"ttl"`
		} `json:"rrset"`
	}
	ttl := h.TTL
	if ttl == 0 {
		ttl = TTL
	}
	seconds := int(ttl.Seconds())
	status, err := h.call(ctx, http.MethodGet, path, nil, &current)
	records := make([]hetznerRecord, len(values))
	for i, value := range values {
		records[i] = hetznerRecord{Value: value}
	}
	var action struct {
		Action hetznerAction `json:"action"`
	}
	switch {
	case status == http.StatusNotFound:
		body := map[string]any{"name": h.Name, "type": kind, "ttl": seconds, "records": records}
		if _, err := h.call(ctx, http.MethodPost, "/zones/"+h.Zone+"/rrsets", body, &action); err != nil {
			return err
		}
	case err != nil:
		return err
	default:
		have := make([]string, 0, len(current.RRSet.Records))
		for _, record := range current.RRSet.Records {
			have = append(have, record.Value)
		}
		slices.Sort(have)
		if current.RRSet.TTL == nil || *current.RRSet.TTL != seconds {
			var changed struct {
				Action hetznerAction `json:"action"`
			}
			if _, err := h.call(ctx, http.MethodPost, path+"/actions/change_ttl", map[string]any{"ttl": seconds}, &changed); err != nil {
				return err
			}
			if err := h.wait(ctx, changed.Action); err != nil {
				return err
			}
		}
		if slices.Equal(have, values) {
			return nil
		}
		if _, err := h.call(ctx, http.MethodPost, path+"/actions/set_records", map[string]any{"records": records}, &action); err != nil {
			return err
		}
	}
	return h.wait(ctx, action.Action)
}

// wait reads the action until it has succeeded or failed.
func (h *Hetzner) wait(ctx context.Context, action hetznerAction) error {
	poll := h.Poll
	if poll == 0 {
		poll = time.Second
	}
	for {
		switch action.Status {
		case "success":
			return nil
		case "error":
			if action.Error != nil {
				return fmt.Errorf("hetzner: DNS action %d failed: %s %s", action.ID, action.Error.Code, action.Error.Message)
			}
			return fmt.Errorf("hetzner: DNS action %d failed", action.ID)
		}
		if action.ID == 0 {
			return nil // nothing to wait for
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("hetzner: DNS action %d not done (%s): %w", action.ID, action.Status, ctx.Err())
		case <-time.After(poll):
		}
		var current struct {
			Action hetznerAction `json:"action"`
		}
		if _, err := h.call(ctx, http.MethodGet, "/actions/"+strconv.FormatInt(action.ID, 10), nil, &current); err != nil {
			continue // read again
		}
		action = current.Action
	}
}

func (h *Hetzner) call(ctx context.Context, method, path string, in, out any) (int, error) {
	endpoint := h.Endpoint
	if endpoint == "" {
		endpoint = hetznerAPI
	}
	var body io.Reader
	if in != nil {
		data, err := json.Marshal(in)
		if err != nil {
			return 0, err
		}
		body = bytes.NewReader(data)
	}
	request, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(endpoint, "/")+path, body)
	if err != nil {
		return 0, err
	}
	request.Header.Set("Authorization", "Bearer "+h.Token)
	if in != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	client := h.Client
	if client == nil {
		client = &http.Client{Timeout: 20 * time.Second}
	}
	response, err := client.Do(request)
	if err != nil {
		return 0, fmt.Errorf("hetzner: %w", err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return response.StatusCode, fmt.Errorf("hetzner: %w", err)
	}
	if response.StatusCode < 200 || response.StatusCode > 299 {
		var failure struct {
			Error struct {
				Code    string `json:"code"`
				Message string `json:"message"`
			} `json:"error"`
		}
		_ = json.Unmarshal(data, &failure)
		return response.StatusCode, fmt.Errorf("hetzner: %s %s: %d %s %s", method, path, response.StatusCode, failure.Error.Code, failure.Error.Message)
	}
	if out == nil {
		return response.StatusCode, nil
	}
	return response.StatusCode, json.Unmarshal(data, out)
}
