package api

import (
	"context"
	"net/http"
	"net/url"
	"time"

	"github.com/PowerPenguini/murder/internal/store"
)

// InviteRequest lets a person add a passkey with a token, until Expires.
type InviteRequest struct {
	// Hash is the SHA-256 of the token, in hex; the token itself never
	// reaches the agent.
	Hash    string    `json:"hash"`
	Expires time.Time `json:"expires"`
}

// PasskeyRequest adds a passkey with an invitation or a recovery code.
type PasskeyRequest struct {
	Hash    string        `json:"hash"`
	Passkey store.Passkey `json:"passkey"`
}

// SessionRequest signs a person in.
type SessionRequest struct {
	Hash    string    `json:"hash"`
	User    string    `json:"user"`
	Expires time.Time `json:"expires"`
}

func (s *Server) panelAuthRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/panel/users", rootOnly(func(w http.ResponseWriter, r *http.Request) {
		if handle := r.URL.Query().Get("handle"); handle != "" {
			s.panelAnswer(w, r, func() (any, error) { return s.store.PanelUserByHandle(handle) })
			return
		}
		writeJSON(w, http.StatusOK, s.store.PanelUsers())
	}))
	mux.HandleFunc("GET /v1/panel/users/{name}", rootOnly(func(w http.ResponseWriter, r *http.Request) {
		s.panelAnswer(w, r, func() (any, error) { return s.store.PanelUser(r.PathValue("name")) })
	}))
	mux.HandleFunc("POST /v1/panel/users/{name}/invite", rootOnly(func(w http.ResponseWriter, r *http.Request) {
		var request InviteRequest
		if readJSON(w, r, &request) {
			s.panelAnswer(w, r, func() (any, error) {
				return s.store.InvitePanelUser(r.PathValue("name"), request.Hash, request.Expires, actor(r))
			})
		}
	}))
	mux.HandleFunc("PUT /v1/panel/users/{name}/recovery", rootOnly(func(w http.ResponseWriter, r *http.Request) {
		var hashes []string
		if readJSON(w, r, &hashes) {
			s.panelDone(w, s.store.SetRecoveryCodes(r.PathValue("name"), hashes, actor(r)))
		}
	}))
	mux.HandleFunc("DELETE /v1/panel/users/{name}", rootOnly(func(w http.ResponseWriter, r *http.Request) {
		s.panelDone(w, s.store.RemovePanelUser(r.PathValue("name"), actor(r)))
	}))
	mux.HandleFunc("GET /v1/panel/enrollments/{hash}", rootOnly(func(w http.ResponseWriter, r *http.Request) {
		s.panelAnswer(w, r, func() (any, error) { return s.store.Enrollment(r.PathValue("hash")) })
	}))
	mux.HandleFunc("POST /v1/panel/passkeys", rootOnly(func(w http.ResponseWriter, r *http.Request) {
		var request PasskeyRequest
		if readJSON(w, r, &request) {
			s.panelDone(w, s.store.AddPasskey(request.Hash, request.Passkey))
		}
	}))
	mux.HandleFunc("PUT /v1/panel/passkeys/{id}", rootOnly(func(w http.ResponseWriter, r *http.Request) {
		var data string
		if readJSON(w, r, &data) {
			s.panelDone(w, s.store.UsePasskey(r.PathValue("id"), data))
		}
	}))
	mux.HandleFunc("DELETE /v1/panel/passkeys/{id}", rootOnly(func(w http.ResponseWriter, r *http.Request) {
		s.panelDone(w, s.store.RemovePasskey(r.PathValue("id"), actor(r)))
	}))
	mux.HandleFunc("POST /v1/panel/sessions", rootOnly(func(w http.ResponseWriter, r *http.Request) {
		var request SessionRequest
		if readJSON(w, r, &request) {
			s.panelDone(w, s.store.StartPanelSession(request.Hash, request.User, request.Expires))
		}
	}))
	mux.HandleFunc("GET /v1/panel/sessions/{hash}", rootOnly(func(w http.ResponseWriter, r *http.Request) {
		s.panelAnswer(w, r, func() (any, error) { return s.store.PanelSession(r.PathValue("hash")) })
	}))
	mux.HandleFunc("DELETE /v1/panel/sessions/{hash}", rootOnly(func(w http.ResponseWriter, r *http.Request) {
		s.panelDone(w, s.store.EndPanelSession(r.PathValue("hash")))
	}))
	mux.HandleFunc("GET /v1/panel/secrets/{name}", rootOnly(func(w http.ResponseWriter, r *http.Request) {
		s.panelAnswer(w, r, func() (any, error) { return s.store.PanelSecret(r.PathValue("name")) })
	}))
}

func (s *Server) panelAnswer(w http.ResponseWriter, _ *http.Request, answer func() (any, error)) {
	value, err := answer()
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, value)
}

func (s *Server) panelDone(w http.ResponseWriter, err error) {
	if err != nil {
		writeError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (c *Client) PanelUsers(ctx context.Context) ([]store.PanelUser, error) {
	var users []store.PanelUser
	err := c.do(ctx, http.MethodGet, "/v1/panel/users", nil, &users)
	return users, err
}

func (c *Client) PanelUser(ctx context.Context, name string) (store.PanelUser, error) {
	var user store.PanelUser
	err := c.do(ctx, http.MethodGet, "/v1/panel/users/"+url.PathEscape(name), nil, &user)
	return user, err
}

func (c *Client) PanelUserByHandle(ctx context.Context, handle string) (store.PanelUser, error) {
	var user store.PanelUser
	err := c.do(ctx, http.MethodGet, "/v1/panel/users?handle="+url.QueryEscape(handle), nil, &user)
	return user, err
}

func (c *Client) InvitePanelUser(ctx context.Context, name string, request InviteRequest) (store.PanelUser, error) {
	var user store.PanelUser
	err := c.do(ctx, http.MethodPost, "/v1/panel/users/"+url.PathEscape(name)+"/invite", request, &user)
	return user, err
}

func (c *Client) SetRecoveryCodes(ctx context.Context, name string, hashes []string) error {
	return c.do(ctx, http.MethodPut, "/v1/panel/users/"+url.PathEscape(name)+"/recovery", hashes, nil)
}

func (c *Client) RemovePanelUser(ctx context.Context, name string) error {
	return c.do(ctx, http.MethodDelete, "/v1/panel/users/"+url.PathEscape(name), nil, nil)
}

func (c *Client) Enrollment(ctx context.Context, hash string) (store.Enrollment, error) {
	var e store.Enrollment
	err := c.do(ctx, http.MethodGet, "/v1/panel/enrollments/"+url.PathEscape(hash), nil, &e)
	return e, err
}

func (c *Client) AddPasskey(ctx context.Context, request PasskeyRequest) error {
	return c.do(ctx, http.MethodPost, "/v1/panel/passkeys", request, nil)
}

func (c *Client) UsePasskey(ctx context.Context, id, data string) error {
	return c.do(ctx, http.MethodPut, "/v1/panel/passkeys/"+url.PathEscape(id), data, nil)
}

func (c *Client) RemovePasskey(ctx context.Context, id string) error {
	return c.do(ctx, http.MethodDelete, "/v1/panel/passkeys/"+url.PathEscape(id), nil, nil)
}

func (c *Client) StartPanelSession(ctx context.Context, request SessionRequest) error {
	return c.do(ctx, http.MethodPost, "/v1/panel/sessions", request, nil)
}

func (c *Client) PanelSession(ctx context.Context, hash string) (store.PanelSession, error) {
	var session store.PanelSession
	err := c.do(ctx, http.MethodGet, "/v1/panel/sessions/"+url.PathEscape(hash), nil, &session)
	return session, err
}

func (c *Client) EndPanelSession(ctx context.Context, hash string) error {
	return c.do(ctx, http.MethodDelete, "/v1/panel/sessions/"+url.PathEscape(hash), nil, nil)
}

func (c *Client) PanelSecret(ctx context.Context, name string) (string, error) {
	var value string
	err := c.do(ctx, http.MethodGet, "/v1/panel/secrets/"+url.PathEscape(name), nil, &value)
	return value, err
}

// WithActor is a copy of the client acting for the person named.
func (c *Client) WithActor(actor string) *Client {
	copy := *c
	copy.Actor = actor
	return &copy
}
