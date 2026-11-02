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

// SessionRequest signs a person in: the session as the state keeps it.
type SessionRequest = store.PanelSession

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

// PanelSession is a session while it lasts; fresh checks it as the
// cluster has it, not as this node last heard.
func (c *Client) PanelSession(ctx context.Context, hash string, fresh bool) (store.PanelSession, error) {
	var session store.PanelSession
	path := "/v1/panel/sessions/" + url.PathEscape(hash)
	if fresh {
		path += "?fresh=1"
	}
	err := c.do(ctx, http.MethodGet, path, nil, &session)
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
