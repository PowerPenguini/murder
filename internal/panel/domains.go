package panel

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/domains"
	"github.com/PowerPenguini/murder/internal/store"
)

// A tenant's page has a Domains box: its own host names, each with its
// state, the DNS record to create and its certificate. A domain is added
// or removed through a review: what will happen, and the DNS record, are
// shown before anything is stored.

// DomainAgent adds and removes tenants' own domains, for the person
// signed in; the agent's client is one.
type DomainAgent interface {
	AddDomain(ctx context.Context, tenant, name string) (api.DomainAdded, error)
	RemoveDomain(ctx context.Context, tenant, name string) error
	CheckDomain(ctx context.Context, tenant, name string) error
	SetCanonical(ctx context.Context, tenant, host string) error
}

// The agent's client adds and removes domains.
var _ DomainAgent = (*api.Client)(nil)

// domainsView is a tenant's Domains box.
type domainsView struct {
	Tenant api.Tenant
	// Acts says the person may add and remove domains.
	Acts bool
	Rows []domainRow
	// Review is a domain to add or remove, shown before it is; Problem
	// says why the name given cannot be, and Given keeps it as typed.
	Review  *domainReview
	Problem string
	Given   string
	// Full says it has as many domains as a tenant may.
	Full bool
	Max  int
}

// domainRow is one of a tenant's domains.
type domainRow struct {
	api.Domain
	State tag
	// Bad says getting its certificate fails; Next when it is tried again.
	Bad  bool
	Next string
}

// domainReview is a domain to add, or remove, or a host to make the
// tenant's canonical one (Primary; an empty Name for none), as the person
// is asked to confirm it.
type domainReview struct {
	Remove  bool
	Primary bool
	Name    string
	Display string
	Records []domains.Record
	// Token binds the add to the name reviewed.
	Token string
}

// domainState says a domain's state in words.
func domainState(d api.Domain, now time.Time) (tag, bool) {
	switch d.State {
	case store.DomainActive:
		return tag{"Active", "ok"}, false
	case store.DomainVerifying:
		return tag{"Checking its DNS", "busy"}, false
	case store.DomainPending:
		if d.Routed() {
			return tag{"Served, no certificate", "warn"}, false
		}
		return tag{"Waiting for its DNS", "warn"}, false
	case store.DomainIssuing:
		return tag{"Getting its certificate", "busy"}, false
	case store.DomainRenewing:
		if d.Attempts > 0 {
			if !d.NotAfter.IsZero() && now.After(d.NotAfter) {
				return tag{"Certificate expired", "bad"}, true
			}
			return tag{"Renewal failing", "bad"}, true
		}
		return tag{"Renewing", "busy"}, false
	case store.DomainFailed:
		return tag{"Certificate failed", "bad"}, true
	}
	return tag{d.State, "mute"}, false
}

// domainToken binds adding name to tenant to the review that showed it.
func domainToken(tenant, name string) string {
	sum := sha256.Sum256([]byte("domain\x00" + tenant + "\x00" + name))
	return hex.EncodeToString(sum[:])[:32]
}

// baseOf is the base domain a tenant's own name is below.
func baseOf(t api.Tenant) string {
	return strings.TrimPrefix(t.Host, t.Spec.Name+".")
}

// publicOf are the nodes' public addresses, which an apex domain's records
// name.
func publicOf(nodes api.Nodes) []netip.Addr {
	list := make([]store.Node, 0, len(nodes.Nodes))
	for _, node := range nodes.Nodes {
		list = append(list, node.Node)
	}
	return domains.PublicAddresses(list)
}

// domainsOf is a tenant's Domains box, with a review the address asks
// for: ?domain=NAME&do=domain-add or do=domain-remove.
func (s *Server) domainsOf(r *http.Request, t api.Tenant, nodes api.Nodes) *domainsView {
	now := s.now()
	view := &domainsView{Tenant: t, Acts: s.Actions != nil, Max: store.MaxDomains, Full: len(t.Domains) >= store.MaxDomains}
	for _, d := range t.Domains {
		state, bad := domainState(d, now)
		row := domainRow{Domain: d, State: state, Bad: bad}
		if !d.NextAt.IsZero() && d.State != store.DomainActive && d.NextAt.After(now) {
			row.Next = "checked again in " + roughly(d.NextAt.Sub(now))
		}
		view.Rows = append(view.Rows, row)
	}
	query := r.URL.Query()
	given := strings.TrimSpace(query.Get("domain"))
	switch query.Get("do") {
	case "domain-add":
		view.Given = given
		name, err := domains.Normalize(given, baseOf(t))
		if err != nil {
			// The box says the name already: only why.
			view.Problem = "it " + strings.TrimPrefix(err.Error(), domains.ErrName.Error()+": "+strconv.Quote(given)+" ")
			return view
		}
		for _, d := range t.Domains {
			if d.Name == name {
				view.Problem = name + " is one of " + t.Spec.Name + "'s domains already"
				return view
			}
		}
		if view.Full {
			view.Problem = t.Spec.Name + " has " + strconv.Itoa(len(t.Domains)) + " domains, the most a tenant may have"
			return view
		}
		view.Review = &domainReview{Name: name, Display: displayIfOther(name), Records: domains.Records(name, t.Host, publicOf(nodes)), Token: domainToken(t.Spec.Name, name)}
	case "domain-remove":
		for _, d := range t.Domains {
			if d.Name == given {
				view.Review = &domainReview{Remove: true, Name: d.Name, Display: d.Display}
			}
		}
	case "domain-primary":
		if given == "" || given == t.Host {
			view.Review = &domainReview{Primary: true, Name: given}
		}
		for _, d := range t.Domains {
			if d.Name == given {
				view.Review = &domainReview{Primary: true, Name: d.Name, Display: d.Display}
			}
		}
	}
	return view
}

func displayIfOther(name string) string {
	if display := domains.Display(name); display != name {
		return display
	}
	return ""
}

// roughly says a wait in the largest whole unit.
func roughly(d time.Duration) string {
	switch {
	case d < time.Minute:
		return "under a minute"
	case d < time.Hour:
		return strconv.Itoa(int(d/time.Minute)) + " min"
	case d < 48*time.Hour:
		return strconv.Itoa(int(d/time.Hour)) + " h"
	default:
		return strconv.Itoa(int(d/(24*time.Hour))) + " days"
	}
}

// domainAction adds, removes or checks one of a tenant's domains, and
// sends back to its page.
func (s *Server) domainAction(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	agent, ok := s.Actions(s.actor(userOf(r))).(DomainAgent)
	back := "/tenants/" + url.PathEscape(name) + "#domains"
	if !ok {
		flash(w, r, "problem", "this panel does not change domains")
		http.Redirect(w, r, back, http.StatusSeeOther)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	domain := strings.TrimSpace(r.FormValue("domain"))
	var err error
	var done string
	switch r.FormValue("do") {
	case "domain-add":
		// Only what was reviewed is added.
		if r.FormValue("reviewed") != domainToken(name, domain) {
			err = errors.New("review the domain before adding it")
			break
		}
		var added api.DomainAdded
		if added, err = agent.AddDomain(ctx, name, domain); err == nil {
			done = added.Domain.Name + " is added to " + name + ": once its DNS record points here, it is served, and its certificate follows"
		}
	case "domain-remove":
		if !typed(r.FormValue("confirm"), domain) {
			err = errors.New("type the domain to remove it")
			break
		}
		if err = agent.RemoveDomain(ctx, name, domain); err == nil {
			done = domain + " is removed from " + name + ": it is served no more, its certificate dropped"
		}
	case "domain-primary":
		if err = agent.SetCanonical(ctx, name, domain); err == nil {
			done = "every host of " + name + " redirects to " + domain + " once the edges serve it there"
			if domain == "" {
				done = "each host of " + name + " serves it: none redirects"
			}
		}
	case "domain-check":
		if err = agent.CheckDomain(ctx, name, domain); err == nil {
			done = domain + "'s DNS is checked within seconds"
		}
	default:
		err = errors.New("unknown action")
	}
	if err != nil {
		flash(w, r, "problem", err.Error())
	} else {
		flash(w, r, "done", done)
	}
	http.Redirect(w, r, back, http.StatusSeeOther)
}
