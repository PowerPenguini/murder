// Command gus is Murder's plugin for GUS's REGON database, BIR1
// (Baza Internetowa REGON): companies and their local units, found by
// NIP, REGON or KRS, and their reports. Murder's releases publish it as
// ghcr.io/powerpenguini/murder-gus (see plugins/Dockerfile and
// docs/plugins.md).
//
// BIR1 is SOAP 1.2: a client logs in with the key (Zaloguj), and sends the
// session it is given in the sid header of its next calls. A tenant's
// client logs in with any key: the plugin writes the login, and the edge
// puts the operator's key in it. The tenant is given a session of the
// plugin's in place of BIR1's (see sessions.go). The plugin writes every call again, from
// what it read of the tenant's, with the service's own address in it, as
// the service takes no other: only BIR1's operations, of parameters it
// checked, go on.
package main

import (
	"bytes"
	"encoding/xml"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"

	"github.com/PowerPenguini/murder/pluginsdk"
)

func main() {
	pluginsdk.Register(manifest, forward)
	pluginsdk.Run()
}

// The metrics it counts, beyond the requests Murder counts: entities
// searched for, by what they were searched by, and reports, by their name.
const (
	metricSearches = "searches"
	metricReports  = "reports"
)

var manifest = pluginsdk.Manifest{
	Name:    "gus",
	Version: "1.0.0",
	Hosts:   []string{"wyszukiwarkaregon.stat.gov.pl"},
	Key:     pluginsdk.Key{Body: true},
	Metrics: []string{metricSearches, metricReports},
	// A call is a short envelope, read to be written again.
	RequestBody:      1 << 20,
	RequestBodyTypes: []string{"application/soap+xml", "text/xml", "application/xml"},
	UI: &pluginsdk.UI{
		Title:       "GUS REGON",
		Description: "GUS's REGON database (BIR1): companies found by NIP, REGON or KRS, and their reports, with the operator's key; searches counted by what they search by, reports by name.",
		Detail:      "kind",
		Metrics: []pluginsdk.MetricUI{
			{Name: metricSearches, Label: "Entities searched for", Unit: "entities"},
			{Name: metricReports, Label: "Reports", Unit: "reports"},
			{Name: "requests", Label: "Requests", Unit: "requests"},
		},
	},
}

// service is the path of BIR1's service on its host.
const service = "/wsBIR/UslugaBIRzewnPubl.svc"

// The namespaces of BIR1's calls, and of SOAP's envelopes.
const (
	nsPublic   = "http://CIS/BIR/PUBL/2014/07"
	nsBIR      = "http://CIS/BIR/2014/07"
	nsData     = "http://CIS/BIR/PUBL/2014/07/DataContract"
	nsSOAP12   = "http://www.w3.org/2003/05/soap-envelope"
	nsSOAP11   = "http://schemas.xmlsoap.org/soap/envelope/"
	nsAddress  = "http://www.w3.org/2005/08/addressing"
	actionBase = "http://CIS/BIR/PUBL/2014/07/IUslugaBIRzewnPubl/"
	getValue   = "http://CIS/BIR/2014/07/IUslugaBIR/GetValue"
)

// call is a call of BIR1's, as a tenant's envelope says it: an operation,
// and its parameters.
type call struct {
	Key     string  `xml:"pKluczUzytkownika"`
	Search  *search `xml:"pParametryWyszukiwania"`
	Regon   string  `xml:"pRegon"`
	Report  string  `xml:"pNazwaRaportu"`
	Date    string  `xml:"pDataRaportu"`
	Param   string  `xml:"pNazwaParametru"`
	Session string  `xml:"pIdentyfikatorSesji"`
}

// search is what an entity is searched by: one of them.
type search struct {
	Krs        string `xml:"Krs"`
	Krsy       string `xml:"Krsy"`
	Nip        string `xml:"Nip"`
	Nipy       string `xml:"Nipy"`
	Regon      string `xml:"Regon"`
	Regony14zn string `xml:"Regony14zn"`
	Regony9zn  string `xml:"Regony9zn"`
}

// reports are BIR1's reports, after BIR11 or BIR12: a count of a report
// is kept by its name when it is one of them, and as other when not, so
// tenants cannot make up details without end (BIR1 answers a name it does
// not know with success, and an error inside).
var reports = []string{
	"OsFizycznaDaneOgolne", "OsFizycznaDzialalnoscCeidg", "OsFizycznaDzialalnoscRolnicza", "OsFizycznaDzialalnoscPozostala",
	"OsFizycznaDzialalnoscSkreslonaDo20141108", "OsFizycznaPkd", "OsFizycznaListaJednLokalnych", "JednLokalnaOsFizycznej",
	"JednLokalnaOsFizycznejPkd", "OsPrawna", "OsPrawnaPkd", "OsPrawnaListaJednLokalnych", "JednLokalnaOsPrawnej",
	"JednLokalnaOsPrawnejPkd", "OsPrawnaSpCywilnaWspolnicy", "TypPodmiotu",
	"NowePodmiotyPrawneOrazDzialalnosciOsFizycznych", "AktualizowanePodmiotyPrawneOrazDzialalnosciOsFizycznych",
	"SkreslonePodmiotyPrawneOrazDzialalnosciOsFizycznych", "NoweJednostkiLokalne", "AktualizowaneJednostkiLokalne",
	"SkresloneJednostkiLokalne",
}

// reportDetail is what a report is counted by.
func reportDetail(name string) string {
	for _, prefix := range []string{"BIR11", "BIR12"} {
		if rest, ok := strings.CutPrefix(name, prefix); ok && slices.Contains(reports, rest) {
			return name
		}
	}
	return "other"
}

var (
	reportName = regexp.MustCompile(`^[A-Za-z0-9]{1,80}$`)
	reportDate = regexp.MustCompile(`^[0-9]{4}-[0-9]{2}-[0-9]{2}$`)
	paramName  = regexp.MustCompile(`^[A-Za-z]{1,40}$`)
	sessionID  = regexp.MustCompile(`^[A-Za-z0-9]{1,64}$`)
	digits     = regexp.MustCompile(`^[0-9]+$`)
	regon      = regexp.MustCompile(`^([0-9]{9}|[0-9]{14})$`)
)

func forward(r *pluginsdk.Request) (*pluginsdk.Forward, error) {
	if len(r.Hosts) == 0 || strings.HasPrefix(r.Hosts[0], "*.") {
		return nil, pluginsdk.Refuse(500, "the plugin's first host is BIR1's: wyszukiwarkaregon.stat.gov.pl, or wyszukiwarkaregontest.stat.gov.pl to test")
	}
	address := "https://" + r.Hosts[0] + service
	path := strings.TrimSuffix(r.Path, "/")
	if r.Method == "GET" {
		// The service's description, which clients may read.
		if path == service && (r.Query == "wsdl" || r.Query == "singleWsdl" || strings.HasPrefix(r.Query, "xsd=")) ||
			strings.HasPrefix(path, "/wsBIR/wsdl/") && strings.HasSuffix(path, ".wsdl") && !strings.Contains(path, "..") {
			to := &pluginsdk.Forward{URL: "https://" + r.Hosts[0] + path, Detail: "description"}
			if r.Query != "" {
				to.URL += "?" + r.Query
			}
			return to, nil
		}
	}
	if r.Method != "POST" || path != service {
		return nil, pluginsdk.Refuse(404, "plugin %s passes on BIR1's calls, POST %s, and its description", r.Plugin, service)
	}
	if r.Body == nil {
		return nil, pluginsdk.Refuse(415, "a call to BIR1 is a SOAP envelope, of type application/soap+xml")
	}
	operation, c, err := read(r.Body)
	if err != nil {
		return nil, pluginsdk.Refuse(400, "%v", err)
	}
	// The tenant's session is the plugin's: BIR1's goes in its place.
	sid, session := known.find(r.Tenant, r.Get("sid"))
	if operation == "Wyloguj" {
		if real, ok := known.find(r.Tenant, c.Session); ok {
			known.close(r.Tenant, c.Session)
			c.Session = real
		}
	}
	body, m, err := write(operation, c, r.KeyMark)
	if err != nil {
		return nil, pluginsdk.Refuse(400, "%v", err)
	}
	if operation == "Zaloguj" {
		m = &login{tenant: r.Tenant}
	}
	action := actionBase + operation
	if operation == "GetValue" {
		action = getValue
	}
	to := &pluginsdk.Forward{
		URL:    address,
		Body:   envelope(address, action, body),
		Header: map[string][]string{"Content-Type": {"application/soap+xml; charset=utf-8"}},
		Detail: operation,
		Meter:  m,
	}
	if session {
		to.Header["Sid"] = []string{sid}
	} else {
		// None, or one the plugin does not know: BIR1 answers as for a
		// session that ran out.
		to.Drop = []string{"Sid"}
	}
	return to, nil
}

// read reads a tenant's envelope, of SOAP 1.2, as BIR1 speaks: the
// operation its body calls, and the call's parameters.
func read(data []byte) (string, call, error) {
	decoder := xml.NewDecoder(bytes.NewReader(data))
	inBody := false
	for {
		token, err := decoder.Token()
		if err != nil {
			return "", call{}, errors.New("the request is no SOAP envelope with a body that calls one of BIR1's operations")
		}
		start, ok := token.(xml.StartElement)
		if !ok {
			continue
		}
		switch {
		case start.Name.Space == nsSOAP11:
			return "", call{}, errors.New("BIR1 speaks SOAP 1.2, of type application/soap+xml: its answers are of 1.2 too")
		case !inBody && start.Name.Space == nsSOAP12 && start.Name.Local == "Body":
			inBody = true
		case !inBody:
			continue
		default:
			operation := start.Name.Local
			want := nsPublic
			if operation == "GetValue" {
				want = nsBIR
			}
			if start.Name.Space != want {
				return "", call{}, fmt.Errorf("the body calls %s of %q: BIR1's operations are of %s", operation, start.Name.Space, nsPublic)
			}
			var c call
			if err := decoder.DecodeElement(&c, &start); err != nil {
				return "", call{}, fmt.Errorf("the call to %s cannot be read: %v", operation, err)
			}
			return operation, c, nil
		}
	}
}

// write writes a call again, from its parameters checked, and the meter
// that counts what it used: the key goes in a login where the edge puts
// it, at mark.
func write(operation string, c call, mark string) (string, pluginsdk.Meter, error) {
	element := func(name, value string) string {
		var escaped bytes.Buffer
		_ = xml.EscapeText(&escaped, []byte(value))
		return "<ns:" + name + ">" + escaped.String() + "</ns:" + name + ">"
	}
	switch operation {
	case "Zaloguj":
		if mark == "" {
			return "", nil, errors.New("the edge gave no mark for the key")
		}
		return "<ns:Zaloguj>" + element("pKluczUzytkownika", mark) + "</ns:Zaloguj>", nil, nil
	case "Wyloguj":
		if !sessionID.MatchString(c.Session) {
			return "", nil, errors.New("Wyloguj takes the session's identifier, pIdentyfikatorSesji")
		}
		return "<ns:Wyloguj>" + element("pIdentyfikatorSesji", c.Session) + "</ns:Wyloguj>", nil, nil
	case "GetValue":
		if !paramName.MatchString(c.Param) {
			return "", nil, errors.New("GetValue takes a parameter's name, pNazwaParametru, as StatusSesji")
		}
		return `<ns:GetValue xmlns:ns="` + nsBIR + `">` + element("pNazwaParametru", c.Param) + "</ns:GetValue>", nil, nil
	case "DaneSzukajPodmioty":
		if c.Search == nil {
			return "", nil, errors.New("DaneSzukajPodmioty takes pParametryWyszukiwania")
		}
		name, value, kind, count, err := c.Search.one()
		if err != nil {
			return "", nil, err
		}
		return "<ns:DaneSzukajPodmioty><ns:pParametryWyszukiwania><dat:" + name + ">" + value + "</dat:" + name + "></ns:pParametryWyszukiwania></ns:DaneSzukajPodmioty>",
			&meter{metric: metricSearches, detail: kind, amount: count}, nil
	case "DanePobierzPelnyRaport":
		if !regon.MatchString(c.Regon) || !reportName.MatchString(c.Report) {
			return "", nil, errors.New("DanePobierzPelnyRaport takes a REGON of 9 or 14 digits, pRegon, and a report's name, pNazwaRaportu")
		}
		return "<ns:DanePobierzPelnyRaport>" + element("pRegon", c.Regon) + element("pNazwaRaportu", c.Report) + "</ns:DanePobierzPelnyRaport>",
			&meter{metric: metricReports, detail: reportDetail(c.Report), amount: 1}, nil
	case "DanePobierzRaportZbiorczy":
		if !reportDate.MatchString(c.Date) || !reportName.MatchString(c.Report) {
			return "", nil, errors.New("DanePobierzRaportZbiorczy takes a day, pDataRaportu, as 2026-11-20, and a report's name, pNazwaRaportu")
		}
		return "<ns:DanePobierzRaportZbiorczy>" + element("pDataRaportu", c.Date) + element("pNazwaRaportu", c.Report) + "</ns:DanePobierzRaportZbiorczy>",
			&meter{metric: metricReports, detail: reportDetail(c.Report), amount: 1}, nil
	}
	return "", nil, fmt.Errorf("BIR1 has no operation %s that the plugin passes on: Zaloguj, Wyloguj, GetValue, DaneSzukajPodmioty, DanePobierzPelnyRaport, DanePobierzRaportZbiorczy", operation)
}

// one is the parameter a search is by, its value, the kind it is counted
// as, and how many entities it searches for: one, or as many as a list
// names, its numbers whole or apart.
func (s search) one() (name, value, kind string, count int64, err error) {
	set := 0
	for _, p := range []struct {
		name, value, kind string
		lengths           []int
		list              bool
	}{
		{"Nip", s.Nip, "nip", []int{10}, false},
		{"Nipy", s.Nipy, "nip", []int{10}, true},
		{"Regon", s.Regon, "regon", []int{9, 14}, false},
		{"Regony9zn", s.Regony9zn, "regon", []int{9}, true},
		{"Regony14zn", s.Regony14zn, "regon", []int{14}, true},
		{"Krs", s.Krs, "krs", []int{10}, false},
		{"Krsy", s.Krsy, "krs", []int{10}, true},
	} {
		given := strings.TrimSpace(p.value)
		if given == "" {
			continue
		}
		set++
		n, ok := numbers(given, p.lengths, p.list)
		if !ok {
			return "", "", "", 0, fmt.Errorf("%s must be numbers of %v digits", p.name, p.lengths)
		}
		// Digits and separators only: nothing to escape.
		name, value, kind, count = p.name, given, p.kind, n
	}
	if set != 1 {
		return "", "", "", 0, errors.New("a search is by one of Nip, Nipy, Regon, Regony9zn, Regony14zn, Krs and Krsy")
	}
	return name, value, kind, count, nil
}

// numbers counts the numbers of a length in value: one, or for a list, as
// many as it names, apart or written one after another.
func numbers(value string, lengths []int, list bool) (int64, bool) {
	if !list {
		for _, length := range lengths {
			if len(value) == length && digits.MatchString(value) {
				return 1, true
			}
		}
		return 0, false
	}
	count := int64(0)
	for _, part := range strings.FieldsFunc(value, func(r rune) bool { return r < '0' || r > '9' }) {
		if len(part)%lengths[0] != 0 {
			return 0, false
		}
		count += int64(len(part) / lengths[0])
	}
	for _, r := range value {
		if !(r >= '0' && r <= '9' || strings.ContainsRune(",; \t\r\n", r)) {
			return 0, false
		}
	}
	return count, count > 0
}

// envelope is a SOAP 1.2 envelope of a call to the service at address.
func envelope(address, action, body string) []byte {
	return []byte(`<soap:Envelope xmlns:soap="` + nsSOAP12 + `" xmlns:ns="` + nsPublic + `" xmlns:dat="` + nsData + `">` +
		`<soap:Header xmlns:wsa="` + nsAddress + `"><wsa:To>` + address + `</wsa:To><wsa:Action>` + action + `</wsa:Action></soap:Header>` +
		`<soap:Body>` + body + `</soap:Body></soap:Envelope>`)
}

// meter counts a search or a report BIR1 answered.
type meter struct {
	metric, detail string
	amount         int64
}

func (m *meter) Response(r *pluginsdk.Response, c *pluginsdk.Counter) bool {
	if r.Status == 200 {
		c.Count(m.metric, m.detail, m.amount)
	}
	return false
}

func (m *meter) Body([]byte, *pluginsdk.Counter) bool { return false }

func (m *meter) End(bool, *pluginsdk.Counter) {}
