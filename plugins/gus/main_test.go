package main

import (
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/PowerPenguini/murder/pluginsdk"
)

func soap12(body string) []byte {
	return []byte(`<?xml version="1.0"?><soap:Envelope xmlns:soap="http://www.w3.org/2003/05/soap-envelope" xmlns:ns="http://CIS/BIR/PUBL/2014/07" xmlns:dat="http://CIS/BIR/PUBL/2014/07/DataContract">` +
		`<soap:Header xmlns:wsa="http://www.w3.org/2005/08/addressing"><wsa:To>http://10.240.0.1:4180/gus/wsBIR/UslugaBIRzewnPubl.svc</wsa:To></soap:Header><soap:Body>` + body + `</soap:Body></soap:Envelope>`)
}

func request(body []byte) *pluginsdk.Request {
	return &pluginsdk.Request{Plugin: "gus", Method: "POST", Path: service, Hosts: []string{"wyszukiwarkaregontest.stat.gov.pl"}, Body: body, KeyMark: "murder-key-0123"}
}

// A login is written again with the mark where the key goes, and the
// service's own address: the tenant's key, and its address, go nowhere.
func TestALoginGetsTheKeysMarkAndTheServicesAddress(t *testing.T) {
	to, err := forward(request(soap12(`<ns:Zaloguj><ns:pKluczUzytkownika>tenants-own</ns:pKluczUzytkownika></ns:Zaloguj>`)))
	if err != nil {
		t.Fatal(err)
	}
	body := string(to.Body)
	if _, replaces := to.Meter.(pluginsdk.Replacer); !replaces {
		t.Fatal("a login's answer is not replaced")
	}
	if to.URL != "https://wyszukiwarkaregontest.stat.gov.pl/wsBIR/UslugaBIRzewnPubl.svc" || to.Detail != "Zaloguj" ||
		!strings.Contains(body, "<ns:pKluczUzytkownika>murder-key-0123</ns:pKluczUzytkownika>") || strings.Contains(body, "tenants-own") ||
		!strings.Contains(body, "<wsa:To>https://wyszukiwarkaregontest.stat.gov.pl/wsBIR/UslugaBIRzewnPubl.svc</wsa:To>") ||
		!strings.Contains(body, "<wsa:Action>http://CIS/BIR/PUBL/2014/07/IUslugaBIRzewnPubl/Zaloguj</wsa:Action>") ||
		to.Header["Content-Type"][0] != "application/soap+xml; charset=utf-8" {
		t.Fatalf("%+v\n%s", to, body)
	}
}

// A search is counted by what it searches by, an entity for each number;
// a report by its name, if it is one of BIR1's.
func TestASearchIsCountedByWhatItSearchesFor(t *testing.T) {
	for given, want := range map[string]struct {
		kind  string
		count int64
	}{
		`<dat:Nip>5261040828</dat:Nip>`:                         {"nip", 1},
		`<dat:Nipy>5261040828,5261040829;5261040830</dat:Nipy>`: {"nip", 3},
		`<dat:Nipy>52610408285261040829</dat:Nipy>`:             {"nip", 2},
		`<dat:Regon>000331501</dat:Regon>`:                      {"regon", 1},
		`<dat:Regon>00033150100000</dat:Regon>`:                 {"regon", 1},
		`<dat:Regony14zn>00033150100000</dat:Regony14zn>`:       {"regon", 1},
		`<dat:Krs>0000012345</dat:Krs>`:                         {"krs", 1},
	} {
		to, err := forward(request(soap12(`<ns:DaneSzukajPodmioty><ns:pParametryWyszukiwania>` + given + `</ns:pParametryWyszukiwania></ns:DaneSzukajPodmioty>`)))
		if err != nil {
			t.Fatalf("%s: %v", given, err)
		}
		m := to.Meter.(*meter)
		if m.metric != metricSearches || m.detail != want.kind || m.amount != want.count {
			t.Errorf("%s: %+v", given, m)
		}
	}
	for name, want := range map[string]string{"BIR11OsPrawna": "BIR11OsPrawna", "BIR12OsPrawnaPkd": "BIR12OsPrawnaPkd", "Wymyslony123": "other", "BIR13OsPrawna": "other"} {
		to, err := forward(request(soap12(`<ns:DanePobierzPelnyRaport><ns:pRegon>000331501</ns:pRegon><ns:pNazwaRaportu>` + name + `</ns:pNazwaRaportu></ns:DanePobierzPelnyRaport>`)))
		if err != nil {
			t.Fatal(err)
		}
		if m := to.Meter.(*meter); m.metric != metricReports || m.detail != want {
			t.Errorf("%s: %+v", name, m)
		}
	}
}

// What is not one of BIR1's calls, of parameters it takes, goes nowhere.
func TestWhatIsNotACallOfBIR1sIsRefused(t *testing.T) {
	for name, body := range map[string][]byte{
		"no envelope":           []byte(`{"nip":"5261040828"}`),
		"another namespace":     soap12(`<x:DaneSzukajPodmioty xmlns:x="http://evil.example"><x:pParametryWyszukiwania><dat:Nip>5261040828</dat:Nip></x:pParametryWyszukiwania></x:DaneSzukajPodmioty>`),
		"an operation unknown":  soap12(`<ns:UsunWszystko/>`),
		"SOAP 1.1":              []byte(`<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/"><s:Body><Zaloguj xmlns="http://CIS/BIR/PUBL/2014/07"/></s:Body></s:Envelope>`),
		"a NIP not ten digits":  soap12(`<ns:DaneSzukajPodmioty><ns:pParametryWyszukiwania><dat:Nip>526104082</dat:Nip></ns:pParametryWyszukiwania></ns:DaneSzukajPodmioty>`),
		"markup in a NIP":       soap12(`<ns:DaneSzukajPodmioty><ns:pParametryWyszukiwania><dat:Nip>5261040828&lt;/dat:Nip&gt;</dat:Nip></ns:pParametryWyszukiwania></ns:DaneSzukajPodmioty>`),
		"two searches":          soap12(`<ns:DaneSzukajPodmioty><ns:pParametryWyszukiwania><dat:Nip>5261040828</dat:Nip><dat:Regon>000331501</dat:Regon></ns:pParametryWyszukiwania></ns:DaneSzukajPodmioty>`),
		"no search":             soap12(`<ns:DaneSzukajPodmioty/>`),
		"a report without name": soap12(`<ns:DanePobierzPelnyRaport><ns:pRegon>000331501</ns:pRegon></ns:DanePobierzPelnyRaport>`),
		"a report of a day":     soap12(`<ns:DanePobierzRaportZbiorczy><ns:pDataRaportu>yesterday</ns:pDataRaportu><ns:pNazwaRaportu>BIR11NowePodmiotyPrawneOrazDzialalnosciOsFizycznych</ns:pNazwaRaportu></ns:DanePobierzRaportZbiorczy>`),
		"a value's name":        soap12(`<ns:GetValue xmlns:ns="http://CIS/BIR/2014/07"><ns:pNazwaParametru>a b</ns:pNazwaParametru></ns:GetValue>`),
		"an entity":             []byte(`<!DOCTYPE x [<!ENTITY e SYSTEM "file:///etc/passwd">]><soap:Envelope xmlns:soap="http://www.w3.org/2003/05/soap-envelope"><soap:Body><ns:Zaloguj xmlns:ns="http://CIS/BIR/PUBL/2014/07"><ns:pKluczUzytkownika>&e;</ns:pKluczUzytkownika></ns:Zaloguj></soap:Body></soap:Envelope>`),
	} {
		if to, err := forward(request(body)); err == nil {
			t.Errorf("%s went on: %s", name, to.Body)
		}
	}
	get := request(nil)
	get.Method, get.Path = "GET", "/wsBIR/wsdl/UslugaBIRzewnPubl-ver11-prod.wsdl"
	if to, err := forward(get); err != nil || to.URL != "https://wyszukiwarkaregontest.stat.gov.pl/wsBIR/wsdl/UslugaBIRzewnPubl-ver11-prod.wsdl" {
		t.Fatalf("the description: %+v %v", to, err)
	}
	get.Path = "/somewhere/else"
	if _, err := forward(get); err == nil {
		t.Fatal("a GET elsewhere went on")
	}
	none := request(nil)
	if _, err := forward(none); err == nil {
		t.Fatal("a call with no body went on")
	}
}

// A report of a day, a value and a logout are written again as they were
// asked; what GUS answered with success is counted.
func TestTheOtherCallsAreWrittenAgain(t *testing.T) {
	for given, want := range map[string]string{
		`<ns:DanePobierzRaportZbiorczy><ns:pDataRaportu>2026-11-20</ns:pDataRaportu><ns:pNazwaRaportu>BIR11NoweJednostkiLokalne</ns:pNazwaRaportu></ns:DanePobierzRaportZbiorczy>`: "<ns:pDataRaportu>2026-11-20</ns:pDataRaportu>",
		`<ns:GetValue xmlns:ns="http://CIS/BIR/2014/07"><ns:pNazwaParametru>StatusSesji</ns:pNazwaParametru></ns:GetValue>`:                                                        `<ns:GetValue xmlns:ns="http://CIS/BIR/2014/07"><ns:pNazwaParametru>StatusSesji</ns:pNazwaParametru></ns:GetValue>`,
		`<ns:Wyloguj><ns:pIdentyfikatorSesji>ey6nu6e2428xp3b434gx</ns:pIdentyfikatorSesji></ns:Wyloguj>`:                                                                           "<ns:pIdentyfikatorSesji>ey6nu6e2428xp3b434gx</ns:pIdentyfikatorSesji>",
	} {
		to, err := forward(request(soap12(given)))
		if err != nil || !strings.Contains(string(to.Body), want) {
			t.Errorf("%s: %v\n%s", given, err, to.Body)
		}
	}
}

// A tenant is given a session of the plugin's in place of BIR1's: BIR1's
// goes on its calls, of that tenant only, and is gone once it logged out.
func TestATenantHoldsNoSessionOfBIR1s(t *testing.T) {
	answer := []byte(`<s:Envelope><s:Body><ZalogujResponse><ZalogujResult>ey6nu6e2428xp3b434gx</ZalogujResult></ZalogujResponse></s:Body></s:Envelope>`)
	replaced, err := (&login{tenant: "acme"}).Replace(&pluginsdk.Response{Status: 200}, answer, nil)
	if err != nil || strings.Contains(string(replaced), "ey6nu6e2428xp3b434gx") {
		t.Fatalf("%s %v", replaced, err)
	}
	token := string(replaced[strings.Index(string(replaced), "<ZalogujResult>")+len("<ZalogujResult>") : strings.Index(string(replaced), "</ZalogujResult>")])
	if len(token) != 20 || !sessionID.MatchString(token) {
		t.Fatalf("token %q", token)
	}
	search := func(tenant, sid string) *pluginsdk.Forward {
		r := request(soap12(`<ns:DaneSzukajPodmioty><ns:pParametryWyszukiwania><dat:Nip>5261040828</dat:Nip></ns:pParametryWyszukiwania></ns:DaneSzukajPodmioty>`))
		r.Tenant, r.Header = tenant, map[string][]string{"Sid": {sid}}
		to, err := forward(r)
		if err != nil {
			t.Fatal(err)
		}
		return to
	}
	if to := search("acme", token); to.Header["Sid"][0] != "ey6nu6e2428xp3b434gx" {
		t.Fatalf("acme's call: %+v", to.Header)
	}
	for tenant, sid := range map[string]string{"beta": token, "acme": "ey6nu6e2428xp3b434gx", "": ""} {
		if to := search(tenant, sid); to.Header["Sid"] != nil || len(to.Drop) != 1 {
			t.Errorf("%s with %q: %+v, drop %v", tenant, sid, to.Header, to.Drop)
		}
	}
	logout := request(soap12(`<ns:Wyloguj><ns:pIdentyfikatorSesji>` + token + `</ns:pIdentyfikatorSesji></ns:Wyloguj>`))
	logout.Tenant = "acme"
	to, err := forward(logout)
	if err != nil || !strings.Contains(string(to.Body), "<ns:pIdentyfikatorSesji>ey6nu6e2428xp3b434gx</ns:pIdentyfikatorSesji>") {
		t.Fatalf("logout: %v %s", err, to.Body)
	}
	if to := search("acme", token); to.Header["Sid"] != nil {
		t.Fatal("a session logged out is still known")
	}
	// A key BIR1 did not take has no session: its answer is as it was.
	empty := []byte(`<ZalogujResponse><ZalogujResult/></ZalogujResponse>`)
	if replaced, err := (&login{tenant: "acme"}).Replace(&pluginsdk.Response{Status: 200}, empty, nil); err != nil || string(replaced) != string(empty) {
		t.Fatalf("%s %v", replaced, err)
	}
}

// Sessions are kept for an hour without a call, and at most so many of
// one tenant.
func TestSessionsAreBounded(t *testing.T) {
	now := time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)
	s := &sessions{byToken: map[string]*session{}, now: func() time.Time { now = now.Add(time.Second); return now }}
	first := s.open("acme", "aaaa")
	for range maxTenantSession {
		s.open("acme", "bbbb")
	}
	if _, ok := s.find("acme", first); ok || len(s.byToken) != maxTenantSession {
		t.Fatalf("acme keeps %d, its first %v", len(s.byToken), ok)
	}
	other := s.open("beta", "cccc")
	now = now.Add(sessionIdle + time.Minute)
	if _, ok := s.find("beta", other); ok {
		t.Fatal("a session idle past an hour is known")
	}
	s.open("beta", "dddd")
	if len(s.byToken) != 1 {
		t.Fatalf("%d kept after an hour", len(s.byToken))
	}
}

// A login's result is read whatever its element's prefix and attributes:
// BIR1's session never passes; what the plugin cannot read for sure
// withholds the answer.
func TestALoginsResultIsReadHoweverItIsWritten(t *testing.T) {
	const sid = "ey6nu6e2428xp3b434gx"
	for _, answer := range []string{
		`<a:ZalogujResult>` + sid + `</a:ZalogujResult>`,
		`<ZalogujResult xmlns="http://CIS/BIR/PUBL/2014/07">` + sid + `</ZalogujResult>`,
		`<b:ZalogujResult xmlns:b="x"> ` + sid + "\n</b:ZalogujResult >",
		`<ß:ZalogujResult xmlns:ß="x">` + sid + `</ß:ZalogujResult>`,
		`<ZalogujResult><![CDATA[` + sid + `]]></ZalogujResult>`,
		`<ZalogujResult a="/>">` + sid + `</ZalogujResult>`,
	} {
		body := []byte(`<s:Envelope><s:Body><ZalogujResponse>` + answer + `</ZalogujResponse></s:Body></s:Envelope>`)
		replaced, err := (&login{tenant: "acme"}).Replace(&pluginsdk.Response{Status: 200}, body, nil)
		if err != nil || strings.Contains(string(replaced), sid) {
			t.Errorf("%s: %s %v", answer, replaced, err)
		}
	}
	for status, answers := range map[int][]string{
		200: {
			`<ZalogujResult>` + sid + `</ZalogujResult><ZalogujResult>` + sid + `</ZalogujResult>`,
			`<ZalogujResult>&#101;y6nu6e2428xp3b434gx</ZalogujResult>`,
			`<ZalogujResult>` + sid,
			`<ZalogujResult><b>` + sid + `</b></ZalogujResult>`,
			`<s:Envelope><ZalogujResult/></s:Envelope>` + sid,
			`<s:Envelope><ZalogujResult/></s:Envelope><x>` + sid + `</x>`,
			`<ZalogujResult/>` + sid,
			`<ZalogujResponse>` + sid + `</ZalogujResponse>`,
		},
		500: {`<ZalogujResult>` + sid + `!</ZalogujResult>`},
	} {
		for _, answer := range answers {
			if replaced, err := (&login{tenant: "acme"}).Replace(&pluginsdk.Response{Status: status}, []byte(answer), nil); err == nil {
				t.Errorf("%d %s: passed as %s", status, answer, replaced)
			}
		}
	}
	// An empty result, of a key BIR1 did not take, and a fault, pass as
	// they were.
	for status, answer := range map[int]string{
		200: `<ZalogujResponse><a:ZalogujResult></a:ZalogujResult></ZalogujResponse>`,
		201: `<ZalogujResponse><ZalogujResult i:nil="true"/></ZalogujResponse>`,
		500: `<s:Fault><s:Reason>no</s:Reason></s:Fault>`,
	} {
		if replaced, err := (&login{tenant: "acme"}).Replace(&pluginsdk.Response{Status: status}, []byte(answer), nil); err != nil || string(replaced) != answer {
			t.Errorf("%d %s: %s %v", status, answer, replaced, err)
		}
	}
}

// A client that writes MTOM sends its envelope as the root part of a
// multipart/related body: it is read from there.
func TestAnMTOMCallIsReadFromItsRootPart(t *testing.T) {
	envelope := soap12(`<ns:DaneSzukajPodmioty><ns:pParametryWyszukiwania><dat:Nip>5261040828</dat:Nip></ns:pParametryWyszukiwania></ns:DaneSzukajPodmioty>`)
	body := "--b\r\nContent-ID: <other>\r\nContent-Type: application/octet-stream\r\n\r\nnot it\r\n" +
		"--b\r\nContent-ID: <root@x>\r\nContent-Type: application/xop+xml; type=\"application/soap+xml\"\r\n\r\n" + string(envelope) + "\r\n--b--\r\n"
	r := request([]byte(body))
	r.Header = map[string][]string{"Content-Type": {`multipart/related; type="application/xop+xml"; boundary=b; start="<root@x>"; start-info="application/soap+xml"`}}
	to, err := forward(r)
	if err != nil {
		t.Fatal(err)
	}
	if m := to.Meter.(*meter); to.Detail != "DaneSzukajPodmioty" || m.detail != "nip" || m.amount != 1 {
		t.Fatalf("%+v %+v", to, m)
	}
	r.Header["Content-Type"] = []string{`multipart/related; boundary=b; start="<none>"`}
	if _, err := forward(r); err == nil {
		t.Fatal("an MTOM call without its root part was taken")
	}
	if !slices.Contains(manifest.RequestBodyTypes, "multipart/related") {
		t.Fatal("the edge does not read an MTOM call")
	}
}

// A login's XOP package is read from its one part; one with anything that
// reads as a session where the plugin does not replace it is withheld.
func TestALoginsPackageIsOfOnePart(t *testing.T) {
	const kind = `multipart/related; type="application/xop+xml"; start="<root>"; boundary="b"`
	part := func(id, text string) string { return "--b\r\nContent-ID: <" + id + ">\r\n\r\n" + text + "\r\n" }
	empty := `<s:Envelope><s:Body><ZalogujResult/></s:Body></s:Envelope>`
	response := &pluginsdk.Response{Status: 200, Header: map[string][]string{"Content-Type": {kind}}}
	one := part("root", empty) + "--b--\r\n"
	if replaced, err := (&login{tenant: "acme"}).Replace(response, []byte(one), nil); err != nil || string(replaced) != one {
		t.Fatalf("%s %v", replaced, err)
	}
	for _, bad := range []string{
		part("root", empty) + part("other", "ey6nu6e2428xp3b434gx") + "--b--\r\n",
		part("root", empty) + "--b--\r\nthe epilogue: ey6nu6e2428xp3b434gx\r\n",
		"the preamble: ey6nu6e2428xp3b434gx\r\n" + one,
		"--b\r\nContent-ID: <root>\r\nX-Sid: ey6nu6e2428xp3b434gx\r\n\r\n" + empty + "\r\n--b--\r\n",
		part("root", `<s:Envelope a="ey6nu6e2428xp3b434gx"><ZalogujResult/></s:Envelope>`) + "--b--\r\n",
		part("root", `<s:Envelope><x>&#101;y6nu6e2428xp3b434gx</x><ZalogujResult/></s:Envelope>`) + "--b--\r\n",
		part("root", `<s:Envelope><x>ey6nu6e242<![CDATA[8xp3b434gx]]></x><ZalogujResult/></s:Envelope>`) + "--b--\r\n",
		part("root", `<s:Envelope><x>ey6nu6e242<!-- -->8xp3b434gx</x><ZalogujResult/></s:Envelope>`) + "--b--\r\n",
		part("root", `<s:Envelope><x>&#101;y6nu6e2428xp3b434gx</x><ZalogujResult>ey6nu6e2428xp3b434gx</ZalogujResult></s:Envelope>`) + "--b--\r\n",
	} {
		if replaced, err := (&login{tenant: "acme"}).Replace(response, []byte(bad), nil); err == nil {
			t.Errorf("%q passed as %s", bad, replaced)
		}
	}
}
