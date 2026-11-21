package main

import (
	"strings"
	"testing"

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
	if to.URL != "https://wyszukiwarkaregontest.stat.gov.pl/wsBIR/UslugaBIRzewnPubl.svc" || to.Detail != "Zaloguj" || to.Meter != nil ||
		!strings.Contains(body, "<ns:pKluczUzytkownika>murder-key-0123</ns:pKluczUzytkownika>") || strings.Contains(body, "tenants-own") ||
		!strings.Contains(body, "<wsa:To>https://wyszukiwarkaregontest.stat.gov.pl/wsBIR/UslugaBIRzewnPubl.svc</wsa:To>") ||
		!strings.Contains(body, "<wsa:Action>http://CIS/BIR/PUBL/2014/07/IUslugaBIRzewnPubl/Zaloguj</wsa:Action>") ||
		to.Header["Content-Type"][0] != "application/soap+xml; charset=utf-8" {
		t.Fatalf("%+v\n%s", to, body)
	}
}

// A search is counted by what it searches by, an entity for each number;
// SOAP 1.1's envelope is taken too, and written as 1.2.
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
	soap11 := []byte(`<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/"><s:Body><DanePobierzPelnyRaport xmlns="http://CIS/BIR/PUBL/2014/07"><pRegon>000331501</pRegon><pNazwaRaportu>BIR11OsPrawna</pNazwaRaportu></DanePobierzPelnyRaport></s:Body></s:Envelope>`)
	to, err := forward(request(soap11))
	if err != nil {
		t.Fatal(err)
	}
	if m := to.Meter.(*meter); m.metric != metricReports || m.detail != "BIR11OsPrawna" || !strings.Contains(string(to.Body), "http://www.w3.org/2003/05/soap-envelope") {
		t.Fatalf("%+v %s", m, to.Body)
	}
}

// What is not one of BIR1's calls, of parameters it takes, goes nowhere.
func TestWhatIsNotACallOfBIR1sIsRefused(t *testing.T) {
	for name, body := range map[string][]byte{
		"no envelope":           []byte(`{"nip":"5261040828"}`),
		"another namespace":     soap12(`<x:DaneSzukajPodmioty xmlns:x="http://evil.example"><x:pParametryWyszukiwania><dat:Nip>5261040828</dat:Nip></x:pParametryWyszukiwania></x:DaneSzukajPodmioty>`),
		"an operation unknown":  soap12(`<ns:UsunWszystko/>`),
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
