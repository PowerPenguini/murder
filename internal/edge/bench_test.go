package edge

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// benchBackend answers like a small JSON API.
func benchBackend(b *testing.B) *httptest.Server {
	b.Helper()
	body := strings.Repeat(`{"id":1,"name":"freight"},`, 40) // about 1 KB
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, body)
	}))
	b.Cleanup(backend.Close)
	return backend
}

func benchClient() *http.Client {
	return &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 256, DisableCompression: true}}
}

func runBench(b *testing.B, url string) {
	client := benchClient()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			request, _ := http.NewRequest("GET", url+"/api/freights", nil)
			request.Host = "acme.example.com"
			response, err := client.Do(request)
			if err != nil {
				b.Error(err)
				return
			}
			_, _ = io.Copy(io.Discard, response.Body)
			response.Body.Close()
			if response.StatusCode != 200 {
				b.Errorf("status %d", response.StatusCode)
				return
			}
		}
	})
}

// BenchmarkProxy is a request through the edge to a tenant on its node.
func BenchmarkProxy(b *testing.B) {
	backend := benchBackend(b)
	table := &LiveTable{}
	table.Set(NewTable(map[string]Route{"acme": {Backend: strings.TrimPrefix(backend.URL, "http://")}}))
	front := httptest.NewServer(NewProxy("example.com", table, quietLog()))
	defer front.Close()
	runBench(b, front.URL)
}

// BenchmarkProxyHandler is the proxy's own work: no client connection.
func BenchmarkProxyHandler(b *testing.B) {
	backend := benchBackend(b)
	table := &LiveTable{}
	table.Set(NewTable(map[string]Route{"acme": {Backend: strings.TrimPrefix(backend.URL, "http://")}}))
	proxy := NewProxy("example.com", table, quietLog())
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			request := httptest.NewRequest("GET", "http://acme.example.com/api/freights", nil)
			recorder := httptest.NewRecorder()
			proxy.ServeHTTP(recorder, request)
			if recorder.Code != 200 {
				b.Errorf("status %d", recorder.Code)
				return
			}
		}
	})
}

// BenchmarkRelay is a request through an edge to a tenant of another node.
func BenchmarkRelay(b *testing.B) {
	backend := benchBackend(b)
	creds := credentialsFor(b, 7)
	backProxy := NewProxy("example.com", staticRoutes{"acme": {Backend: strings.TrimPrefix(backend.URL, "http://")}}, quietLog()).WithRelay(creds.RelayClient())
	back := httptest.NewUnstartedServer(backProxy.Relay())
	back.TLS = creds.RelayServer()
	back.StartTLS()
	defer back.Close()
	front := httptest.NewServer(NewProxy("example.com", staticRoutes{"acme": {Relay: strings.TrimPrefix(back.URL, "https://")}}, quietLog()).WithRelay(creds.RelayClient()))
	defer front.Close()
	runBench(b, front.URL)
}
