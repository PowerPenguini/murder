// Command testapp is a tiny tenant app for Murder's integration tests: it
// reports what it sees and keeps one value in its data directory.
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

func main() {
	dataDir := os.Getenv("DATA_DIR")
	// The "oom" variant uses memory, once, until the kernel kills it for
	// going over its limit, as a leaking app would; started again, it
	// serves.
	if os.Getenv("VARIANT") == "oom" && firstStart(filepath.Join(dataDir, "oomed")) {
		var held [][]byte
		for {
			block := make([]byte, 1<<20)
			for i := range block {
				block[i] = 1
			}
			held = append(held, block)
		}
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "ok") })
	mux.HandleFunc("GET /info", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"host":         r.Host,
			"forwardedFor": r.Header.Get("X-Forwarded-For"),
			"proto":        r.Header.Get("X-Forwarded-Proto"),
			"publicURL":    os.Getenv("PUBLIC_URL"),
			"secretLength": len(os.Getenv("APP_SECRET")),
			"uid":          os.Getuid(),
			"variant":      os.Getenv("VARIANT"),
		})
	})
	mux.HandleFunc("PUT /value", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(io.LimitReader(r.Body, 1024))
		if err := os.WriteFile(filepath.Join(dataDir, "value"), body, 0o600); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET /value", func(w http.ResponseWriter, _ *http.Request) {
		data, err := os.ReadFile(filepath.Join(dataDir, "value"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		_, _ = w.Write(data)
	})
	// dial reports whether this tenant can open a TCP connection to addr,
	// which isolation tests expect it cannot for another tenant.
	mux.HandleFunc("GET /dial", func(w http.ResponseWriter, r *http.Request) {
		conn, err := net.DialTimeout("tcp", r.URL.Query().Get("addr"), 2*time.Second)
		if err != nil {
			fmt.Fprint(w, "refused: ", err)
			return
		}
		_ = conn.Close()
		fmt.Fprint(w, "connected")
	})
	mux.HandleFunc("GET /root-writable", func(w http.ResponseWriter, _ *http.Request) {
		err := os.WriteFile("/should-fail", []byte("x"), 0o600)
		fmt.Fprint(w, err == nil)
	})
	if err := http.ListenAndServe(":8080", mux); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// firstStart reports whether marker did not exist, and creates it.
func firstStart(marker string) bool {
	file, err := os.OpenFile(marker, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return false
	}
	_ = file.Close()
	return true
}
