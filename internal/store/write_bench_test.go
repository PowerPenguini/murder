package store

import (
	"fmt"
	"strings"
	"testing"

	"github.com/PowerPenguini/murder/internal/spec"
)

// BenchmarkWrite measures one write (a suspension) with n tenants stored:
// each write reloads the state it changed.
func BenchmarkWrite(b *testing.B) {
	verify := verifyReloads
	verifyReloads = false
	defer func() { verifyReloads = verify }()
	for _, n := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("tenants=%d", n), func(b *testing.B) {
			st, _ := openNewB(b)
			app := spec.Tenant{Image: "sha256:" + strings.Repeat("a", 64), HealthPath: "/health", User: "1000:1000", Secrets: spec.Secrets{"S": {}}}
			if _, err := st.PutApp(App{Name: "web", Spec: app}, "bench"); err != nil {
				b.Fatal(err)
			}
			for i := range n {
				if _, err := st.Create(NewTenant{Name: fmt.Sprintf("t%04d", i), App: "web"}, "bench"); err != nil {
					b.Fatal(err)
				}
			}
			b.ResetTimer()
			for i := 0; b.Loop(); i++ {
				if _, err := st.SetSuspended("t0000", i%2 == 0, "bench"); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func openNewB(b *testing.B) (*SQLite, string) {
	b.Helper()
	path := b.TempDir() + "/murder.db"
	if err := Init(path, "node-1"); err != nil {
		b.Fatal(err)
	}
	st, err := Open(path, options())
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = st.Close() })
	return st, path
}
