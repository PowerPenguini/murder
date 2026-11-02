package store

import (
	"fmt"
	"testing"
)

// A write that changes a few tenants reads only those again; one that
// changes an app, which every tenant's spec comes from, reads all.
func TestAWriteReadsAgainWhatItChanged(t *testing.T) {
	st, _ := withApp(t, options())
	for i := range 20 {
		if _, err := st.Create(NewTenant{Name: fmt.Sprintf("t%02d", i), App: "web"}, "alice"); err != nil {
			t.Fatal(err)
		}
	}
	for _, c := range []struct {
		what  string
		write func() error
		whole bool
	}{
		{"a suspension", func() error { _, err := st.SetSuspended("t03", true, "alice"); return err }, false},
		{"a rotation", func() error { _, err := st.Rotate("t04", "SESSION", "alice"); return err }, false},
		{"an archive", func() error { return st.Archive("t05", "alice") }, false},
		{"a purge, its secrets going with it", func() error { return st.Forget("t05", "alice") }, false},
		{"a new tenant", func() error { _, err := st.Create(NewTenant{Name: "new", App: "web"}, "alice"); return err }, false},
		{"a note", func() error { return st.Note("test", "", "alice") }, false},
		{"an image forgotten", func() error { return st.ForgetImage(image) }, false},
		{"an app", func() error {
			next := appSpec()
			next.Env = map[string]string{"MODE": "prod"}
			_, err := st.PutApp(App{Name: "web", Spec: next}, "alice")
			return err
		}, true},
	} {
		whole, part := reloadsWhole.Load(), reloadsInPart.Load()
		if err := c.write(); err != nil {
			t.Fatalf("%s: %v", c.what, err)
		}
		if got := reloadsWhole.Load() > whole; got != c.whole || reloadsInPart.Load() == part && !c.whole {
			t.Errorf("%s read the whole state: %v, want %v", c.what, got, c.whole)
		}
	}
}
