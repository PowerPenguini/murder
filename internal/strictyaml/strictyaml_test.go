package strictyaml

import (
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

type item struct {
	Name  string `yaml:"name"`
	Items []item `yaml:"items"`
}

func decodeChild(t *testing.T, document string) error {
	t.Helper()
	var root yaml.Node
	if err := yaml.Unmarshal([]byte(document), &root); err != nil {
		return err
	}
	done := make(chan error, 1)
	go func() {
		var out map[string]item
		done <- DecodeNode(root.Content[0], &out)
	}()
	select {
	case err := <-done:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("DecodeNode did not return")
		return nil
	}
}

func TestDecodeNodeExpandsAliases(t *testing.T) {
	if err := decodeChild(t, "a: &x {name: one}\nb: {<<: *x, items: [*x]}\n"); err != nil {
		t.Fatal(err)
	}
	if err := decodeChild(t, "a: {name: one, typo: 1}\n"); err == nil {
		t.Fatal("an unknown field was accepted")
	}
}

func TestDecodeNodeRefusesCyclesAndBombs(t *testing.T) {
	if err := decodeChild(t, "a: &x {name: one, items: [*x]}\n"); err == nil || !strings.Contains(err.Error(), "contains it") {
		t.Fatalf("a self-referencing alias = %v", err)
	}
	bomb := "a: &a {name: x}\n"
	previous := "a"
	for _, level := range []string{"b", "c", "d", "e", "f", "g", "h", "i"} {
		bomb += level + ": &" + level + " {items: [*" + previous + ", *" + previous + ", *" + previous + ", *" + previous + ", *" + previous + ", *" + previous + ", *" + previous + ", *" + previous + "]}\n"
		previous = level
	}
	if err := decodeChild(t, bomb); err == nil || !strings.Contains(err.Error(), "too large") {
		t.Fatalf("an alias bomb = %v", err)
	}
}

func TestDecodeRefusesASecondDocument(t *testing.T) {
	var out map[string]string
	if err := Decode([]byte("a: b\n---\nc: d\n"), &out); err == nil {
		t.Fatal("a second document was accepted")
	}
	if err := Decode([]byte("a: b\n"), &out); err != nil || out["a"] != "b" {
		t.Fatalf("Decode = %v, %v", out, err)
	}
}
