package strictyaml

import "testing"

func TestDecodeRefusesASecondDocument(t *testing.T) {
	var out map[string]string
	if err := Decode([]byte("a: b\n---\nc: d\n"), &out); err == nil {
		t.Fatal("a second document was accepted")
	}
	if err := Decode([]byte("a: b\n"), &out); err != nil || out["a"] != "b" {
		t.Fatalf("Decode = %v, %v", out, err)
	}
}
