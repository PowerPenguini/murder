package pluginsdk

import (
	"strings"
	"testing"
)

// A long answer is kept by its head and its tail, in bounded memory, and
// its model and usage read from them.
func TestKeptKeepsAShortBodyWholeAndALongOneByItsEnds(t *testing.T) {
	var short Kept
	short.Add([]byte(`{"model":"m",`))
	short.Add([]byte(`"usage":{"n":1}}`))
	if whole, ok := short.Whole(); !ok || string(whole) != `{"model":"m","usage":{"n":1}}` {
		t.Fatalf("short: %q %v", whole, ok)
	}
	var long Kept
	long.Add([]byte(`{"object":"list","data":[`))
	item := []byte(`{"text":"\"usage\": {\"n\": 9}, \"model\": \"fake\""},`)
	for range 100_000 {
		long.Add(item)
	}
	long.Add([]byte(`{}],"model":"text-embedding-3-small","usage":{"n":5000}}`))
	if _, ok := long.Whole(); ok {
		t.Fatal("a long body kept whole")
	}
	if len(long.Head()) > keptWhole || len(long.Tail()) > 2*keptTail+len(item) {
		t.Fatalf("kept %d and %d bytes", len(long.Head()), len(long.Tail()))
	}
	var usage struct{ N int }
	var model string
	if !LastField(long.Tail(), "usage", &usage) || usage.N != 5000 || !LastField(long.Tail(), "model", &model) || model != "text-embedding-3-small" {
		t.Fatalf("usage %+v, model %q", usage, model)
	}
	// What a string holds, its quotes escaped, is no field.
	if LastField([]byte(strings.Repeat(string(item), 3)), "usage", &usage) {
		t.Fatal("a field read from inside a string")
	}
}
