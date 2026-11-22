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

// A field cut where the head ends is read whole in the tail: the tail
// goes on from the end of the head.
func TestAFieldCutAtTheEndOfTheHeadIsReadInTheTail(t *testing.T) {
	answer := `{"data":"` + strings.Repeat("x", keptWhole-len(`{"data":"`)-len(`","usage":`)) + `","usage":{"prompt_tokens":7,"completion_tokens":9}}`
	var kept Kept
	for chunk := []byte(answer); len(chunk) > 0; {
		n := min(16<<10, len(chunk))
		kept.Add(chunk[:n])
		chunk = chunk[n:]
	}
	if _, ok := kept.Whole(); ok || !strings.HasSuffix(string(kept.Head()), `"usage":`) {
		t.Fatalf("the head ends %q", kept.Head()[len(kept.Head())-20:])
	}
	var usage struct {
		Prompt     int `json:"prompt_tokens"`
		Completion int `json:"completion_tokens"`
	}
	if !LastField(kept.Tail(), "usage", &usage) || usage.Prompt != 7 || usage.Completion != 9 {
		t.Fatalf("usage %+v", usage)
	}
}

// A chunk is kept whole in the tail, however long the tail was.
func TestAChunkIsKeptWholeInTheTail(t *testing.T) {
	var kept Kept
	kept.Add([]byte(strings.Repeat("h", keptWhole)))
	chunk := `"usage":{"n":3}` + strings.Repeat(" ", keptTail)
	kept.Add([]byte(chunk))
	var usage struct{ N int }
	if !LastField(kept.Tail(), "usage", &usage) || usage.N != 3 {
		t.Fatalf("usage %+v", usage)
	}
}
