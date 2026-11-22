package plugins

import (
	"bytes"
	"io"
	"strings"
	"testing"
)

// A line past maxLine passes unread, though it ends in what the reader
// read with it: only what the container takes is shown to it.
func TestALinePastTheLongestReadPassesUnread(t *testing.T) {
	for _, past := range []int{1, 10 << 10, 31 << 10, 40 << 10} {
		line := "data: " + strings.Repeat("x", maxLine+past) + "\n"
		stream := line + "\ndata: next\n\n"
		var shown []int
		s := newEventStream(io.NopCloser(strings.NewReader(stream)), func(data []byte) (bool, error) {
			shown = append(shown, len(data))
			return true, nil
		}, func(bool) {})
		out, err := io.ReadAll(s)
		if err != nil || !bytes.Equal(out, []byte(stream)) {
			t.Fatalf("%d past: %v, %d bytes of %d", past, err, len(out), len(stream))
		}
		if len(shown) != 1 || shown[0] != len("next") {
			t.Errorf("%d past: shown %v", past, shown)
		}
	}
}
