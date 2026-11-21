package pluginsdk

import (
	"bytes"
	"encoding/json"
)

// Kept keeps what a Meter needs of an answer's body to read it at its
// end, in little memory however long the body: the whole of a short one,
// and of a longer one its head and its tail, where a JSON answer says its
// model and its usage. One container meters every tenant's requests at
// once, in the memory it has: a meter that kept whole answers would run
// out of it.
type Kept struct {
	head, tail []byte
	long       bool
}

// How much Kept keeps: a body of up to keptWhole bytes whole, and of a
// longer one its first keptWhole bytes and at least its last keptTail.
const (
	keptWhole = 64 << 10
	keptTail  = 64 << 10
)

// Add keeps what it needs of the next chunk of the body.
func (k *Kept) Add(chunk []byte) {
	if !k.long {
		if len(k.head)+len(chunk) <= keptWhole {
			k.head = append(k.head, chunk...)
			return
		}
		k.long = true
	}
	k.tail = append(k.tail, chunk...)
	if len(k.tail) > 2*keptTail {
		k.tail = append(make([]byte, 0, 2*keptTail), k.tail[len(k.tail)-keptTail:]...)
	}
}

// Whole is the body, when it was short enough to be kept whole.
func (k *Kept) Whole() ([]byte, bool) { return k.head, !k.long }

// Head and Tail are what was kept of a long body's beginning and end.
func (k *Kept) Head() []byte { return k.head }
func (k *Kept) Tail() []byte { return k.tail }

// LastField reads into value the JSON value of the last field of a name in
// data, such as "usage" in the tail of a long answer, which may begin in
// the middle of a value: a name in quotes and a colon are a field
// wherever they are, as a string of JSON escapes its quotes.
func LastField(data []byte, name string, value any) bool {
	key := []byte(`"` + name + `"`)
	for end := len(data); ; {
		at := bytes.LastIndex(data[:end], key)
		if at < 0 {
			return false
		}
		rest := bytes.TrimLeft(data[at+len(key):], " \t\r\n")
		if rest, ok := bytes.CutPrefix(rest, []byte(":")); ok {
			if json.NewDecoder(bytes.NewReader(rest)).Decode(value) == nil {
				return true
			}
		}
		end = at
	}
}
