//go:build wasip1

package pluginsdk

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"unsafe"
)

var (
	manifest Manifest
	forward  func(*Request) (*Forward, error)
	// inputs are the memory given to the edge for the current call's
	// inputs; answer what the last call answered, until the next.
	inputs [][]byte
	answer []byte
	// meter reads the answer to the current request; replaced its body.
	meter    Meter
	replaced []byte
	failed   error
)

// Register makes the module a plugin: m is its manifest, f says where
// each request goes, or refuses it (see Refuse).
func Register(m Manifest, f func(*Request) (*Forward, error)) {
	m.ABI = ABI
	manifest, forward = m, f
}

// Log writes to the edge's log, with the module's failures.
func Log(format string, args ...any) { fmt.Fprintf(os.Stderr, format+"\n", args...) }

//go:wasmexport murder_alloc
func murderAlloc(size uint32) uint32 {
	buffer := make([]byte, size)
	inputs = append(inputs, buffer)
	return uint32(uintptr(unsafe.Pointer(unsafe.SliceData(buffer))))
}

// input is the memory murder_alloc gave the edge at pointer, holding
// length bytes: the slice itself, so that what keeps it keeps it alive.
func input(pointer, length uint32) []byte {
	if length == 0 {
		return nil
	}
	for _, buffer := range inputs {
		if uint32(uintptr(unsafe.Pointer(unsafe.SliceData(buffer)))) == pointer && uint32(len(buffer)) >= length {
			return buffer[:length]
		}
	}
	panic("the edge gave an input murder_alloc did not")
}

// give answers a call with data, kept until the next call; the memory of
// the call's inputs is the module's again.
func give(data []byte) uint64 {
	inputs, answer = nil, data
	if len(data) == 0 {
		return 0
	}
	return uint64(uintptr(unsafe.Pointer(unsafe.SliceData(data))))<<32 | uint64(len(data))
}

func giveJSON(value any) uint64 {
	data, err := json.Marshal(value)
	if err != nil {
		data, _ = json.Marshal(map[string]string{"error": err.Error()})
	}
	return give(data)
}

// panicked is a panic of the plugin's, which the edge takes as the
// module failing, not as the request refused.
type panicked struct{ value any }

func (p panicked) Error() string { return fmt.Sprintf("panic: %v", p.value) }

// guard turns a panic into an error the edge is answered.
func guard(err *error) {
	if r := recover(); r != nil {
		*err = panicked{r}
	}
}

//go:wasmexport murder_manifest
func murderManifest() uint64 {
	if forward == nil {
		return giveJSON(map[string]string{"error": "the module registers no plugin: call pluginsdk.Register in an init function"})
	}
	return giveJSON(manifest)
}

type forwarded struct {
	URL    string              `json:"url"`
	Header map[string][]string `json:"header,omitempty"`
	Drop   []string            `json:"drop,omitempty"`
	Body   bool                `json:"body,omitempty"`
	Detail string              `json:"detail,omitempty"`
	Meter  bool                `json:"meter,omitempty"`
	Refuse *Refusal            `json:"refuse,omitempty"`
	Error  string              `json:"error,omitempty"`
}

//go:wasmexport murder_request
func murderRequest(pointer, length, bodyPointer, bodyLength uint32) uint64 {
	meter, replaced, failed, counted = nil, nil, nil, nil
	var request Request
	if err := json.Unmarshal(input(pointer, length), &request); err != nil {
		return giveJSON(forwarded{Error: "the edge's request is not JSON: " + err.Error()})
	}
	if bodyLength > 0 {
		request.Body = input(bodyPointer, bodyLength)
	}
	to, err := func() (to *Forward, err error) {
		defer guard(&err)
		return forward(&request)
	}()
	var refusal *Refusal
	switch {
	case errors.As(err, new(panicked)):
		return giveJSON(forwarded{Error: err.Error()})
	case errors.As(err, &refusal):
		return giveJSON(forwarded{Refuse: refusal})
	case err != nil:
		return giveJSON(forwarded{Refuse: &Refusal{Status: 400, Message: err.Error()}})
	case to == nil:
		return giveJSON(forwarded{Error: "the plugin forwards the request nowhere"})
	}
	meter, replaced = to.Meter, to.Body
	return giveJSON(forwarded{URL: to.URL, Header: to.Header, Drop: to.Drop, Body: to.Body != nil, Detail: to.Detail, Meter: to.Meter != nil})
}

//go:wasmexport murder_request_body
func murderRequestBody() uint64 { return give(replaced) }

type metered struct {
	Counts []count `json:"counts,omitempty"`
	Body   bool    `json:"body,omitempty"`
	Error  string  `json:"error,omitempty"`
}

// counts answers what was counted since the last answer.
func counts(body bool, err error) uint64 {
	if err == nil {
		err = failed
	}
	answer := metered{Counts: counted, Body: body}
	if err != nil {
		answer = metered{Error: err.Error()}
	}
	counted, failed = nil, nil
	return giveJSON(answer)
}

//go:wasmexport murder_response
func murderResponse(pointer, length uint32) uint64 {
	var response Response
	if err := json.Unmarshal(input(pointer, length), &response); err != nil {
		return counts(false, err)
	}
	if meter == nil {
		return counts(false, nil)
	}
	var body bool
	err := func() (err error) {
		defer guard(&err)
		body = meter.Response(&response)
		return nil
	}()
	return counts(body, err)
}

//go:wasmexport murder_body
func murderBody(pointer, length uint32) uint32 {
	if meter == nil {
		inputs = nil
		return 1
	}
	var more bool
	failed = func() (err error) {
		defer guard(&err)
		more = meter.Body(input(pointer, length))
		return nil
	}()
	inputs = nil
	switch {
	case failed != nil:
		return 2
	case more:
		return 0
	}
	return 1
}

//go:wasmexport murder_end
func murderEnd(pointer, length uint32) uint64 {
	var end struct {
		Complete bool `json:"complete"`
	}
	if err := json.Unmarshal(input(pointer, length), &end); err != nil {
		return counts(false, err)
	}
	if meter == nil {
		return counts(false, nil)
	}
	err := func() (err error) {
		defer guard(&err)
		meter.End(end.Complete)
		return nil
	}()
	meter, replaced = nil, nil
	return counts(false, err)
}
