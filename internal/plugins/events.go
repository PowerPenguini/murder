package plugins

import (
	"bufio"
	"bytes"
	"io"
)

// eventStream passes server-sent events on, line by line, so each is
// passed as soon as it came. A data line is shown to each, which may take
// its event out; lines past maxLine pass unread. done is told once
// whether the stream was read to its end.
type eventStream struct {
	body    io.ReadCloser
	reader  *bufio.Reader
	each    func([]byte) bool
	done    func(complete bool)
	out     []byte
	err     error
	skip    bool
	dropped bool
	ended   bool
}

// maxLine is the longest event line read: a Responses API's completed
// event holds the whole answer.
const maxLine = 16 << 20

func newEventStream(body io.ReadCloser, each func([]byte) bool, done func(bool)) *eventStream {
	return &eventStream{body: body, reader: bufio.NewReaderSize(body, 32<<10), each: each, done: done}
}

func (s *eventStream) Read(buffer []byte) (int, error) {
	for len(s.out) == 0 {
		if s.err != nil {
			s.end(s.err == io.EOF)
			return 0, s.err
		}
		s.fill()
	}
	n := copy(buffer, s.out)
	s.out = s.out[n:]
	return n, nil
}

// fill reads the next line.
func (s *eventStream) fill() {
	line, err := s.reader.ReadSlice('\n')
	if err == bufio.ErrBufferFull {
		// A long line: kept whole up to maxLine, to be read.
		whole := append([]byte(nil), line...)
		for err == bufio.ErrBufferFull && len(whole) <= maxLine {
			line, err = s.reader.ReadSlice('\n')
			whole = append(whole, line...)
		}
		if err == bufio.ErrBufferFull {
			// Past maxLine: passed on unread, to its end.
			s.out, s.skip, s.err = whole, true, nil
			return
		}
		line = whole
	} else {
		line = append([]byte(nil), line...)
	}
	s.err = err
	if s.skip {
		s.out, s.skip = line, !bytes.HasSuffix(line, []byte("\n"))
		return
	}
	trimmed := bytes.TrimRight(line, "\r\n")
	if len(trimmed) == 0 && s.dropped {
		// The blank line that ends an event taken out.
		s.dropped = false
		return
	}
	if data, ok := bytes.CutPrefix(trimmed, []byte("data:")); ok {
		if !s.each(bytes.TrimPrefix(data, []byte(" "))) {
			s.dropped = true
			return
		}
	}
	s.out = line
}

func (s *eventStream) end(complete bool) {
	if !s.ended {
		s.ended = true
		s.done(complete)
	}
}

func (s *eventStream) Close() error {
	s.end(false)
	return s.body.Close()
}
