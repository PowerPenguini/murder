package panel

import (
	"regexp"
	"strconv"
	"strings"
)

// A spec that does not read is said in plain words, with the line and the
// setting it is about, beside the editor.

var (
	specType    = regexp.MustCompile("line (\\d+): cannot unmarshal !!(\\w+) `([^`]*)` into ([\\w.\\[\\]]+)")
	specUnknown = regexp.MustCompile(`line (\d+): field (\S+) not found`)
	specLine    = regexp.MustCompile(`line (\d+)`)
)

// specProblem says why a spec does not read, and its line: 0 if none.
func specProblem(problem, file string) (string, int) {
	lines := strings.Split(file, "\n")
	setting := func(n int) string {
		if n < 1 || n > len(lines) {
			return ""
		}
		key, _, _ := strings.Cut(strings.TrimSpace(lines[n-1]), ":")
		if key == "" || strings.ContainsAny(key, " \t") {
			return ""
		}
		return " (" + key + ")"
	}
	if m := specType.FindStringSubmatch(problem); m != nil {
		n, _ := strconv.Atoi(m[1])
		want := map[string]string{"int": "a whole number", "int64": "a whole number", "float64": "a number", "bool": "true or false", "string": "text"}[m[4]]
		if want == "" {
			want = "a " + strings.TrimPrefix(m[4], "spec.")
		}
		return "Line " + m[1] + setting(n) + ": " + want + " is needed here, not \"" + m[3] + "\"", n
	}
	if m := specUnknown.FindStringSubmatch(problem); m != nil {
		n, _ := strconv.Atoi(m[1])
		return "Line " + m[1] + ": \"" + m[2] + "\" is not a setting of an app spec: check its spelling", n
	}
	text := strings.TrimPrefix(problem, "the spec: ")
	text = strings.TrimPrefix(text, "yaml: ")
	n := 0
	if m := specLine.FindStringSubmatch(text); m != nil {
		n, _ = strconv.Atoi(m[1])
	}
	if text != "" {
		text = strings.ToUpper(text[:1]) + strings.TrimSuffix(text[1:], ".")
	}
	return text, n
}
