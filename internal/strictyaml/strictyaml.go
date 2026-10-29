// Package strictyaml decodes YAML the way Murder's spec files expect:
// unknown fields are errors, and so is a second document in one file.
package strictyaml

import (
	"bytes"
	"errors"
	"io"

	"gopkg.in/yaml.v3"
)

// Decode decodes the single YAML document in data into out.
func Decode(data []byte, out any) error {
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(out); err != nil {
		return err
	}
	var extra yaml.Node
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err != nil {
			return err
		}
		return errors.New("only one YAML document is allowed; remove the extra ---")
	}
	return nil
}
