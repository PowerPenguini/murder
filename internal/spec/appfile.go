package spec

import (
	"bytes"
	"slices"

	"gopkg.in/yaml.v3"
)

// MarshalYAML writes a secret as its kind alone when its length is the
// kind's default, as app files do.
func (s Secret) MarshalYAML() (any, error) {
	if s.Bytes == 0 || s.Bytes == defaultSecretBytes[s.Kind] {
		return s.Kind, nil
	}
	type plain Secret
	return plain(s), nil
}

// AppFile is an app spec as its file says it: what `murder app apply`
// reads, and the panel edits. An app names no tenant, so no name.
func AppFile(app Tenant) ([]byte, error) {
	app.Suspended = false
	var doc yaml.Node
	if err := doc.Encode(app); err != nil {
		return nil, err
	}
	for i := 0; i+1 < len(doc.Content); i += 2 {
		if doc.Content[i].Value == "name" {
			doc.Content = slices.Delete(doc.Content, i, i+2)
			break
		}
	}
	// No scratch mounts at all is said as such: left out, /tmp is mounted.
	if app.Tmpfs != nil && len(app.Tmpfs) == 0 {
		doc.Content = append(doc.Content,
			&yaml.Node{Kind: yaml.ScalarNode, Value: "tmpfs"},
			&yaml.Node{Kind: yaml.MappingNode, Style: yaml.FlowStyle})
	}
	var out bytes.Buffer
	encoder := yaml.NewEncoder(&out)
	encoder.SetIndent(2)
	if err := encoder.Encode(&doc); err != nil {
		return nil, err
	}
	return out.Bytes(), encoder.Close()
}
