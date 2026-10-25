// Package strictyaml decodes YAML the way Corral's config files expect:
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

// DecodeNode decodes node into out, refusing unknown fields. A custom
// unmarshaler needs it because yaml.Node.Decode does not inherit the file
// decoder's strictness. Aliases are expanded first, so anchors defined
// elsewhere in the file still resolve.
func DecodeNode(node *yaml.Node, out any) error {
	encoded, err := yaml.Marshal(expand(node))
	if err != nil {
		return err
	}
	decoder := yaml.NewDecoder(bytes.NewReader(encoded))
	decoder.KnownFields(true)
	return decoder.Decode(out)
}

// expand returns a copy of node with every alias replaced by what it refers
// to and no anchors left.
func expand(node *yaml.Node) *yaml.Node {
	for node.Kind == yaml.AliasNode {
		node = node.Alias
	}
	copied := *node
	copied.Anchor = ""
	copied.Content = make([]*yaml.Node, len(node.Content))
	for i, child := range node.Content {
		copied.Content[i] = expand(child)
	}
	return &copied
}
