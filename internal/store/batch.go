package store

import (
	"encoding/json"
	"errors"
	"fmt"
)

// ErrStale reports a batch computed against another version of the state
// than the one it was to be applied to; nothing was changed.
var ErrStale = errors.New("stale change")

// ErrSchema reports a batch made by a release with another schema.
var ErrSchema = errors.New("schema mismatch")

// A Batch is one change of the replicated state: the statements one write
// made, with every value they need, computed against state version
// Version. Applying a batch is deterministic, so every node that applies
// the same batches in the same order holds the same state.
type Batch struct {
	// Schema is the schema version of the release that made the batch.
	Schema  int   `json:"schema"`
	Version int64 `json:"version"`
	// Signal wakes the reconciler once the batch is applied: the desired
	// state changed.
	Signal     bool        `json:"signal,omitempty"`
	Statements []Statement `json:"statements"`
}

// Statement is one SQL statement of a batch with its arguments.
type Statement struct {
	SQL  string `json:"sql"`
	Args []Arg  `json:"args,omitempty"`
}

// Arg is one statement argument. Its type is kept, so it reads back from
// JSON as it was written; a nil argument (SQL NULL) has no field set.
type Arg struct {
	Text *string `json:"s,omitempty"`
	Int  *int64  `json:"i,omitempty"`
	Bool *bool   `json:"b,omitempty"`
}

func newArg(value any) (Arg, error) {
	switch v := value.(type) {
	case nil:
		return Arg{}, nil
	case string:
		return Arg{Text: &v}, nil
	case bool:
		return Arg{Bool: &v}, nil
	case int:
		i := int64(v)
		return Arg{Int: &i}, nil
	case int64:
		return Arg{Int: &v}, nil
	default:
		return Arg{}, fmt.Errorf("statement argument of type %T", value)
	}
}

func (a Arg) value() (any, error) {
	switch {
	case a.Text != nil && a.Int == nil && a.Bool == nil:
		return *a.Text, nil
	case a.Int != nil && a.Text == nil && a.Bool == nil:
		return *a.Int, nil
	case a.Bool != nil && a.Text == nil && a.Int == nil:
		return *a.Bool, nil
	case a.Text == nil && a.Int == nil && a.Bool == nil:
		return nil, nil
	default:
		return nil, errors.New("statement argument with more than one value")
	}
}

func (s Statement) values() ([]any, error) {
	values := make([]any, len(s.Args))
	for i, arg := range s.Args {
		value, err := arg.value()
		if err != nil {
			return nil, err
		}
		values[i] = value
	}
	return values, nil
}

// EncodeBatch and DecodeBatch carry a batch through a log.
func EncodeBatch(batch Batch) ([]byte, error) { return json.Marshal(batch) }

func DecodeBatch(data []byte) (Batch, error) {
	var batch Batch
	if err := json.Unmarshal(data, &batch); err != nil {
		return Batch{}, fmt.Errorf("decode batch: %w", err)
	}
	return batch, nil
}

// recorder collects the statements of a write instead of running them.
// The write reads the state from its snapshot, so it never needs what a
// statement returns.
type recorder struct {
	statements []Statement
}

func (r *recorder) Exec(query string, args ...any) error {
	statement := Statement{SQL: query, Args: make([]Arg, len(args))}
	for i, value := range args {
		arg, err := newArg(value)
		if err != nil {
			return err
		}
		statement.Args[i] = arg
	}
	r.statements = append(r.statements, statement)
	return nil
}
