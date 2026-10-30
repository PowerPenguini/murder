package cluster

import (
	"context"
	"encoding/json"
	"log/slog"
	"maps"
	"slices"
	"strings"

	"github.com/hashicorp/go-hclog"
)

// raftLogger has Raft log through the agent's logger, at the right levels.
func raftLogger(log *slog.Logger) hclog.Logger {
	return hclog.New(&hclog.LoggerOptions{Name: "raft", Level: hclog.Info, JSONFormat: true, Output: slogWriter{log.With("component", "raft")}})
}

// slogWriter takes the JSON lines hclog writes, one per call.
type slogWriter struct{ log *slog.Logger }

func (w slogWriter) Write(line []byte) (int, error) {
	var entry map[string]any
	if err := json.Unmarshal(line, &entry); err != nil {
		w.log.Info(strings.TrimSpace(string(line)))
		return len(line), nil
	}
	level := slog.LevelInfo
	switch entry["@level"] {
	case "trace", "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}
	message, _ := entry["@message"].(string)
	var attrs []any
	for _, key := range slices.Sorted(maps.Keys(entry)) {
		if !strings.HasPrefix(key, "@") {
			attrs = append(attrs, key, entry[key])
		}
	}
	w.log.Log(context.Background(), level, message, attrs...)
	return len(line), nil
}
