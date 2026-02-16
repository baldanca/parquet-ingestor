package ingestor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
)

type NDJSONEncoder[iType any] struct {
	TrailingNewline bool
}

func (e NDJSONEncoder[iType]) FileExtension() string { return ".ndjson" }

func (e NDJSONEncoder[iType]) Encode(ctx context.Context, items []iType) ([]byte, string, error) {
	_ = ctx

	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)

	for i, it := range items {
		if err := enc.Encode(it); err != nil {
			return nil, "", fmt.Errorf("ndjson encode item %d: %w", i, err)
		}
	}

	if !e.TrailingNewline && buf.Len() > 0 {
		b := buf.Bytes()
		if b[len(b)-1] == '\n' {
			buf.Truncate(buf.Len() - 1)
		}
	}

	return buf.Bytes(), "application/x-ndjson", nil
}
