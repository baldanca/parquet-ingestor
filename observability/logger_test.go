package observability

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

// TestNewSlogLogger_NilReturnsNopLogger verifies that passing nil returns a
// working no-op logger (does not panic on any call).
func TestNewSlogLogger_NilReturnsNopLogger(t *testing.T) {
	l := NewSlogLogger(nil)
	if l == nil {
		t.Fatal("expected non-nil logger")
	}
	// Must not panic.
	l.Debug("msg")
	l.Info("msg")
	l.Warn("msg")
	l.Error("msg")
}

// TestNewSlogLogger_WithSlogLogger verifies that a real *slog.Logger is wrapped
// and that messages pass through to the underlying handler.
func TestNewSlogLogger_WithSlogLogger(t *testing.T) {
	var buf bytes.Buffer
	h := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	sl := slog.New(h)

	l := NewSlogLogger(sl)
	if l == nil {
		t.Fatal("expected non-nil logger")
	}
	if _, ok := l.(slogAdapter); !ok {
		t.Fatalf("expected slogAdapter, got %T", l)
	}

	l.Debug("dbg-message", "k", "v")
	l.Info("info-message")
	l.Warn("warn-message")
	l.Error("err-message")

	out := buf.String()
	for _, want := range []string{"dbg-message", "info-message", "warn-message", "err-message"} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q; got:\n%s", want, out)
		}
	}
}

// TestNopLogger_AllMethodsSilent verifies that NopLogger returns a non-nil
// logger whose methods execute without panicking.
func TestNopLogger_AllMethodsSilent(t *testing.T) {
	l := NopLogger()
	if l == nil {
		t.Fatal("expected non-nil nop logger")
	}
	l.Debug("d")
	l.Info("i")
	l.Warn("w")
	l.Error("e")
}

// TestNopLogger_IsNopLoggerType confirms the concrete type.
func TestNopLogger_IsNopLoggerType(t *testing.T) {
	l := NopLogger()
	if _, ok := l.(nopLogger); !ok {
		t.Fatalf("expected nopLogger, got %T", l)
	}
}
