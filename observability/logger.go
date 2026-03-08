package observability

import "log/slog"

// Logger is a minimal structured logger interface used across the project.
type Logger interface {
	Debug(msg string, args ...any)
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

type slogAdapter struct{ l *slog.Logger }

func (s slogAdapter) Debug(msg string, args ...any) { s.l.Debug(msg, args...) }
func (s slogAdapter) Info(msg string, args ...any)  { s.l.Info(msg, args...) }
func (s slogAdapter) Warn(msg string, args ...any)  { s.l.Warn(msg, args...) }
func (s slogAdapter) Error(msg string, args ...any) { s.l.Error(msg, args...) }

type nopLogger struct{}

func (nopLogger) Debug(string, ...any) {}
func (nopLogger) Info(string, ...any)  {}
func (nopLogger) Warn(string, ...any)  {}
func (nopLogger) Error(string, ...any) {}

// NewSlogLogger wraps slog.Logger into the project logger interface.
func NewSlogLogger(l *slog.Logger) Logger {
	if l == nil {
		return nopLogger{}
	}
	return slogAdapter{l: l}
}

// NopLogger returns a logger that discards all logs.
func NopLogger() Logger { return nopLogger{} }
