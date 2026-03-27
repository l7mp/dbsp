package logger

import (
	"github.com/go-logr/logr"
	"github.com/go-logr/zapr"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	TraceLevel = zapcore.DebugLevel - 1
	DebugLevel = zapcore.DebugLevel
	InfoLevel  = zapcore.InfoLevel
	WarnLevel  = zapcore.WarnLevel
	ErrorLevel = zapcore.ErrorLevel
	PanicLevel = zapcore.PanicLevel
)

// DiscardLogger returns a no-op logger.
func DiscardLogger() logr.Logger {
	return logr.Discard()
}

// NewZapLogger creates a logr.Logger backed by zap.
func NewZapLogger(level zapcore.Level) logr.Logger {
	zc := zap.NewDevelopmentConfig()
	zc.Level = zap.NewAtomicLevelAt(level)
	z, _ := zc.Build()
	return zapr.NewLogger(z)
}

func NormalizeLogger(logger logr.Logger) logr.Logger {
	if logger == (logr.Logger{}) {
		return logr.Discard()
	}
	return logger
}
