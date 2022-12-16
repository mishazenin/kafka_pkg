package consumer

import (
	"context"
	"io"

	"github.com/rs/zerolog"
)

type logger interface {
	Debug() *zerolog.Event
	Err(error) *zerolog.Event
	Error() *zerolog.Event
	Fatal() *zerolog.Event
	GetLevel() zerolog.Level
	Hook(zerolog.Hook) zerolog.Logger
	Info() *zerolog.Event
	Level(zerolog.Level) zerolog.Logger
	Log() *zerolog.Event
	Output(io.Writer) zerolog.Logger
	Panic() *zerolog.Event
	Print(...interface{})
	Printf(string, ...interface{})
	Sample(zerolog.Sampler) zerolog.Logger
	Trace() *zerolog.Event
	UpdateContext(func(c zerolog.Context) zerolog.Context)
	Warn() *zerolog.Event
	With() zerolog.Context
	WithContext(context.Context) context.Context
	WithLevel(zerolog.Level) *zerolog.Event
	Write([]byte) (int, error)
}
