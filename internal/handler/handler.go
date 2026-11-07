package handler

import (
	"context"
	"errors"

	"github.com/Prateet-Github/flowq/internal/job"
)

type Handler interface {
	Handle(ctx context.Context, j *job.Job) error
}

var ErrUnknownJobType = errors.New("unknown job type")

// its the adapter that will convert plain go fns into types that satisfy handler interface
type HandlerFunc func(ctx context.Context, j *job.Job) error

// this will handle calls f(ctx, job)
func (f HandlerFunc) Handle(ctx context.Context, j *job.Job) error {
	return f(ctx, j)
}
