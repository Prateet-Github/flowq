package queue

import "context"

type Handler interface {
	Handle(ctx context.Context, job *Job) error
}

// its the adapter that will convert plain go fns into types that satisfy handler interface
type HandlerFunc func(ctx context.Context, job *Job) error

// this will handle calls f(ctx, job)
func (f HandlerFunc) Handle(ctx context.Context, job *Job) error {
	return f(ctx, job)
}
