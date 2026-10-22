package queue

import "context"

type Handler interface {
	Handle(ctx context.Context, job *Job) error
}

type HandlerFunc func(ctx context.Context, job *Job) error

func (f HandlerFunc) Handle(ctx context.Context, job *Job) error {
	return f(ctx, job)
}
