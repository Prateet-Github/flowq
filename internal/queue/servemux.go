package queue

import (
	"context"
	"fmt"
)

type ServeMux struct {
	handlers map[string]Handler
}

func NewServeMux() *ServeMux {
	return &ServeMux{
		handlers: make(map[string]Handler),
	}
}

func (m *ServeMux) Handle(jobType string, handler Handler) {
	if jobType == "" {
		panic("queue: job type cannot be empty")
	}
	if handler == nil {
		panic("queue: handler cannot be nil")
	}

	m.handlers[jobType] = handler
}

func (m *ServeMux) Process(ctx context.Context, job *Job) error {
	handler, ok := m.handlers[job.Type]
	if !ok {
		return fmt.Errorf("queue: no handler registered for job type %q", job.Type)
	}

	return handler.Handle(ctx, job)
}
