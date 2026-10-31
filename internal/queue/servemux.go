package queue

import (
	"context"
	"fmt"
)

type ServeMux struct {
	handlers map[string]Handler
}

// sort of job(or task as we say in go) router
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

// check the job type and call the appropriate handler
func (m *ServeMux) Process(ctx context.Context, job *Job) (err error) {
	handler, ok := m.handlers[job.Type]
	if !ok {
		return fmt.Errorf("queue: no handler registered for job type %q", job.Type)
	}

	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("queue: handler panic for job type %q: %v", job.Type, recovered)
		}
	}()

	return handler.Handle(ctx, job)
}
