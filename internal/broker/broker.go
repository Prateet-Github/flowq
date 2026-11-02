package broker

import (
	"context"

	"github.com/Prateet-Github/flowq/internal/job"
)

type Redis interface {
	Enqueue(ctx context.Context, job *job.Job) error
	Dequeue(ctx context.Context, queue string) (*job.Job, error)
}
