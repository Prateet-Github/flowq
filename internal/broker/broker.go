package broker

import (
	"context"

	"github.com/Prateet-Github/flowq/internal/queue"
)

type Redis interface {
	Enqueue(ctx context.Context, job *queue.Job) error
	Dequeue(ctx context.Context, queue string) (*queue.Job, error)
}
