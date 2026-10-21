package broker

import (
	"context"

	"github.com/Prateet-Github/flowq/internal/queue"
)

type Broker interface {
	Enqueue(ctx context.Context, job *queue.Job) error
}
