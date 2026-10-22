package broker

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/Prateet-Github/flowq/internal/queue"
	"github.com/redis/go-redis/v9"
)

type RedisBroker struct {
	client *redis.Client
}

func NewRedisBroker(client *redis.Client) *RedisBroker {
	return &RedisBroker{
		client: client,
	}
}

func (b *RedisBroker) Enqueue(ctx context.Context, job *queue.Job) error {
	data, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("marshal job: %w", err)
	}

	key := fmt.Sprintf("{q}:pending:%s", job.Queue)

	if err := b.client.RPush(ctx, key, data).Err(); err != nil {
		return fmt.Errorf("enqueue job: %w", err)
	}

	return nil
}

func (b *RedisBroker) Dequeue(ctx context.Context, queueName string) (*queue.Job, error) {
	key := fmt.Sprintf("{q}:pending:%s", queueName)

	data, err := b.client.LPop(ctx, key).Bytes()
	if err != nil {
		return nil, err
	}

	var job queue.Job
	if err := json.Unmarshal(data, &job); err != nil {
		return nil, fmt.Errorf("unmarshal job: %w", err)
	}

	return &job, nil
}
