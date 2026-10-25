package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

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

// func (b *RedisBroker) Dequeue(ctx context.Context, queueName string) (*queue.Job, error) {
// 	key := fmt.Sprintf("{q}:pending:%s", queueName)

// 	data, err := b.client.LPop(ctx, key).Bytes()
// 	if err != nil {
// 		if errors.Is(err, redis.Nil) {
// 			return nil, nil
// 		}
// 		return nil, err
// 	}

// 	var job queue.Job
// 	if err := json.Unmarshal(data, &job); err != nil {
// 		return nil, fmt.Errorf("unmarshal job: %w", err)
// 	}

// 	return &job, nil
// }

func (b *RedisBroker) Dequeue(ctx context.Context, queueName string) (*queue.Job, error) {
	pendingKey := fmt.Sprintf("{q}:pending:%s", queueName)
	activeKey := "{q}:active"
	jobPrefix := "{q}:job:"
	leaseUntil := time.Now().Add(30 * time.Second).UnixMilli()

	script := redis.NewScript(`
		local job_data = redis.call("LPOP", KEYS[1])

		if not job_data then
			return nil
		end

		local job = cjson.decode(job_data)
		local job_key = ARGV[1] .. job.id

		redis.call("HSET", job_key,
			"data", job_data,
			"state", "active"
		)

		redis.call("ZADD", KEYS[2], ARGV[2], job.id)

		return job_data
	`)

	result, err := script.Run(
		ctx,
		b.client,
		[]string{pendingKey, activeKey},
		jobPrefix,
		leaseUntil,
	).Text()

	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return nil, fmt.Errorf("dequeue job: %w", err)
	}

	var job queue.Job
	if err := json.Unmarshal([]byte(result), &job); err != nil {
		return nil, fmt.Errorf("unmarshal job: %w", err)
	}

	return &job, nil
}

func (b *RedisBroker) Ack(ctx context.Context, jobID string) error {
	script := redis.NewScript(`
		local job_key = ARGV[1] .. ARGV[2]

		redis.call("ZREM", KEYS[1], ARGV[2])
		redis.call("DEL", job_key)

		return 1
	`)

	err := script.Run(
		ctx,
		b.client,
		[]string{"{q}:active"},
		"{q}:job:",
		jobID,
	).Err()

	if err != nil {
		return fmt.Errorf("ack job %s: %w", jobID, err)
	}

	return nil
}
