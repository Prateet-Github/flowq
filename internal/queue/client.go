package queue

import (
	"context"
	"errors"
	"time"
)

type Broker interface {
	Enqueue(ctx context.Context, job *Job) error
	Dequeue(ctx context.Context, queueName string) (*Job, error)
	Ack(ctx context.Context, jobID, leaseToken string) error
	RecoverExpired(ctx context.Context) (int, error)
	RenewLease(ctx context.Context, jobID, leaseToken string) error
	ReleaseLease(ctx context.Context, jobID, leaseToken string) error
	ScheduleRetry(ctx context.Context, job *Job, delay time.Duration) error
	PromoteRetries(ctx context.Context, limit int) (int, error)
}

type Client struct {
	broker Broker
}

func NewClient(b Broker) *Client {
	return &Client{
		broker: b,
	}
}

// the api to enqueue job, broker.Enqueue willl do the actual enqueueing of the job
func (c *Client) Enqueue(ctx context.Context, job *Job) error {
	if job == nil {
		return errors.New("job cannot be nil")
	}

	return c.broker.Enqueue(ctx, job)
}
