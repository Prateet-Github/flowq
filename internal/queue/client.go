package queue

import (
	"context"
	"errors"
)

type Broker interface {
	Enqueue(ctx context.Context, job *Job) error
	Dequeue(ctx context.Context, queueName string) (*Job, error)
	Ack(ctx context.Context, jobID, leaseToken string) error
	RenewLease(ctx context.Context, jobID, leaseToken string) error
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
