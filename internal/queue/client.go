package queue

import (
	"context"
	"fmt"
	"time"

	"github.com/Prateet-Github/flowq/internal/job"
)

type Broker interface {
	Enqueue(ctx context.Context, j *job.Job) error
	Dequeue(ctx context.Context, queueName string) (*job.Job, error)
	Ack(ctx context.Context, jobID, leaseToken string) error
	RecoverExpired(ctx context.Context) (int, error)
	RenewLease(ctx context.Context, jobID, leaseToken string) error
	ReleaseLease(ctx context.Context, jobID, leaseToken string) error
	ScheduleRetry(ctx context.Context, j *job.Job, delay time.Duration) error
	PromoteScheduled(ctx context.Context, limit int) (int, error)
	Dlq(ctx context.Context, jobID string, leaseToken string) error
	HeartbeatWorker(
		ctx context.Context,
		workerID string,
		inFlight []string,
		ttl time.Duration,
	) error
	UnregisterWorker(ctx context.Context, workerID string) error
	LeaseDuration() time.Duration
	EnqueueIn(ctx context.Context, j *job.Job, delay time.Duration) error
	EnqueueAt(ctx context.Context, j *job.Job, at time.Time) error
}

type Client struct {
	broker Broker
}

func NewClient(b Broker) *Client {
	return &Client{
		broker: b,
	}
}

func (c *Client) Enqueue(
	ctx context.Context,
	j *job.Job,
	options ...EnqueueOption,
) error {
	if err := j.Validate(); err != nil {
		return err
	}

	opts := &EnqueueOptions{}

	for _, option := range options {
		if option == nil {
			continue
		}
		option(opts)
	}

	if opts.delay < 0 {
		return fmt.Errorf("enqueue delay cannot be negative")
	}

	if opts.processAt != nil {
		return c.broker.EnqueueAt(ctx, j, *opts.processAt)
	}

	if opts.delay > 0 {
		return c.broker.EnqueueIn(ctx, j, opts.delay)
	}

	return c.broker.Enqueue(ctx, j)
}
