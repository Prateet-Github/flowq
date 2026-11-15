package broker

import (
	"context"
	_ "embed"

	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/Prateet-Github/flowq/internal/job"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

func WithLease(d time.Duration) RedisBrokerOption {
	return func(b *RedisBroker) {
		if d > 0 {
			b.lease = d
		}
	}
}

func (b *RedisBroker) LeaseDuration() time.Duration {
	return b.lease
}

func (b *RedisBroker) Dequeue(ctx context.Context, queueName string) (*job.Job, error) {
	pendingKey := fmt.Sprintf("{q}:pending:%s", queueName)
	activeKey := "{q}:active"
	jobPrefix := "{q}:job:"
	leaseUntil := time.Now().Add(b.lease).UnixMilli()
	leaseToken := uuid.NewString()

	result, err := dequeueScript.Run(
		ctx,
		b.client,
		[]string{pendingKey, activeKey},
		jobPrefix,
		leaseUntil,
		leaseToken,
	).Text()

	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return nil, fmt.Errorf("dequeue job: %w", err)
	}

	var j job.Job
	if err := json.Unmarshal([]byte(result), &j); err != nil {
		return nil, fmt.Errorf("unmarshal job: %w", err)
	}

	j.LeaseToken = leaseToken

	return &j, nil
}

func (b *RedisBroker) Ack(ctx context.Context, jobID, leaseToken string) error {
	result, err := ackScript.Run(
		ctx,
		b.client,
		[]string{"{q}:active"},
		jobID,
		"{q}:job:",
		leaseToken,
	).Int()
	if err != nil {
		return fmt.Errorf("ack job %s: %w", jobID, err)
	}

	if result == 0 {
		return fmt.Errorf("cannot acknowledge job %s: lease expired or ownership lost", jobID)
	}

	return nil
}

func (b *RedisBroker) RecoverExpired(ctx context.Context) (int, error) {
	result, err := recoverScript.Run(
		ctx,
		b.client,
		[]string{"{q}:active"},
		"{q}:job:",
		time.Now().UnixMilli(),
	).Int()

	if err != nil {
		return 0, fmt.Errorf("recover expired jobs: %w", err)
	}

	return result, nil
}

func (b *RedisBroker) RenewLease(ctx context.Context, jobID, leaseToken string) error {
	leaseUntil := time.Now().Add(b.lease).UnixMilli()

	result, err := renewScript.Run(
		ctx,
		b.client,
		[]string{"{q}:active"},
		"{q}:job:",
		jobID,
		leaseToken,
		leaseUntil,
	).Int()
	if err != nil {
		return fmt.Errorf("renew lease for job %s: %w", jobID, err)
	}

	if result == 0 {
		return fmt.Errorf("cannot renew lease for job %s: lease expired or ownership lost", jobID)
	}

	return nil
}

func (b *RedisBroker) ReleaseLease(
	ctx context.Context,
	jobID string,
	leaseToken string,
) error {
	result, err := releaseScript.Run(
		ctx,
		b.client,
		[]string{
			"{q}:job:" + jobID,
			"{q}:active",
		},
		jobID,
		leaseToken,
	).Int()

	if err != nil {
		return fmt.Errorf("release lease for job %s: %w", jobID, err)
	}

	if result == 0 {
		return fmt.Errorf(
			"cannot release lease for job %s: lease expired or ownership lost",
			jobID,
		)
	}

	return nil
}
