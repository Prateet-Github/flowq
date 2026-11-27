package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Prateet-Github/flowq/internal/job"
)

func (b *RedisBroker) Enqueue(ctx context.Context, j *job.Job) error {
	if err := j.Validate(); err != nil {
		return err
	}

	data, err := json.Marshal(j)
	if err != nil {
		return fmt.Errorf("marshal job: %w", err)
	}

	key := fmt.Sprintf("{q}:pending:%s", j.Queue)

	if err := b.client.RPush(ctx, key, data).Err(); err != nil {
		return fmt.Errorf("enqueue job: %w", err)
	}

	return nil
}

func (b *RedisBroker) EnqueueIn(
	ctx context.Context,
	j *job.Job,
	delay time.Duration,
) error {
	if delay < 0 {
		return fmt.Errorf("enqueue delay cannot be negative")
	}

	return b.enqueueScheduled(ctx, j, "delay", delay.Milliseconds())
}

func (b *RedisBroker) EnqueueAt(
	ctx context.Context,
	j *job.Job,
	at time.Time,
) error {
	return b.enqueueScheduled(ctx, j, "at", at.UnixMilli())
}

func (b *RedisBroker) enqueueScheduled(
	ctx context.Context,
	j *job.Job,
	mode string,
	value int64,
) error {
	if err := j.Validate(); err != nil {
		return err
	}

	data, err := json.Marshal(j)
	if err != nil {
		return fmt.Errorf("marshal job: %w", err)
	}

	pendingKey := fmt.Sprintf("{q}:pending:%s", j.Queue)
	scheduledKey := "{q}:scheduled"
	jobKey := fmt.Sprintf("{q}:job:%s", j.ID)

	_, err = enqueueScheduledScript.Run(
		ctx,
		b.client,
		[]string{pendingKey, scheduledKey, jobKey},
		j.ID,
		data,
		mode,
		value,
	).Result()

	if err != nil {
		return fmt.Errorf("enqueue scheduled job: %w", err)
	}

	return nil
}
