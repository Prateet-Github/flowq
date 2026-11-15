package broker

import (
	"context"
	_ "embed"

	"encoding/json"

	"fmt"
	"time"
)

func (b *RedisBroker) Dlq(
	ctx context.Context,
	jobID string,
	leaseToken string,
) error {
	result, err := dlqScript.Run(
		ctx,
		b.client,
		[]string{
			"{q}:active",
			"{q}:dlq",
		},
		jobID,
		"{q}:job:",
		leaseToken,
	).Int()

	if err != nil {
		return fmt.Errorf("dead-letter job %s: %w", jobID, err)
	}

	if result == 0 {
		return fmt.Errorf(
			"cannot dead-letter job %s: lease expired or ownership lost",
			jobID,
		)
	}

	return nil
}

func (b *RedisBroker) HeartbeatWorker(
	ctx context.Context,
	workerID string,
	inFlight []string,
	ttl time.Duration,
) error {
	data, err := json.Marshal(inFlight)
	if err != nil {
		return fmt.Errorf("marshal in-flight jobs: %w", err)
	}

	key := "{q}:worker:" + workerID

	_, err = workerHeartbeatScript.Run(
		ctx,
		b.client,
		[]string{key},
		time.Now().UnixMilli(),
		data,
		int(ttl.Seconds()),
	).Result()

	if err != nil {
		return fmt.Errorf("heartbeat worker %s: %w", workerID, err)
	}

	return nil
}

func (b *RedisBroker) UnregisterWorker(
	ctx context.Context,
	workerID string,
) error {
	key := "{q}:worker:" + workerID

	if err := b.client.Del(ctx, key).Err(); err != nil {
		return fmt.Errorf("unregister worker %s: %w", workerID, err)
	}

	return nil
}
