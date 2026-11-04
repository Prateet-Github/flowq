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

//go:embed scripts/dequeue.lua
var dequeueLua string

//go:embed scripts/ack.lua
var ackLua string

//go:embed scripts/recover.lua
var recoverLua string

//go:embed scripts/renew.lua
var renewLua string

//go:embed scripts/release.lua
var releaseLua string

//go:embed scripts/retry.lua
var retryLua string

//go:embed scripts/promote_retries.lua
var promoteRetriesLua string

//go:embed scripts/dlq.lua
var dlqLua string

//go:embed scripts/worker_heartbeat.lua
var workerHeartbeatLua string

var (
	dequeueScript         = redis.NewScript(dequeueLua)
	ackScript             = redis.NewScript(ackLua)
	recoverScript         = redis.NewScript(recoverLua)
	renewScript           = redis.NewScript(renewLua)
	releaseScript         = redis.NewScript(releaseLua)
	retryScript           = redis.NewScript(retryLua)
	promoteRetriesScript  = redis.NewScript(promoteRetriesLua)
	dlqScript             = redis.NewScript(dlqLua)
	workerHeartbeatScript = redis.NewScript(workerHeartbeatLua)
)

type RedisBroker struct {
	client *redis.Client
}

func NewRedisBroker(client *redis.Client) *RedisBroker {
	return &RedisBroker{
		client: client,
	}
}

func (b *RedisBroker) Enqueue(ctx context.Context, job *job.Job) error {
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

func (b *RedisBroker) Dequeue(ctx context.Context, queueName string) (*job.Job, error) {
	pendingKey := fmt.Sprintf("{q}:pending:%s", queueName)
	activeKey := "{q}:active"
	jobPrefix := "{q}:job:"
	leaseUntil := time.Now().Add(30 * time.Second).UnixMilli()
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

	var job job.Job
	if err := json.Unmarshal([]byte(result), &job); err != nil {
		return nil, fmt.Errorf("unmarshal job: %w", err)
	}

	job.LeaseToken = leaseToken

	return &job, nil
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
	leaseUntil := time.Now().Add(30 * time.Second).UnixMilli()

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

func (b *RedisBroker) ScheduleRetry(
	ctx context.Context,
	job *job.Job,
	delay time.Duration,
) error {
	data, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("marshal retry job: %w", err)
	}

	retryAt := time.Now().Add(delay).UnixMilli()

	result, err := retryScript.Run(
		ctx,
		b.client,
		[]string{
			"{q}:active",
			"{q}:delayed",
		},
		job.ID,
		"{q}:job:",
		data,
		job.LeaseToken,
		retryAt,
	).Int()

	if err != nil {
		return fmt.Errorf("schedule retry for job %s: %w", job.ID, err)
	}

	if result == 0 {
		return fmt.Errorf(
			"cannot schedule retry for job %s: lease expired or ownership lost",
			job.ID,
		)
	}

	return nil
}

func (b *RedisBroker) PromoteRetries(
	ctx context.Context,
	limit int,
) (int, error) {
	if limit <= 0 {
		return 0, fmt.Errorf("retry promotion limit must be positive")
	}

	result, err := promoteRetriesScript.Run(
		ctx,
		b.client,
		[]string{"{q}:delayed"},
		time.Now().UnixMilli(),
		limit,
	).Int()
	if err != nil {
		return 0, fmt.Errorf("promote due retries: %w", err)
	}

	return result, nil
}

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
