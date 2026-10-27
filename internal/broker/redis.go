package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/Prateet-Github/flowq/internal/queue"
	"github.com/google/uuid"
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
	pendingKey := fmt.Sprintf("{q}:pending:%s", queueName)
	activeKey := "{q}:active"
	jobPrefix := "{q}:job:"
	leaseUntil := time.Now().Add(30 * time.Second).UnixMilli()
	leaseToken := uuid.NewString()

	script := redis.NewScript(`
		local job_data = redis.call("LPOP", KEYS[1])

		if not job_data then
			return nil
		end

		local job = cjson.decode(job_data)
		local job_key = ARGV[1] .. job.id

		redis.call("HSET", job_key,
			"data", job_data,
			"state", "active",
			"lease_token", ARGV[3]
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
		leaseToken,
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

	job.LeaseToken = leaseToken

	return &job, nil
}

func (b *RedisBroker) Ack(ctx context.Context, jobID, leaseToken string) error {
	script := redis.NewScript(`
		local job_id = ARGV[1]
		local job_key = ARGV[2] .. job_id
		local lease_token = ARGV[3]

		local current_token = redis.call("HGET", job_key, "lease_token")
		if not current_token or current_token ~= lease_token then
			return 0
		end

		local score = redis.call("ZSCORE", KEYS[1], job_id)
		if not score then
			return 0
		end

		if tonumber(score) <= tonumber(ARGV[4]) then
			return 0
		end

		redis.call("ZREM", KEYS[1], job_id)
		redis.call("DEL", job_key)
		return 1
	`)

	result, err := script.Run(
		ctx,
		b.client,
		[]string{"{q}:active"},
		jobID,
		"{q}:job:",
		leaseToken,
		time.Now().UnixMilli(),
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
	script := redis.NewScript(`
		local active_key = KEYS[1]
		local job_prefix = ARGV[1]
		local now = tonumber(ARGV[2])

		local expired_jobs = redis.call(
			"ZRANGEBYSCORE", active_key, "-inf", now,
			"LIMIT", 0, 100
		)

		local recovered = 0

		for _, job_id in ipairs(expired_jobs) do
			local job_key = job_prefix .. job_id
			local job_data = redis.call("HGET", job_key, "data")

			if job_data then
				local job = cjson.decode(job_data)
				local pending_key = "{q}:pending:" .. job.queue

				redis.call("HSET", job_key, "state", "pending")
        redis.call("HDEL", job_key, "lease_token")
        redis.call("RPUSH", pending_key, job_data)
			end

			redis.call("ZREM", active_key, job_id)
			recovered = recovered + 1
		end

		return recovered
	`)

	result, err := script.Run(
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

	script := redis.NewScript(`
		local job_key = ARGV[1] .. ARGV[2]
		local job_id = ARGV[2]
		local lease_token = ARGV[3]
		local lease_until = ARGV[4]

		local current_token = redis.call("HGET", job_key, "lease_token")
		if not current_token or current_token ~= lease_token then
			return 0
		end

		local score = redis.call("ZSCORE", KEYS[1], job_id)
		if not score then
			return 0
		end

		if tonumber(score) <= tonumber(ARGV[5]) then
			return 0
		end

		redis.call("ZADD", KEYS[1], lease_until, job_id)
		return 1
	`)

	result, err := script.Run(
		ctx,
		b.client,
		[]string{"{q}:active"},
		"{q}:job:",
		jobID,
		leaseToken,
		leaseUntil,
		time.Now().UnixMilli(),
	).Int()
	if err != nil {
		return fmt.Errorf("renew lease for job %s: %w", jobID, err)
	}

	if result == 0 {
		return fmt.Errorf("cannot renew lease for job %s: lease expired or ownership lost", jobID)
	}

	return nil
}

func (r *RedisBroker) ReleaseLease(
	ctx context.Context,
	jobID string,
	leaseToken string,
) error {
	script := `
		local jobKey = KEYS[1]
		local activeKey = KEYS[2]

		local currentToken = redis.call("HGET", jobKey, "lease_token")

		if not currentToken or currentToken ~= ARGV[1] then
			return 0
		end

		local leaseUntil = redis.call("ZSCORE", activeKey, ARGV[2])

		if not leaseUntil or tonumber(leaseUntil) <= tonumber(ARGV[3]) then
			return 0
		end

		local jobData = redis.call("HGET", jobKey, "data")

		if not jobData then
			return 0
		end

		local job = cjson.decode(jobData)
		local queueName = job.queue or "default"
		local pendingKey = "{q}:pending:" .. queueName

		redis.call("RPUSH", pendingKey, jobData)
		redis.call("ZREM", activeKey, ARGV[2])
		redis.call("DEL", jobKey)

		return 1
	`

	result, err := r.client.Eval(
		ctx,
		script,
		[]string{
			"{q}:job:" + jobID,
			"{q}:active",
		},
		leaseToken,
		jobID,
		time.Now().UnixMilli(),
	).Int()

	if err != nil {
		return err
	}

	if result == 0 {
		return fmt.Errorf(
			"cannot release lease for job %s: lease expired or ownership lost",
			jobID,
		)
	}

	return nil
}
