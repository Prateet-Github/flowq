package broker

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/Prateet-Github/flowq/internal/queue"
	"github.com/redis/go-redis/v9"
)

func setupRedisBroker(t *testing.T) (*RedisBroker, context.Context) {
	t.Helper()

	ctx := context.Background()
	client := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
		DB:   15,
	})

	if err := client.Ping(ctx).Err(); err != nil {
		client.Close()

		if os.Getenv("CI") == "true" {
			t.Fatalf("Redis is required in CI but unavailable: %v", err)
		}

		t.Skipf("Redis is not available: %v", err)
	}

	if err := client.FlushDB(ctx).Err(); err != nil {
		client.Close()
		t.Fatalf("failed to flush test database: %v", err)
	}

	t.Cleanup(func() {
		client.Close()
	})

	return NewRedisBroker(client), ctx
}

func TestEnqueueDequeueAck(t *testing.T) {
	broker, ctx := setupRedisBroker(t)
	queueName := "test-" + time.Now().Format("150405.000000000")
	pendingKey := "{q}:pending:" + queueName

	t.Cleanup(func() {
		broker.client.Del(ctx, pendingKey)
	})

	original := queue.NewJob("send_email", []byte(`{"to":"test@example.com"}`))
	original.Queue = queueName

	if err := broker.Enqueue(ctx, original); err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	job, err := broker.Dequeue(ctx, queueName)
	if err != nil {
		t.Fatalf("dequeue failed: %v", err)
	}
	if job == nil {
		t.Fatal("expected a job, got nil")
	}
	if job.ID != original.ID {
		t.Fatalf("expected job ID %s, got %s", original.ID, job.ID)
	}
	if job.LeaseToken == "" {
		t.Fatal("expected a lease token")
	}

	var payload map[string]string
	if err := json.Unmarshal(job.Payload, &payload); err != nil {
		t.Fatalf("invalid payload: %v", err)
	}
	if payload["to"] != "test@example.com" {
		t.Fatalf("unexpected payload: %v", payload)
	}

	if err := broker.Ack(ctx, job.ID, job.LeaseToken); err != nil {
		t.Fatalf("ack failed: %v", err)
	}

	jobKey := "{q}:job:" + job.ID
	if exists := broker.client.Exists(ctx, jobKey).Val(); exists != 0 {
		t.Fatal("expected acknowledged job to be removed")
	}

	if active := broker.client.ZScore(ctx, "{q}:active", job.ID).Err(); active == nil {
		t.Fatal("expected acknowledged job to be removed from active set")
	} else if active != redis.Nil {
		t.Fatalf("failed checking active set: %v", active)
	}
}

func TestDequeueEmptyQueue(t *testing.T) {
	broker, ctx := setupRedisBroker(t)
	queueName := "empty-" + time.Now().Format("150405.000000000")

	job, err := broker.Dequeue(ctx, queueName)
	if err != nil {
		t.Fatalf("expected no error for empty queue, got %v", err)
	}
	if job != nil {
		t.Fatalf("expected nil job, got %+v", job)
	}
}

func TestAckRejectsInvalidLeaseToken(t *testing.T) {
	broker, ctx := setupRedisBroker(t)

	queueName := "lease-test-" + time.Now().Format("150405.000000000")
	pendingKey := "{q}:pending:" + queueName

	t.Cleanup(func() {
		broker.client.Del(ctx, pendingKey)
	})

	job := queue.NewJob("send_email", []byte(`{}`))
	job.Queue = queueName

	if err := broker.Enqueue(ctx, job); err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	dequeued, err := broker.Dequeue(ctx, queueName)
	if err != nil {
		t.Fatalf("dequeue failed: %v", err)
	}
	if dequeued == nil {
		t.Fatal("expected a job, got nil")
	}

	err = broker.Ack(ctx, dequeued.ID, "invalid-token")
	if err == nil {
		t.Fatal("expected acknowledgement with invalid token to fail")
	}

	exists, err := broker.client.Exists(ctx, "{q}:job:"+dequeued.ID).Result()
	if err != nil {
		t.Fatalf("failed checking job existence: %v", err)
	}
	if exists != 1 {
		t.Fatal("expected job to remain stored after invalid acknowledgement")
	}

	if err := broker.Ack(ctx, dequeued.ID, dequeued.LeaseToken); err != nil {
		t.Fatalf("cleanup acknowledgement failed: %v", err)
	}
}

func TestRecoverExpired(t *testing.T) {
	broker, ctx := setupRedisBroker(t)

	queueName := "recovery-test-" + time.Now().Format("150405.000000000")
	pendingKey := "{q}:pending:" + queueName

	t.Cleanup(func() {
		broker.client.Del(ctx, pendingKey)
	})

	job := queue.NewJob("send_email", []byte(`{}`))
	job.Queue = queueName

	if err := broker.Enqueue(ctx, job); err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	dequeued, err := broker.Dequeue(ctx, queueName)
	if err != nil {
		t.Fatalf("dequeue failed: %v", err)
	}
	if dequeued == nil {
		t.Fatal("expected a job, got nil")
	}

	// Force the lease to expire.
	if err := broker.client.ZAdd(ctx, "{q}:active", redis.Z{
		Score:  float64(time.Now().Add(-time.Second).UnixMilli()),
		Member: dequeued.ID,
	}).Err(); err != nil {
		t.Fatalf("failed to expire lease: %v", err)
	}

	recovered, err := broker.RecoverExpired(ctx)
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}
	if recovered < 1 {
		t.Fatalf("expected at least 1 recovered job, got %d", recovered)
	}

	pending, err := broker.client.LRange(ctx, pendingKey, 0, -1).Result()
	if err != nil {
		t.Fatalf("failed to inspect pending queue: %v", err)
	}
	if len(pending) != 1 {
		t.Fatalf("expected 1 pending job after recovery, got %d", len(pending))
	}

	activeScore, err := broker.client.ZScore(ctx, "{q}:active", dequeued.ID).Result()
	if err == nil {
		t.Fatalf("expected job to leave active set, got score %v", activeScore)
	}
	if err != redis.Nil {
		t.Fatalf("failed checking active set: %v", err)
	}

	requeued, err := broker.Dequeue(ctx, queueName)
	if err != nil {
		t.Fatalf("dequeue after recovery failed: %v", err)
	}
	if requeued == nil || requeued.ID != job.ID {
		t.Fatalf("expected recovered job %s, got %+v", job.ID, requeued)
	}

	if err := broker.Ack(ctx, requeued.ID, requeued.LeaseToken); err != nil {
		t.Fatalf("cleanup acknowledgement failed: %v", err)
	}
}

func TestReleaseLease(t *testing.T) {
	broker, ctx := setupRedisBroker(t)

	queueName := "release-test-" + time.Now().Format("150405.000000000")
	pendingKey := "{q}:pending:" + queueName

	t.Cleanup(func() {
		broker.client.Del(ctx, pendingKey)
	})

	job := queue.NewJob("send_email", []byte(`{}`))
	job.Queue = queueName

	if err := broker.Enqueue(ctx, job); err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	dequeued, err := broker.Dequeue(ctx, queueName)
	if err != nil {
		t.Fatalf("dequeue failed: %v", err)
	}
	if dequeued == nil {
		t.Fatal("expected a job, got nil")
	}

	if err := broker.ReleaseLease(ctx, dequeued.ID, dequeued.LeaseToken); err != nil {
		t.Fatalf("release lease failed: %v", err)
	}

	pending, err := broker.client.LRange(ctx, pendingKey, 0, -1).Result()
	if err != nil {
		t.Fatalf("failed to inspect pending queue: %v", err)
	}
	if len(pending) != 1 {
		t.Fatalf("expected 1 pending job, got %d", len(pending))
	}

	activeExists, err := broker.client.ZScore(ctx, "{q}:active", dequeued.ID).Result()
	if err == nil {
		t.Fatalf("expected job to leave active set, got score %v", activeExists)
	}
	if err != redis.Nil {
		t.Fatalf("failed checking active set: %v", err)
	}

	jobExists, err := broker.client.Exists(ctx, "{q}:job:"+dequeued.ID).Result()
	if err != nil {
		t.Fatalf("failed checking job record: %v", err)
	}
	if jobExists != 0 {
		t.Fatal("expected job record to be removed")
	}
}

func TestRenewLease(t *testing.T) {
	broker, ctx := setupRedisBroker(t)

	queueName := "renew-test-" + time.Now().Format("150405.000000000")
	pendingKey := "{q}:pending:" + queueName

	t.Cleanup(func() {
		broker.client.Del(ctx, pendingKey)
	})

	job := queue.NewJob("send_email", []byte(`{}`))
	job.Queue = queueName

	if err := broker.Enqueue(ctx, job); err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	dequeued, err := broker.Dequeue(ctx, queueName)
	if err != nil {
		t.Fatalf("dequeue failed: %v", err)
	}
	if dequeued == nil {
		t.Fatal("expected a job, got nil")
	}

	activeKey := "{q}:active"
	before, err := broker.client.ZScore(ctx, activeKey, dequeued.ID).Result()
	if err != nil {
		t.Fatalf("failed to get initial lease expiry: %v", err)
	}

	time.Sleep(10 * time.Millisecond)

	if err := broker.RenewLease(ctx, dequeued.ID, dequeued.LeaseToken); err != nil {
		t.Fatalf("renew lease failed: %v", err)
	}

	after, err := broker.client.ZScore(ctx, activeKey, dequeued.ID).Result()
	if err != nil {
		t.Fatalf("failed to get renewed lease expiry: %v", err)
	}
	if after <= before {
		t.Fatalf("expected lease expiry to increase: before=%v, after=%v", before, after)
	}

	if err := broker.Ack(ctx, dequeued.ID, dequeued.LeaseToken); err != nil {
		t.Fatalf("cleanup acknowledgement failed: %v", err)
	}
}
