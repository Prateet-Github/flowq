package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/Prateet-Github/flowq/internal/job"
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

	original := job.NewJob("send_email", []byte(`{"to":"test@example.com"}`))
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

	job := job.NewJob("send_email", []byte(`{}`))
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

	job := job.NewJob("send_email", []byte(`{}`))
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

	recoveredAgain, err := broker.RecoverExpired(ctx)
	if err != nil {
		t.Fatalf("second recovery failed: %v", err)
	}
	if recoveredAgain != 0 {
		t.Fatalf("expected 0 jobs on second recovery, got %d", recoveredAgain)
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

	if requeued.Attempts != 1 {
		t.Fatalf("expected attempts to be 1 after recovery, got %d", requeued.Attempts)
	}
}

func TestReleaseLease(t *testing.T) {
	broker, ctx := setupRedisBroker(t)

	queueName := "release-test-" + time.Now().Format("150405.000000000")
	pendingKey := "{q}:pending:" + queueName

	t.Cleanup(func() {
		broker.client.Del(ctx, pendingKey)
	})

	job := job.NewJob("send_email", []byte(`{}`))
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

	job := job.NewJob("send_email", []byte(`{}`))
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

func TestDlqMovesJobToDeadLetterQueue(t *testing.T) {
	broker, ctx := setupRedisBroker(t)

	queueName := "dlq-test-" + time.Now().Format("150405.000000000")
	pendingKey := "{q}:pending:" + queueName
	dlqKey := "{q}:dlq"
	activeKey := "{q}:active"

	job := job.NewJob("send_email", []byte(`{"to":"test@example.com"}`))
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

	if err := broker.Dlq(ctx, dequeued.ID, dequeued.LeaseToken); err != nil {
		t.Fatalf("dead-letter failed: %v", err)
	}

	// Verify the job was added to the dead-letter queue.
	dlqJobs, err := broker.client.LRange(ctx, dlqKey, 0, -1).Result()
	if err != nil {
		t.Fatalf("failed to inspect DLQ: %v", err)
	}
	if len(dlqJobs) != 1 {
		t.Fatalf("expected 1 job in DLQ, got %d", len(dlqJobs))
	}

	var stored struct {
		ID string `json:"ID"`
	}

	if err := json.Unmarshal([]byte(dlqJobs[0]), &stored); err != nil {
		t.Fatalf("failed to decode dead-lettered job: %v", err)
	}
	if stored.ID != dequeued.ID {
		t.Fatalf("expected DLQ job ID %s, got %s", dequeued.ID, stored.ID)
	}

	activeExists, err := broker.client.ZScore(ctx, activeKey, dequeued.ID).Result()
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
		t.Fatal("expected dead-lettered job record to be removed")
	}

	t.Cleanup(func() {
		broker.client.Del(ctx, pendingKey, dlqKey)
	})
}

func TestDlqRejectsInvalidLeaseToken(t *testing.T) {
	broker, ctx := setupRedisBroker(t)

	queueName := "dlq-invalid-lease-" + time.Now().Format("150405.000000000")
	pendingKey := "{q}:pending:" + queueName
	dlqKey := "{q}:dlq"

	job := job.NewJob("send_email", []byte(`{}`))
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

	err = broker.Dlq(ctx, dequeued.ID, "invalid-token")
	if err == nil {
		t.Fatal("expected dead-lettering with an invalid token to fail")
	}

	jobExists, err := broker.client.Exists(ctx, "{q}:job:"+dequeued.ID).Result()
	if err != nil {
		t.Fatalf("failed checking job record: %v", err)
	}
	if jobExists != 1 {
		t.Fatal("expected job record to remain after invalid lease token")
	}

	activeExists, err := broker.client.ZScore(ctx, "{q}:active", dequeued.ID).Result()
	if err != nil {
		t.Fatalf("expected job to remain active, got error: %v", err)
	}
	_ = activeExists

	dlqCount, err := broker.client.LLen(ctx, dlqKey).Result()
	if err != nil {
		t.Fatalf("failed checking DLQ: %v", err)
	}
	if dlqCount != 0 {
		t.Fatalf("expected empty DLQ, got %d jobs", dlqCount)
	}

	if err := broker.Ack(ctx, dequeued.ID, dequeued.LeaseToken); err != nil {
		t.Fatalf("cleanup acknowledgement failed: %v", err)
	}

	t.Cleanup(func() {
		broker.client.Del(ctx, pendingKey, dlqKey)
	})
}

func TestScheduleRetryAtomicallyRemovesActiveJob(t *testing.T) {
	broker, ctx := setupRedisBroker(t)

	queueName := "retry-test-" + time.Now().Format("150405.000000000")

	original := job.NewJob("send_email", []byte(`{"to":"test@example.com"}`))
	original.Queue = queueName

	if err := broker.Enqueue(ctx, original); err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	dequeued, err := broker.Dequeue(ctx, queueName)
	if err != nil {
		t.Fatalf("dequeue failed: %v", err)
	}
	if dequeued == nil {
		t.Fatal("expected a job")
	}

	if err := broker.ScheduleRetry(ctx, dequeued, time.Minute); err != nil {
		t.Fatalf("schedule retry failed: %v", err)
	}

	jobKey := "{q}:job:" + dequeued.ID

	data, err := broker.client.HGet(ctx, jobKey, "data").Result()
	if err != nil {
		t.Fatalf("expected job data to remain stored: %v", err)
	}

	var stored job.Job
	if err := json.Unmarshal([]byte(data), &stored); err != nil {
		t.Fatalf("failed to decode scheduled job: %v", err)
	}
	if stored.ID != dequeued.ID {
		t.Fatalf("expected job ID %s, got %s", dequeued.ID, stored.ID)
	}

	state, err := broker.client.HGet(ctx, jobKey, "state").Result()
	if err != nil {
		t.Fatalf("failed to read job state: %v", err)
	}
	if state != "scheduled" {
		t.Fatalf("expected state scheduled, got %s", state)
	}

	if _, err := broker.client.HGet(ctx, jobKey, "lease_token").Result(); err != redis.Nil {
		t.Fatalf("expected lease token to be removed, got %v", err)
	}

	if _, err := broker.client.ZScore(ctx, "{q}:scheduled", dequeued.ID).Result(); err != nil {
		t.Fatalf("expected job in scheduled set: %v", err)
	}

	if _, err := broker.client.ZScore(ctx, "{q}:active", dequeued.ID).Result(); err != redis.Nil {
		t.Fatalf("expected job to leave active set, got %v", err)
	}
}

func TestScheduleRetryRejectsInvalidLeaseToken(t *testing.T) {
	broker, ctx := setupRedisBroker(t)

	queueName := "retry-invalid-" + time.Now().Format("150405.000000000")

	original := job.NewJob("send_email", []byte(`{}`))
	original.Queue = queueName

	if err := broker.Enqueue(ctx, original); err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	dequeued, err := broker.Dequeue(ctx, queueName)
	if err != nil {
		t.Fatalf("dequeue failed: %v", err)
	}
	if dequeued == nil {
		t.Fatal("expected a job")
	}

	dequeued.LeaseToken = "invalid-token"

	err = broker.ScheduleRetry(ctx, dequeued, time.Minute)
	if err == nil {
		t.Fatal("expected retry scheduling with invalid token to fail")
	}

	if _, err := broker.client.ZScore(
		ctx,
		"{q}:active",
		dequeued.ID,
	).Result(); err != nil {
		t.Fatalf("expected job to remain active: %v", err)
	}

	exists, err := broker.client.Exists(
		ctx,
		"{q}:job:"+dequeued.ID,
	).Result()
	if err != nil {
		t.Fatalf("failed checking job record: %v", err)
	}
	if exists != 1 {
		t.Fatal("expected original job record to remain")
	}

	if _, err := broker.client.ZScore(
		ctx,
		"{q}:scheduled",
		dequeued.ID,
	).Result(); err != redis.Nil {
		t.Fatalf("expected job not to be scheduled: %v", err)
	}
}

func TestHeartbeatWorker(t *testing.T) {
	broker, _ := setupRedisBroker(t)
	ctx := context.Background()

	workerID := "test-worker-1"
	inFlight := []string{"job-1", "job-2"}
	ttl := 10 * time.Second

	err := broker.HeartbeatWorker(ctx, workerID, inFlight, ttl)
	if err != nil {
		t.Fatalf("heartbeat worker: %v", err)
	}

	key := "{q}:worker:" + workerID

	data, err := broker.client.HGetAll(ctx, key).Result()
	if err != nil {
		t.Fatalf("read worker heartbeat: %v", err)
	}

	if len(data) == 0 {
		t.Fatal("expected worker heartbeat data")
	}

	var got []string
	if err := json.Unmarshal([]byte(data["in_flight"]), &got); err != nil {
		t.Fatalf("unmarshal in-flight jobs: %v", err)
	}

	if !reflect.DeepEqual(got, inFlight) {
		t.Fatalf("expected in-flight jobs %v, got %v", inFlight, got)
	}

	remainingTTL, err := broker.client.TTL(ctx, key).Result()
	if err != nil {
		t.Fatalf("read worker TTL: %v", err)
	}

	if remainingTTL <= 0 {
		t.Fatal("expected worker heartbeat to have a TTL")
	}
}

func TestUnregisterWorker(t *testing.T) {
	broker, ctx := setupRedisBroker(t)

	workerID := "test-worker-cleanup"
	key := "{q}:worker:" + workerID

	err := broker.HeartbeatWorker(
		ctx,
		workerID,
		[]string{"job-1"},
		10*time.Second,
	)
	if err != nil {
		t.Fatalf("create worker heartbeat: %v", err)
	}

	exists, err := broker.client.Exists(ctx, key).Result()
	if err != nil {
		t.Fatalf("check worker key: %v", err)
	}
	if exists != 1 {
		t.Fatal("expected worker heartbeat key to exist")
	}

	if err := broker.UnregisterWorker(ctx, workerID); err != nil {
		t.Fatalf("unregister worker: %v", err)
	}

	exists, err = broker.client.Exists(ctx, key).Result()
	if err != nil {
		t.Fatalf("check deleted worker key: %v", err)
	}
	if exists != 0 {
		t.Fatal("expected worker heartbeat key to be deleted")
	}
}

func TestWorkerHeartbeatExpires(t *testing.T) {
	broker, ctx := setupRedisBroker(t)

	workerID := "test-worker-expiry"
	key := "{q}:worker:" + workerID

	err := broker.HeartbeatWorker(
		ctx,
		workerID,
		[]string{"job-1"},
		2*time.Second,
	)
	if err != nil {
		t.Fatalf("create worker heartbeat: %v", err)
	}

	exists, err := broker.client.Exists(ctx, key).Result()
	if err != nil {
		t.Fatalf("check worker key: %v", err)
	}
	if exists != 1 {
		t.Fatal("expected worker heartbeat key to exist")
	}

	time.Sleep(2500 * time.Millisecond)

	exists, err = broker.client.Exists(ctx, key).Result()
	if err != nil {
		t.Fatalf("check expired worker key: %v", err)
	}
	if exists != 0 {
		t.Fatal("expected worker heartbeat key to expire")
	}
}

func TestRecoverExpiredMovesExhaustedJobToDLQ(t *testing.T) {
	broker, ctx := setupRedisBroker(t)

	j := job.NewJob("send_email", []byte(`{}`))
	j.Queue = "recovery-dlq-test"
	j.MaxRetry = 0

	if err := broker.Enqueue(ctx, j); err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	dequeued, err := broker.Dequeue(ctx, j.Queue)
	if err != nil {
		t.Fatalf("dequeue failed: %v", err)
	}
	if dequeued == nil {
		t.Fatal("expected a job")
	}

	if err := broker.client.ZAdd(ctx, "{q}:active", redis.Z{
		Score:  float64(time.Now().Add(-time.Second).UnixMilli()),
		Member: j.ID,
	}).Err(); err != nil {
		t.Fatalf("failed to expire lease: %v", err)
	}

	recovered, err := broker.RecoverExpired(ctx)
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}
	if recovered != 1 {
		t.Fatalf("expected 1 recovered job, got %d", recovered)
	}

	dlqJobs, err := broker.client.LRange(ctx, "{q}:dlq", 0, -1).Result()
	if err != nil {
		t.Fatalf("failed to inspect DLQ: %v", err)
	}
	if len(dlqJobs) != 1 {
		t.Fatalf("expected 1 job in DLQ, got %d", len(dlqJobs))
	}

	var stored job.Job
	if err := json.Unmarshal([]byte(dlqJobs[0]), &stored); err != nil {
		t.Fatalf("failed to decode DLQ job: %v", err)
	}

	if stored.ID != j.ID {
		t.Fatalf("expected DLQ job %s, got %s", j.ID, stored.ID)
	}
	if stored.Attempts != 1 {
		t.Fatalf("expected 1 attempt, got %d", stored.Attempts)
	}

	exists, err := broker.client.Exists(ctx, "{q}:job:"+j.ID).Result()
	if err != nil {
		t.Fatalf("failed to check job record: %v", err)
	}
	if exists != 0 {
		t.Fatal("expected exhausted job record to be deleted")
	}

	if _, err := broker.client.ZScore(ctx, "{q}:active", j.ID).Result(); err != redis.Nil {
		t.Fatalf("expected job to leave active set, got %v", err)
	}
}

func TestRecoverExpiredUsesDefaultQueueWhenMissing(t *testing.T) {
	broker, ctx := setupRedisBroker(t)

	jobID := "recovery-missing-queue-test"
	jobKey := "{q}:job:" + jobID

	data := `{"id":"recovery-missing-queue-test","type":"task","attempts":0,"max_retry":3}`

	if err := broker.client.HSet(ctx, jobKey,
		"data", data,
		"state", "active",
	).Err(); err != nil {
		t.Fatalf("failed to create job record: %v", err)
	}

	if err := broker.client.ZAdd(ctx, "{q}:active", redis.Z{
		Score:  float64(time.Now().Add(-time.Second).UnixMilli()),
		Member: jobID,
	}).Err(); err != nil {
		t.Fatalf("failed to create expired lease: %v", err)
	}

	recovered, err := broker.RecoverExpired(ctx)
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}
	if recovered != 1 {
		t.Fatalf("expected 1 recovered job, got %d", recovered)
	}

	pending, err := broker.client.LRange(ctx, "{q}:pending:default", 0, -1).Result()
	if err != nil {
		t.Fatalf("failed to inspect default queue: %v", err)
	}
	if len(pending) != 1 {
		t.Fatalf("expected 1 job in default queue, got %d", len(pending))
	}

	var stored job.Job
	if err := json.Unmarshal([]byte(pending[0]), &stored); err != nil {
		t.Fatalf("failed to decode recovered job: %v", err)
	}

	if stored.Queue == "" {
		t.Fatal("expected recovered job to have a queue")
	}
	if stored.Attempts != 1 {
		t.Fatalf("expected 1 attempt, got %d", stored.Attempts)
	}
}

func TestRecoverExpiredPreservesLargeTimeout(t *testing.T) {
	broker, ctx := setupRedisBroker(t)

	j := job.NewJob("large-timeout-test", []byte(`{}`))
	j.Queue = "large-timeout-test"
	j.Timeout = 100000000000000 * time.Nanosecond

	if err := broker.Enqueue(ctx, j); err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	dequeued, err := broker.Dequeue(ctx, j.Queue)
	if err != nil {
		t.Fatalf("dequeue failed: %v", err)
	}
	if dequeued == nil {
		t.Fatal("expected a job")
	}

	if err := broker.client.ZAdd(ctx, "{q}:active", redis.Z{
		Score:  float64(time.Now().Add(-time.Second).UnixMilli()),
		Member: j.ID,
	}).Err(); err != nil {
		t.Fatalf("failed to expire lease: %v", err)
	}

	if _, err := broker.RecoverExpired(ctx); err != nil {
		t.Fatalf("recovery failed: %v", err)
	}

	recoveredData, err := broker.client.LIndex(
		ctx,
		"{q}:pending:"+j.Queue,
		0,
	).Result()
	if err != nil {
		t.Fatalf("failed to read recovered job: %v", err)
	}

	var recovered job.Job
	if err := json.Unmarshal([]byte(recoveredData), &recovered); err != nil {
		t.Fatalf("failed to decode recovered job: %v", err)
	}

	if recovered.Timeout != j.Timeout {
		t.Fatalf(
			"timeout changed: expected %d, got %d",
			j.Timeout,
			recovered.Timeout,
		)
	}

	if recovered.Attempts != 1 {
		t.Fatalf("expected 1 attempt, got %d", recovered.Attempts)
	}
}

func TestPromoteRetriesMovesDueJobToPending(t *testing.T) {
	broker, ctx := setupRedisBroker(t)

	queueName := "promote-due"

	j := job.NewJob("send_email", []byte(`{}`))
	j.Queue = queueName

	if err := broker.Enqueue(ctx, j); err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	dequeued, err := broker.Dequeue(ctx, queueName)
	if err != nil {
		t.Fatalf("dequeue failed: %v", err)
	}
	if dequeued == nil {
		t.Fatal("expected a job")
	}

	if err := broker.ScheduleRetry(ctx, dequeued, 0); err != nil {
		t.Fatalf("schedule retry failed: %v", err)
	}

	promoted, err := broker.PromoteRetries(ctx, 100)
	if err != nil {
		t.Fatalf("promote retries failed: %v", err)
	}
	if promoted != 1 {
		t.Fatalf("expected 1 promoted job, got %d", promoted)
	}

	pending, err := broker.client.LRange(
		ctx,
		"{q}:pending:"+queueName,
		0,
		-1,
	).Result()
	if err != nil {
		t.Fatalf("read pending queue: %v", err)
	}
	if len(pending) != 1 {
		t.Fatalf("expected 1 pending job, got %d", len(pending))
	}

	state, err := broker.client.HGet(
		ctx,
		"{q}:job:"+j.ID,
		"state",
	).Result()
	if err != nil {
		t.Fatalf("read job state: %v", err)
	}
	if state != "pending" {
		t.Fatalf("expected state pending, got %q", state)
	}

	if _, err := broker.client.ZScore(
		ctx,
		"{q}:scheduled",
		j.ID,
	).Result(); err != redis.Nil {
		t.Fatalf("expected job to be removed from scheduled set, got %v", err)
	}
}

func TestPromoteRetriesKeepsFutureJobScheduled(t *testing.T) {
	broker, ctx := setupRedisBroker(t)

	queueName := "promote-future"

	j := job.NewJob("send_email", []byte(`{}`))
	j.Queue = queueName

	if err := broker.Enqueue(ctx, j); err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	dequeued, err := broker.Dequeue(ctx, queueName)
	if err != nil {
		t.Fatalf("dequeue failed: %v", err)
	}
	if dequeued == nil {
		t.Fatal("expected a job")
	}

	if err := broker.ScheduleRetry(ctx, dequeued, time.Hour); err != nil {
		t.Fatalf("schedule retry failed: %v", err)
	}

	promoted, err := broker.PromoteRetries(ctx, 100)
	if err != nil {
		t.Fatalf("promote retries failed: %v", err)
	}
	if promoted != 0 {
		t.Fatalf("expected 0 promoted jobs, got %d", promoted)
	}

	if _, err := broker.client.ZScore(
		ctx,
		"{q}:scheduled",
		j.ID,
	).Result(); err != nil {
		t.Fatalf("expected job to remain scheduled: %v", err)
	}
}

func TestPromoteRetriesRemovesStaleEntries(t *testing.T) {
	broker, ctx := setupRedisBroker(t)

	jobID := "missing-job"

	if err := broker.client.ZAdd(
		ctx,
		"{q}:scheduled",
		redis.Z{
			Score:  0,
			Member: jobID,
		},
	).Err(); err != nil {
		t.Fatalf("add stale scheduled entry: %v", err)
	}

	promoted, err := broker.PromoteRetries(ctx, 100)
	if err != nil {
		t.Fatalf("promote retries failed: %v", err)
	}
	if promoted != 0 {
		t.Fatalf("expected 0 promoted jobs, got %d", promoted)
	}

	if _, err := broker.client.ZScore(
		ctx,
		"{q}:scheduled",
		jobID,
	).Result(); err != redis.Nil {
		t.Fatalf("expected stale entry to be removed, got %v", err)
	}
}

func TestPromoteRetriesRespectsBatchLimit(t *testing.T) {
	broker, ctx := setupRedisBroker(t)

	const totalJobs = 3

	for i := 0; i < totalJobs; i++ {
		jobID := fmt.Sprintf("scheduled-job-%d", i)
		jobKey := "{q}:job:" + jobID

		j := job.NewJob("send_email", []byte(`{}`))
		j.ID = jobID
		j.Queue = "promote-batch"

		data, err := json.Marshal(j)
		if err != nil {
			t.Fatalf("marshal job: %v", err)
		}

		if err := broker.client.HSet(
			ctx,
			jobKey,
			"data", string(data),
			"state", "scheduled",
		).Err(); err != nil {
			t.Fatalf("create job record: %v", err)
		}

		if err := broker.client.ZAdd(
			ctx,
			"{q}:scheduled",
			redis.Z{
				Score:  0,
				Member: jobID,
			},
		).Err(); err != nil {
			t.Fatalf("add scheduled job: %v", err)
		}
	}

	promoted, err := broker.PromoteRetries(ctx, 2)
	if err != nil {
		t.Fatalf("promote retries failed: %v", err)
	}
	if promoted != 2 {
		t.Fatalf("expected 2 promoted jobs, got %d", promoted)
	}

	remaining, err := broker.client.ZCard(ctx, "{q}:scheduled").Result()
	if err != nil {
		t.Fatalf("count scheduled jobs: %v", err)
	}
	if remaining != 1 {
		t.Fatalf("expected 1 scheduled job remaining, got %d", remaining)
	}

	pending, err := broker.client.LLen(
		ctx,
		"{q}:pending:promote-batch",
	).Result()
	if err != nil {
		t.Fatalf("count pending jobs: %v", err)
	}
	if pending != 2 {
		t.Fatalf("expected 2 pending jobs, got %d", pending)
	}
}
