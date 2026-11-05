package worker

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/Prateet-Github/flowq/internal/handler"
	"github.com/Prateet-Github/flowq/internal/job"
	"github.com/Prateet-Github/flowq/internal/retry"
	"github.com/redis/go-redis/v9"

	"github.com/Prateet-Github/flowq/internal/broker"
)

type mockWorkerBroker struct {
	mu               sync.Mutex
	job              *job.Job
	dlqCalled        bool
	ackCalled        bool
	releaseCalled    bool
	retryCalled      bool
	retryJob         *job.Job
	scheduleRetryErr error
	renewCalls       int
}

func (m *mockWorkerBroker) Enqueue(context.Context, *job.Job) error {
	return nil
}

func (m *mockWorkerBroker) Dequeue(context.Context, string) (*job.Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.job == nil {
		return nil, nil
	}

	j := m.job
	m.job = nil
	return j, nil
}

func (m *mockWorkerBroker) Ack(context.Context, string, string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ackCalled = true
	return nil
}

func (m *mockWorkerBroker) LeaseDuration() time.Duration {
	return 30 * time.Second
}

func (m *mockWorkerBroker) RecoverExpired(context.Context) (int, error) {
	return 0, nil
}

func (m *mockWorkerBroker) RenewLease(
	context.Context,
	string,
	string,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.renewCalls++
	return nil
}

func (m *mockWorkerBroker) ReleaseLease(context.Context, string, string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.releaseCalled = true
	return nil
}

func (m *mockWorkerBroker) ScheduleRetry(
	_ context.Context,
	j *job.Job,
	_ time.Duration,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.retryCalled = true
	m.retryJob = j
	return m.scheduleRetryErr
}

func (m *mockWorkerBroker) PromoteRetries(context.Context, int) (int, error) {
	return 0, nil
}

func (m *mockWorkerBroker) Dlq(context.Context, string, string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.dlqCalled = true
	return nil
}

func TestWorkerMovesNonRetryableJobToDLQ(t *testing.T) {
	testJob := job.NewJob("send_email", []byte(`{}`))
	testJob.Queue = "default"
	testJob.LeaseToken = "test-lease-token"

	broker := &mockWorkerBroker{job: testJob}
	mux := handler.NewServeMux()

	mux.Handle("send_email", handler.HandlerFunc(func(
		context.Context,
		*job.Job,
	) error {
		return errors.Join(errors.New("invalid recipient"), retry.ErrNonRetryable)
	}))

	pool := NewWorkerPool(1, broker, mux, "default")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		defer close(done)
		pool.Start(ctx)
	}()

	// Wait for the worker to process the job.
	deadline := time.After(2 * time.Second)
	for {
		broker.mu.Lock()
		called := broker.dlqCalled
		broker.mu.Unlock()

		if called {
			break
		}

		select {
		case <-deadline:
			cancel()
			<-done
			t.Fatal("expected worker to move non-retryable job to DLQ")
		case <-time.After(10 * time.Millisecond):
		}
	}

	cancel()
	<-done

	broker.mu.Lock()
	defer broker.mu.Unlock()

	if !broker.dlqCalled {
		t.Fatal("expected Dlq to be called")
	}
	if broker.ackCalled {
		t.Fatal("expected worker not to acknowledge the job separately")
	}
	if broker.releaseCalled {
		t.Fatal("expected worker not to release the job")
	}
}

func TestWorkerMovesExhaustedRetriesToDLQ(t *testing.T) {
	testJob := job.NewJob("send_email", []byte(`{}`))
	testJob.Queue = "default"
	testJob.LeaseToken = "test-lease-token"
	testJob.MaxRetry = 1
	testJob.Attempts = 1

	broker := &mockWorkerBroker{job: testJob}
	mux := handler.NewServeMux()

	mux.Handle("send_email", handler.HandlerFunc(func(
		context.Context,
		*job.Job,
	) error {
		return errors.Join(errors.New("temporary failure"), retry.ErrRetryable)
	}))

	pool := NewWorkerPool(1, broker, mux, "default")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		defer close(done)
		pool.Start(ctx)
	}()

	deadline := time.After(2 * time.Second)
	for {
		broker.mu.Lock()
		called := broker.dlqCalled
		broker.mu.Unlock()

		if called {
			break
		}

		select {
		case <-deadline:
			cancel()
			<-done
			t.Fatal("expected exhausted job to be moved to DLQ")
		case <-time.After(10 * time.Millisecond):
		}
	}

	cancel()
	<-done

	broker.mu.Lock()
	defer broker.mu.Unlock()

	if !broker.dlqCalled {
		t.Fatal("expected Dlq to be called")
	}
	if broker.ackCalled {
		t.Fatal("expected worker not to acknowledge the job separately")
	}
	if broker.releaseCalled {
		t.Fatal("expected worker not to release the job")
	}
}

func TestWorkerSchedulesRetryWithoutDLQ(t *testing.T) {
	testJob := job.NewJob("send_email", []byte(`{}`))
	testJob.Queue = "default"
	testJob.LeaseToken = "test-lease-token"
	testJob.MaxRetry = 3
	testJob.Attempts = 0

	broker := &mockWorkerBroker{job: testJob}
	mux := handler.NewServeMux()

	mux.Handle("send_email", handler.HandlerFunc(func(
		context.Context,
		*job.Job,
	) error {
		return errors.Join(errors.New("temporary failure"), retry.ErrRetryable)
	}))

	pool := NewWorkerPool(1, broker, mux, "default")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		defer close(done)
		pool.Start(ctx)
	}()

	deadline := time.After(2 * time.Second)
	for {
		broker.mu.Lock()
		called := broker.retryCalled
		broker.mu.Unlock()

		if called {
			break
		}

		select {
		case <-deadline:
			cancel()
			<-done
			t.Fatal("expected retryable job to be scheduled for retry")
		case <-time.After(10 * time.Millisecond):
		}
	}

	cancel()
	<-done

	broker.mu.Lock()
	defer broker.mu.Unlock()

	if !broker.retryCalled {
		t.Fatal("expected ScheduleRetry to be called")
	}
	if broker.ackCalled {
		t.Fatal("expected worker not to acknowledge job separately after scheduling retry")
	}
	if broker.dlqCalled {
		t.Fatal("expected job not to be moved to DLQ")
	}
	if broker.releaseCalled {
		t.Fatal("expected job not to be released")
	}
	if broker.retryJob == nil {
		t.Fatal("expected retry job to be recorded")
	}
	if broker.retryJob.Attempts != 1 {
		t.Fatalf("expected 1 retry attempt, got %d", broker.retryJob.Attempts)
	}
}

func TestWorkerReleasesLeaseWhenRetrySchedulingFails(t *testing.T) {
	testJob := job.NewJob("send_email", []byte(`{}`))
	testJob.Queue = "default"
	testJob.LeaseToken = "test-lease-token"
	testJob.MaxRetry = 3

	broker := &mockWorkerBroker{
		job:              testJob,
		scheduleRetryErr: errors.New("Redis unavailable"),
	}

	mux := handler.NewServeMux()
	mux.Handle("send_email", handler.HandlerFunc(func(
		context.Context,
		*job.Job,
	) error {
		return errors.Join(errors.New("temporary failure"), retry.ErrRetryable)
	}))

	pool := NewWorkerPool(1, broker, mux, "default")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		defer close(done)
		pool.Start(ctx)
	}()

	deadline := time.After(2 * time.Second)
	for {
		broker.mu.Lock()
		released := broker.releaseCalled
		broker.mu.Unlock()

		if released {
			break
		}

		select {
		case <-deadline:
			cancel()
			<-done
			t.Fatal("expected worker to release lease after retry scheduling failed")
		case <-time.After(10 * time.Millisecond):
		}
	}

	cancel()
	<-done

	broker.mu.Lock()
	defer broker.mu.Unlock()

	if !broker.retryCalled {
		t.Fatal("expected ScheduleRetry to be called")
	}
	if !broker.releaseCalled {
		t.Fatal("expected ReleaseLease to be called")
	}
	if broker.dlqCalled {
		t.Fatal("expected job not to be moved to DLQ")
	}
	if broker.ackCalled {
		t.Fatal("expected job not to be acknowledged")
	}
}

func (m *mockWorkerBroker) HeartbeatWorker(
	ctx context.Context,
	workerID string,
	inFlight []string,
	ttl time.Duration,
) error {
	return nil
}

func (m *mockWorkerBroker) UnregisterWorker(
	ctx context.Context,
	workerID string,
) error {
	return nil
}

func TestWorkerRetriesUnclassifiedError(t *testing.T) {
	testJob := job.NewJob("send_email", []byte(`{}`))
	testJob.Queue = "default"
	testJob.LeaseToken = "test-lease-token"
	testJob.MaxRetry = 3

	broker := &mockWorkerBroker{job: testJob}
	mux := handler.NewServeMux()

	mux.Handle("send_email", handler.HandlerFunc(func(
		context.Context,
		*job.Job,
	) error {
		return errors.New("unexpected handler failure")
	}))

	pool := NewWorkerPool(1, broker, mux, "default")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		defer close(done)
		pool.Start(ctx)
	}()

	deadline := time.After(2 * time.Second)

	for {
		broker.mu.Lock()
		called := broker.retryCalled
		broker.mu.Unlock()

		if called {
			break
		}

		select {
		case <-deadline:
			cancel()
			<-done
			t.Fatal("expected unclassified error to be retried")
		case <-time.After(10 * time.Millisecond):
		}
	}

	cancel()
	<-done

	broker.mu.Lock()
	defer broker.mu.Unlock()

	if !broker.retryCalled {
		t.Fatal("expected ScheduleRetry to be called")
	}
	if broker.retryJob.Attempts != 1 {
		t.Fatalf("expected 1 attempt, got %d", broker.retryJob.Attempts)
	}
	if broker.dlqCalled || broker.ackCalled || broker.releaseCalled {
		t.Fatal("unexpected DLQ, ACK, or lease release")
	}
}

func TestWorkerRetriesHandlerPanic(t *testing.T) {
	testJob := job.NewJob("send_email", []byte(`{}`))
	testJob.Queue = "default"
	testJob.LeaseToken = "test-lease-token"
	testJob.MaxRetry = 3

	broker := &mockWorkerBroker{job: testJob}
	mux := handler.NewServeMux()

	mux.Handle("send_email", handler.HandlerFunc(func(
		context.Context,
		*job.Job,
	) error {
		panic("unexpected panic")
	}))

	pool := NewWorkerPool(1, broker, mux, "default")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		defer close(done)
		pool.Start(ctx)
	}()

	deadline := time.After(2 * time.Second)

	for {
		broker.mu.Lock()
		called := broker.retryCalled
		broker.mu.Unlock()

		if called {
			break
		}

		select {
		case <-deadline:
			cancel()
			<-done
			t.Fatal("expected handler panic to trigger retry")
		case <-time.After(10 * time.Millisecond):
		}
	}

	cancel()
	<-done

	broker.mu.Lock()
	defer broker.mu.Unlock()

	if !broker.retryCalled {
		t.Fatal("expected ScheduleRetry to be called")
	}
	if broker.retryJob.Attempts != 1 {
		t.Fatalf("expected 1 attempt, got %d", broker.retryJob.Attempts)
	}
	if broker.dlqCalled || broker.ackCalled || broker.releaseCalled {
		t.Fatal("unexpected DLQ, ACK, or lease release")
	}
}

func TestWorkerRetriesTimedOutJob(t *testing.T) {
	testJob := job.NewJob("send_email", []byte(`{}`))
	testJob.Queue = "default"
	testJob.LeaseToken = "test-lease-token"
	testJob.MaxRetry = 3
	testJob.Timeout = 50 * time.Millisecond

	broker := &mockWorkerBroker{job: testJob}
	mux := handler.NewServeMux()

	mux.Handle("send_email", handler.HandlerFunc(func(
		ctx context.Context,
		_ *job.Job,
	) error {
		<-ctx.Done()
		return ctx.Err()
	}))

	pool := NewWorkerPool(1, broker, mux, "default")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		defer close(done)
		pool.Start(ctx)
	}()

	deadline := time.After(2 * time.Second)

	for {
		broker.mu.Lock()
		called := broker.retryCalled
		broker.mu.Unlock()

		if called {
			break
		}

		select {
		case <-deadline:
			cancel()
			<-done
			t.Fatal("expected timed-out job to be retried")
		case <-time.After(10 * time.Millisecond):
		}
	}

	cancel()
	<-done

	broker.mu.Lock()
	defer broker.mu.Unlock()

	if !broker.retryCalled {
		t.Fatal("expected ScheduleRetry to be called")
	}
	if broker.retryJob.Attempts != 1 {
		t.Fatalf("expected 1 attempt, got %d", broker.retryJob.Attempts)
	}
	if broker.dlqCalled || broker.ackCalled || broker.releaseCalled {
		t.Fatal("unexpected DLQ, ACK, or lease release")
	}
}

func TestWorkerMovesUnknownJobTypeToDLQ(t *testing.T) {
	testJob := job.NewJob("unknown_type", []byte(`{}`))
	testJob.Queue = "default"
	testJob.LeaseToken = "test-lease-token"

	broker := &mockWorkerBroker{job: testJob}
	mux := handler.NewServeMux()

	pool := NewWorkerPool(1, broker, mux, "default")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		defer close(done)
		pool.Start(ctx)
	}()

	deadline := time.After(2 * time.Second)

	for {
		broker.mu.Lock()
		called := broker.dlqCalled
		broker.mu.Unlock()

		if called {
			break
		}

		select {
		case <-deadline:
			cancel()
			<-done
			t.Fatal("expected unknown job type to be moved to DLQ")
		case <-time.After(10 * time.Millisecond):
		}
	}

	cancel()
	<-done

	broker.mu.Lock()
	defer broker.mu.Unlock()

	if !broker.dlqCalled {
		t.Fatal("expected Dlq to be called")
	}
	if broker.retryCalled || broker.ackCalled || broker.releaseCalled {
		t.Fatal("unexpected retry, ACK, or lease release")
	}
}

func TestStartHeartbeatRenewsLease(t *testing.T) {
	broker := &mockWorkerBroker{}
	testJob := job.NewJob("send_email", []byte(`{}`))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stop := StartHeartbeat(
		ctx,
		cancel,
		broker,
		testJob,
		50*time.Millisecond,
	)

	time.Sleep(180 * time.Millisecond)
	stop()

	broker.mu.Lock()
	calls := broker.renewCalls
	broker.mu.Unlock()

	if calls < 2 {
		t.Fatalf("expected at least 2 lease renewals, got %d", calls)
	}
}

func TestStartHeartbeatRenewsRedisLease(t *testing.T) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   15,
	})
	defer rdb.Close()

	if err := rdb.Ping(ctx).Err(); err != nil {
		if os.Getenv("CI") == "true" {
			t.Fatalf("Redis unavailable in CI: %v", err)
		}
		t.Skipf("Redis unavailable: %v", err)
	}

	if err := rdb.FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}

	redisBroker := broker.NewRedisBroker(
		rdb,
		broker.WithLease(6*time.Second),
	)

	testJob := job.NewJob("heartbeat_test", []byte(`{}`))
	testJob.Queue = "default"

	if err := redisBroker.Enqueue(ctx, testJob); err != nil {
		t.Fatal(err)
	}

	dequeuedJob, err := redisBroker.Dequeue(ctx, "default")
	if err != nil {
		t.Fatal(err)
	}
	if dequeuedJob == nil {
		t.Fatal("expected job to be dequeued")
	}

	initialScore, err := rdb.ZScore(
		ctx,
		"{q}:active",
		dequeuedJob.ID,
	).Result()
	if err != nil {
		t.Fatal(err)
	}

	stop := StartHeartbeat(
		ctx,
		cancel,
		redisBroker,
		dequeuedJob,
		time.Second,
	)
	defer stop()

	time.Sleep(1500 * time.Millisecond)

	renewedScore, err := rdb.ZScore(
		ctx,
		"{q}:active",
		dequeuedJob.ID,
	).Result()
	if err != nil {
		t.Fatal(err)
	}

	if renewedScore <= initialScore {
		t.Fatalf(
			"expected lease expiry to increase: initial=%v renewed=%v",
			initialScore,
			renewedScore,
		)
	}

	t.Logf(
		"Lease successfully renewed: initial=%v renewed=%v",
		initialScore,
		renewedScore,
	)
}
