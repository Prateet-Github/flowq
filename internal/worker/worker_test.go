package worker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Prateet-Github/flowq/internal/handler"
	"github.com/Prateet-Github/flowq/internal/job"
	"github.com/Prateet-Github/flowq/internal/retry"
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

func (m *mockWorkerBroker) RecoverExpired(context.Context) (int, error) {
	return 0, nil
}

func (m *mockWorkerBroker) RenewLease(context.Context, string, string) error {
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
