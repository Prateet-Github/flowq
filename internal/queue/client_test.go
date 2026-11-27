package queue

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Prateet-Github/flowq/internal/job"
)

type mockBroker struct {
	enqueueErr     error
	enqueueCalls   int
	enqueueInCalls int
	enqueueAtCalls int
	lastDelay      time.Duration
	lastProcessAt  time.Time
}

func (m *mockBroker) Enqueue(ctx context.Context, j *job.Job) error {
	m.enqueueCalls++
	return m.enqueueErr
}

func (m *mockBroker) EnqueueIn(
	ctx context.Context,
	j *job.Job,
	delay time.Duration,
) error {
	m.enqueueInCalls++
	m.lastDelay = delay
	return m.enqueueErr
}

func (m *mockBroker) EnqueueAt(
	ctx context.Context,
	j *job.Job,
	at time.Time,
) error {
	m.enqueueAtCalls++
	m.lastProcessAt = at
	return m.enqueueErr
}

func (m *mockBroker) Dequeue(ctx context.Context, queueName string) (*job.Job, error) {
	return nil, nil
}

func (m *mockBroker) Ack(ctx context.Context, jobID, leaseToken string) error {
	return nil
}

func (m *mockBroker) LeaseDuration() time.Duration {
	return 30 * time.Second
}

func (m *mockBroker) RecoverExpired(ctx context.Context) (int, error) {
	return 0, nil
}

func (m *mockBroker) RenewLease(ctx context.Context, jobID, leaseToken string) error {
	return nil
}

func (m *mockBroker) ReleaseLease(ctx context.Context, jobID, leaseToken string) error {
	return nil
}

func (m *mockBroker) HeartbeatWorker(
	ctx context.Context,
	workerID string,
	inFlight []string,
	ttl time.Duration,
) error {
	return nil
}

func (m *mockBroker) ScheduleRetry(
	ctx context.Context,
	j *job.Job,
	delay time.Duration,
) error {
	return nil
}

func (m *mockBroker) PromoteScheduled(
	ctx context.Context,
	limit int,
) (int, error) {
	return 0, nil
}

func (m *mockBroker) Dlq(
	ctx context.Context,
	jobID string,
	leaseToken string,
) error {
	return nil
}

func (m *mockBroker) UnregisterWorker(
	ctx context.Context,
	workerID string,
) error {
	return nil
}

func TestClientEnqueue(t *testing.T) {
	t.Run("successful enqueue", func(t *testing.T) {
		broker := &mockBroker{}
		client := NewClient(broker)

		err := client.Enqueue(
			context.Background(),
			job.NewJob("send_email", []byte(`{}`)),
		)

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if broker.enqueueCalls != 1 {
			t.Fatalf("expected 1 broker call, got %d", broker.enqueueCalls)
		}
	})

	t.Run("broker error", func(t *testing.T) {
		expectedErr := errors.New("redis unavailable")
		client := NewClient(&mockBroker{enqueueErr: expectedErr})

		err := client.Enqueue(
			context.Background(),
			job.NewJob("send_email", []byte(`{}`)),
		)

		if !errors.Is(err, expectedErr) {
			t.Fatalf("expected %v, got %v", expectedErr, err)
		}
	})

	t.Run("invalid jobs", func(t *testing.T) {
		tests := []struct {
			name string
			job  *job.Job
		}{
			{
				name: "nil job",
				job:  nil,
			},
			{
				name: "empty ID",
				job: &job.Job{
					Type:  "task",
					Queue: "default",
				},
			},
			{
				name: "empty type",
				job: &job.Job{
					ID:    "job-1",
					Queue: "default",
				},
			},
			{
				name: "empty queue",
				job: &job.Job{
					ID:   "job-1",
					Type: "task",
				},
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				broker := &mockBroker{}
				client := NewClient(broker)

				err := client.Enqueue(context.Background(), tt.job)

				if err == nil {
					t.Fatal("expected validation error")
				}

				if broker.enqueueCalls != 0 {
					t.Fatalf(
						"expected broker not to be called, got %d calls",
						broker.enqueueCalls,
					)
				}
			})
		}
	})
}

func TestClientEnqueueOptions(t *testing.T) {
	t.Run("ProcessIn", func(t *testing.T) {
		broker := &mockBroker{}
		client := NewClient(broker)

		delay := 30 * time.Second
		err := client.Enqueue(
			context.Background(),
			job.NewJob("send_email", []byte(`{}`)),
			ProcessIn(delay),
		)

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if broker.enqueueInCalls != 1 {
			t.Fatalf("expected 1 EnqueueIn call, got %d", broker.enqueueInCalls)
		}
		if broker.lastDelay != delay {
			t.Fatalf("expected delay %v, got %v", delay, broker.lastDelay)
		}
	})

	t.Run("ProcessAt", func(t *testing.T) {
		broker := &mockBroker{}
		client := NewClient(broker)

		at := time.Now().Add(time.Hour).Truncate(time.Millisecond)
		err := client.Enqueue(
			context.Background(),
			job.NewJob("send_email", []byte(`{}`)),
			ProcessAt(at),
		)

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if broker.enqueueAtCalls != 1 {
			t.Fatalf("expected 1 EnqueueAt call, got %d", broker.enqueueAtCalls)
		}
		if !broker.lastProcessAt.Equal(at) {
			t.Fatalf("expected time %v, got %v", at, broker.lastProcessAt)
		}
	})

	t.Run("zero delay uses immediate enqueue", func(t *testing.T) {
		broker := &mockBroker{}
		client := NewClient(broker)

		err := client.Enqueue(
			context.Background(),
			job.NewJob("send_email", []byte(`{}`)),
			ProcessIn(0),
		)

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if broker.enqueueCalls != 1 {
			t.Fatalf("expected 1 immediate enqueue call, got %d", broker.enqueueCalls)
		}
	})

	t.Run("negative delay is rejected", func(t *testing.T) {
		broker := &mockBroker{}
		client := NewClient(broker)

		err := client.Enqueue(
			context.Background(),
			job.NewJob("send_email", []byte(`{}`)),
			ProcessIn(-time.Second),
		)

		if err == nil {
			t.Fatal("expected negative delay error")
		}
		if broker.enqueueCalls+broker.enqueueInCalls+broker.enqueueAtCalls != 0 {
			t.Fatal("broker should not be called for negative delay")
		}
	})
}
