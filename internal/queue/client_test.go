package queue

import (
	"context"
	"errors"
	"github.com/Prateet-Github/flowq/internal/job"
	"testing"
	"time"
)

type mockBroker struct {
	enqueueErr error
}

func (m *mockBroker) Enqueue(ctx context.Context, job *job.Job) error {
	return m.enqueueErr
}

func (m *mockBroker) Dequeue(ctx context.Context, queueName string) (*job.Job, error) {
	return nil, nil
}

func (m *mockBroker) Ack(ctx context.Context, jobID, leaseToken string) error {
	return nil
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

func TestClientEnqueue(t *testing.T) {
	t.Run("successful enqueue", func(t *testing.T) {
		client := NewClient(&mockBroker{})

		err := client.Enqueue(context.Background(), job.NewJob("send_email", []byte(`{}`)))
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
	})

	t.Run("broker error", func(t *testing.T) {
		expectedErr := errors.New("redis unavailable")
		client := NewClient(&mockBroker{enqueueErr: expectedErr})

		err := client.Enqueue(context.Background(), job.NewJob("send_email", []byte(`{}`)))
		if !errors.Is(err, expectedErr) {
			t.Fatalf("expected %v, got %v", expectedErr, err)
		}
	})

	t.Run("nil job", func(t *testing.T) {
		client := NewClient(&mockBroker{})

		err := client.Enqueue(context.Background(), nil)
		if err == nil {
			t.Fatal("expected error for nil job")
		}
	})
}

func (m *mockBroker) ScheduleRetry(
	ctx context.Context,
	job *job.Job,
	delay time.Duration,
) error {
	return nil
}

func (m *mockBroker) PromoteRetries(
	ctx context.Context,
	limit int,
) (int, error) {
	return 0, nil
}
