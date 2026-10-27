package queue

import "testing"

func TestNewJob(t *testing.T) {
	job := NewJob("send_email", []byte(`{"to":"test@example.com"}`))

	if job.ID == "" {
		t.Fatal("expected job ID to be generated")
	}

	if job.Type != "send_email" {
		t.Fatalf("expected type send_email, got %s", job.Type)
	}

	if job.Queue != "default" {
		t.Fatalf("expected queue default, got %s", job.Queue)
	}

	if job.MaxRetry != 3 {
		t.Fatalf("expected MaxRetry 3, got %d", job.MaxRetry)
	}

	if job.Timeout <= 0 {
		t.Fatalf("expected positive timeout, got %v", job.Timeout)
	}

	if len(job.Payload) == 0 {
		t.Fatal("expected payload to be set")
	}
}

func TestNewJobUniqueIDs(t *testing.T) {
	job1 := NewJob("task", nil)
	job2 := NewJob("task", nil)

	if job1.ID == job2.ID {
		t.Fatal("expected unique IDs, got identical IDs")
	}
}
