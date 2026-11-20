package job

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

func TestJobValidate(t *testing.T) {
	tests := []struct {
		name    string
		job     *Job
		wantErr bool
	}{
		{
			name:    "nil job",
			job:     nil,
			wantErr: true,
		},
		{
			name:    "empty ID",
			job:     &Job{Type: "task", Queue: "default"},
			wantErr: true,
		},
		{
			name:    "empty type",
			job:     &Job{ID: "job-1", Queue: "default"},
			wantErr: true,
		},
		{
			name:    "empty queue",
			job:     &Job{ID: "job-1", Type: "task"},
			wantErr: true,
		},
		{
			name:    "whitespace ID",
			job:     &Job{ID: "  ", Type: "task", Queue: "default"},
			wantErr: true,
		},
		{
			name:    "whitespace type",
			job:     &Job{ID: "job-1", Type: "  ", Queue: "default"},
			wantErr: true,
		},
		{
			name:    "whitespace queue",
			job:     &Job{ID: "job-1", Type: "task", Queue: "  "},
			wantErr: true,
		},
		{
			name: "valid job",
			job: &Job{
				ID:    "job-1",
				Type:  "task",
				Queue: "default",
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.job.Validate()

			if tt.wantErr && err == nil {
				t.Fatal("expected validation error")
			}

			if !tt.wantErr && err != nil {
				t.Fatalf("unexpected validation error: %v", err)
			}
		})
	}
}
