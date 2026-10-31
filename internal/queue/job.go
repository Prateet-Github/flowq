package queue

import (
	"time"

	"github.com/google/uuid"
)

type Job struct {
	ID         string        `json:"id"`
	Type       string        `json:"type"`
	Payload    []byte        `json:"payload"`
	Queue      string        `json:"queue"`
	MaxRetry   int           `json:"max_retry"`
	Attempts   int           `json:"attempts"`
	Timeout    time.Duration `json:"timeout"`
	LeaseToken string        `json:"lease_token,omitempty"`
}

func NewJob(jobType string, payload []byte) *Job {
	return &Job{
		ID:       uuid.NewString(),
		Type:     jobType,
		Payload:  payload,
		Queue:    "default",
		MaxRetry: 3,
		Attempts: 0,
		Timeout:  30 * time.Second,
	}
}
