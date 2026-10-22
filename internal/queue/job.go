package queue

import (
	"time"

	"github.com/google/uuid"
)

type Job struct {
	ID       string        `json:"id"`
	Type     string        `json:"type"`
	Payload  []byte        `json:"payload"`
	Queue    string        `json:"queue"`
	MaxRetry int           `json:"max_retry"`
	Timeout  time.Duration `json:"timeout"`
}

func NewJob(jobType string, payload []byte) *Job {
	return &Job{
		ID:       uuid.NewString(),
		Type:     jobType,
		Payload:  payload,
		Queue:    "default",
		MaxRetry: 3,
		Timeout:  30 * time.Second,
	}
}
