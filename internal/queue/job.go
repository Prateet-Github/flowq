package queue

import "time"

type Job struct {
	ID       string        `json:"id"`
	Type     string        `json:"type"`
	Payload  []byte        `json:"payload"`
	Queue    string        `json:"queue"`
	MaxRetry int           `json:"max_retry"`
	Timeout  time.Duration `json:"timeout"`
}
