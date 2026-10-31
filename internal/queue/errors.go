package queue

import "errors"

var (
	ErrRetryable    = errors.New("queue: retryable error")
	ErrNonRetryable = errors.New("queue: non-retryable error")
)
