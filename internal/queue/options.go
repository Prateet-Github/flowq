package queue

import "time"

type EnqueueOptions struct {
	delay     time.Duration
	processAt *time.Time
}

type EnqueueOption func(*EnqueueOptions)

func ProcessIn(duration time.Duration) EnqueueOption {
	return func(opts *EnqueueOptions) {
		opts.delay = duration
		opts.processAt = nil
	}
}

func ProcessAt(timestamp time.Time) EnqueueOption {
	return func(opts *EnqueueOptions) {
		opts.processAt = &timestamp
	}
}
