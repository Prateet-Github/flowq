package queue

import (
	"math"
	"math/rand/v2"
	"time"
)

const (
	BaseRetryDelay = time.Second
	MaxRetryDelay  = 5 * time.Minute
)

func RetryDelay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}

	capDelay := float64(BaseRetryDelay) * math.Pow(2, float64(attempt-1))
	if capDelay > float64(MaxRetryDelay) {
		capDelay = float64(MaxRetryDelay)
	}

	return time.Duration(rand.Int64N(int64(capDelay) + 1))
}
