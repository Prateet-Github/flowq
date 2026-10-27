package queue

import (
	"context"
	"log"
	"time"
)

type Recovery interface {
	RecoverExpired(ctx context.Context) (int, error)
}

func StartRecovery(ctx context.Context, broker Recovery, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			recovered, err := broker.RecoverExpired(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("job recovery failed: %v", err)
				continue
			}

			if recovered > 0 {
				log.Printf("recovered %d expired job(s)", recovered)
			}
		}
	}
}
