package queue

import (
	"context"
	"log"
	"time"
)

func StartHeartbeat(
	ctx context.Context,
	broker Broker,
	job *Job,
	interval time.Duration,
) func() {
	stop := make(chan struct{})
	done := make(chan struct{})

	go func() {
		defer close(done)

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-stop:
				return
			case <-ticker.C:
				if err := broker.RenewLease(ctx, job.ID, job.LeaseToken); err != nil {
					log.Printf("heartbeat failed for job %s: %v", job.ID, err)
					return
				}
			}
		}
	}()

	return func() {
		close(stop)
		<-done
	}
}
