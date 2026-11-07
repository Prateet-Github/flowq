package worker

import (
	"context"
	"log"
	"time"

	"github.com/Prateet-Github/flowq/internal/job"
	"github.com/Prateet-Github/flowq/internal/queue"
)

func StartHeartbeat(
	ctx context.Context,
	cancel context.CancelFunc,
	broker queue.Broker,
	j *job.Job,
	interval time.Duration,
) func() {
	stop := make(chan struct{})
	done := make(chan struct{})

	if interval <= 0 {
		panic("heartbeat interval must be positive")
	}

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
				if err := broker.RenewLease(ctx, j.ID, j.LeaseToken); err != nil {
					log.Printf("heartbeat failed for job %s: %v", j.ID, err)
					cancel()
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

func StartWorkerHeartbeat(
	ctx context.Context,
	broker queue.Broker,
	workerID string,
	getInFlight func() []string,
	interval time.Duration,
	ttl time.Duration,
) func() {
	done := make(chan struct{})

	if interval <= 0 || ttl <= 0 {
		panic("worker heartbeat interval and TTL must be positive")
	}

	go func() {
		defer close(done)

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		heartbeat := func() {
			if err := broker.HeartbeatWorker(
				ctx,
				workerID,
				getInFlight(),
				ttl,
			); err != nil {
				log.Printf("worker heartbeat failed for %s: %v", workerID, err)
			}
		}

		heartbeat()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				heartbeat()
			}
		}
	}()

	return func() {
		<-done
	}
}
