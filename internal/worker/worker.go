package worker

import (
	"context"

	"errors"

	"github.com/Prateet-Github/flowq/internal/handler"
	"github.com/Prateet-Github/flowq/internal/queue"
	"github.com/Prateet-Github/flowq/internal/retry"

	"log"
	"sync"
	"time"
)

type WorkerPool struct {
	workers   int
	mux       *handler.ServeMux
	broker    queue.Broker
	queueName string
}

const ShutdownTimeout = 30 * time.Second

func NewWorkerPool(workers int, broker queue.Broker, mux *handler.ServeMux, queueName string) *WorkerPool {
	return &WorkerPool{
		workers:   workers,
		broker:    broker,
		mux:       mux,
		queueName: queueName,
	}
}

func waitContext(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// start the worker pool then dequeue then process the job
func (p *WorkerPool) Start(ctx context.Context) {
	var wg sync.WaitGroup

	shutdownCtx, cancelShutdown := context.WithCancel(context.Background())
	defer cancelShutdown()

	for i := 0; i < p.workers; i++ {
		wg.Go(func() {
			for {
				if ctx.Err() != nil {
					return
				}

				_, err := p.broker.PromoteRetries(ctx, 100)
				if err != nil {
					log.Printf("failed to promote delayed retries: %v", err)
				}

				job, err := p.broker.Dequeue(ctx, p.queueName)
				if err != nil {
					if ctx.Err() != nil {
						return
					}

					log.Printf("dequeue failed: %v", err)
					if !waitContext(ctx, time.Second) {
						return
					}
					continue
				}

				if job == nil {
					if !waitContext(ctx, 100*time.Millisecond) {
						return
					}
					continue
				}

				if ctx.Err() != nil {
					releaseCtx, cancel := context.WithTimeout(
						context.Background(),
						5*time.Second,
					)

					if err := p.broker.ReleaseLease(
						releaseCtx,
						job.ID,
						job.LeaseToken,
					); err != nil {
						log.Printf("failed to release job %s: %v", job.ID, err)
					}

					cancel()
					return
				}

				jobTimeout := job.Timeout
				if jobTimeout <= 0 {
					jobTimeout = 30 * time.Second
				}

				jobCtx, cancelJob := context.WithTimeout(
					context.WithoutCancel(ctx),
					jobTimeout,
				)

				jobDone := make(chan struct{})
				go func() {
					select {
					case <-shutdownCtx.Done():
						cancelJob()
					case <-jobDone:
					}
				}()

				stopHeartbeat := StartHeartbeat(
					jobCtx,
					cancelJob,
					p.broker,
					job,
					10*time.Second,
				)

				processErr := p.mux.Process(jobCtx, job)
				jobErr := jobCtx.Err()

				close(jobDone)
				stopHeartbeat()
				cancelJob()

				if jobErr != nil {
					log.Printf("job %s interrupted; releasing its lease", job.ID)

					releaseCtx, cancel := context.WithTimeout(
						context.Background(),
						5*time.Second,
					)

					if err := p.broker.ReleaseLease(
						releaseCtx,
						job.ID,
						job.LeaseToken,
					); err != nil {
						log.Printf("failed to release job %s: %v", job.ID, err)
					}

					cancel()
					continue
				}

				if processErr != nil {
					switch {
					case errors.Is(processErr, retry.ErrNonRetryable):
						log.Printf("job %s failed permanently: %v", job.ID, processErr)

						dlqCtx, cancel := context.WithTimeout(
							context.Background(),
							5*time.Second,
						)

						if err := p.broker.Dlq(dlqCtx, job.ID, job.LeaseToken); err != nil {
							log.Printf("failed to move job %s to DLQ: %v", job.ID, err)
						}

						cancel()
						continue

					case errors.Is(processErr, retry.ErrRetryable):
						job.Attempts++

						if job.Attempts > job.MaxRetry {
							log.Printf(
								"job %s exhausted retries after %d attempts: %v",
								job.ID,
								job.Attempts,
								processErr,
							)

							dlqCtx, cancel := context.WithTimeout(
								context.Background(),
								5*time.Second,
							)

							if err := p.broker.Dlq(dlqCtx, job.ID, job.LeaseToken); err != nil {
								log.Printf("failed to move job %s to DLQ: %v", job.ID, err)
							}

							cancel()
							continue
						}

						delay := retry.RetryDelay(job.Attempts)

						retryCtx, cancel := context.WithTimeout(
							context.Background(),
							5*time.Second,
						)

						err := p.broker.ScheduleRetry(retryCtx, job, delay)
						cancel()

						if err != nil {
							log.Printf("failed to schedule retry for job %s: %v", job.ID, err)

							releaseCtx, cancel := context.WithTimeout(
								context.Background(),
								5*time.Second,
							)

							if releaseErr := p.broker.ReleaseLease(
								releaseCtx,
								job.ID,
								job.LeaseToken,
							); releaseErr != nil {
								log.Printf("failed to release job %s: %v", job.ID, releaseErr)
							}

							cancel()
							continue
						}

						// Removing the currently active job after scheduling its retry
						ackCtx, cancel := context.WithTimeout(
							context.Background(),
							5*time.Second,
						)

						if err := p.broker.Ack(ackCtx, job.ID, job.LeaseToken); err != nil {
							log.Printf("failed to acknowledge retried job %s: %v", job.ID, err)
						}

						cancel()

					default:
						log.Printf("job %s failed with unclassified error: %v", job.ID, processErr)

						releaseCtx, cancel := context.WithTimeout(
							context.Background(),
							5*time.Second,
						)

						if err := p.broker.ReleaseLease(
							releaseCtx,
							job.ID,
							job.LeaseToken,
						); err != nil {
							log.Printf("failed to release job %s: %v", job.ID, err)
						}

						cancel()
					}

					continue
				}

				ackCtx, cancel := context.WithTimeout(
					context.Background(),
					5*time.Second,
				)

				if err := p.broker.Ack(
					ackCtx,
					job.ID,
					job.LeaseToken,
				); err != nil {
					log.Printf("ack failed for job %s: %v", job.ID, err)
				}

				cancel()
			}
		})
	}

	<-ctx.Done()
	log.Println("Shutdown signal received; waiting for in-flight jobs.")

	graceCtx, cancelGrace := context.WithTimeout(
		context.Background(),
		ShutdownTimeout,
	)
	defer cancelGrace()

	done := make(chan struct{})

	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Println("All workers stopped. Shutting down.")

	case <-graceCtx.Done():
		log.Println("Shutdown timeout exceeded; cancelling remaining jobs.")
		cancelShutdown()

		cleanupTimer := time.NewTimer(5 * time.Second)
		defer cleanupTimer.Stop()

		select {
		case <-done:
			log.Println("All workers stopped. Shutting down.")
		case <-cleanupTimer.C:
			log.Println("Cleanup deadline exceeded; some workers are still exiting.")
		}
	}
}
