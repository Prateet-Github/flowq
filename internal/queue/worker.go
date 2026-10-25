package queue

import (
	"context"
	"log"
	"sync"
	"time"
)

type WorkerPool struct {
	workers   int
	mux       *ServeMux
	broker    Broker
	queueName string
}

func NewWorkerPool(workers int, broker Broker, mux *ServeMux, queueName string) *WorkerPool {
	return &WorkerPool{
		workers:   workers,
		broker:    broker,
		mux:       mux,
		queueName: queueName,
	}
}

// start the worker pool then dequeue then process the job
func (p *WorkerPool) Start(ctx context.Context) {
	var wg sync.WaitGroup

	for i := 0; i < p.workers; i++ {

		wg.Go(func() {

			for {
				if ctx.Err() != nil {
					return
				}

				job, err := p.broker.Dequeue(ctx, p.queueName)
				if err != nil {
					if ctx.Err() != nil {
						return
					}

					log.Printf("dequeue failed: %v", err)
					time.Sleep(time.Second)
					continue
				}

				if job == nil {
					time.Sleep(100 * time.Millisecond)
					continue
				}

				if err := p.mux.Process(ctx, job); err != nil {
					log.Printf("job %s failed: %v", job.ID, err)
				}

				if err := p.broker.Ack(ctx, job.ID); err != nil {
					log.Printf("ack failed for job %s: %v", job.ID, err)
				}

			}
		})
	}

	<-ctx.Done()
	wg.Wait()
}
