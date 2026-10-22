package queue

import (
	"context"
	"log"
	"sync"
)

type WorkerPool struct {
	workers int
	mux     *ServeMux
	jobs    chan *Job
}

func NewWorkerPool(workers int, mux *ServeMux) *WorkerPool {
	return &WorkerPool{
		workers: workers,
		mux:     mux,
		jobs:    make(chan *Job),
	}
}

func (p *WorkerPool) Start(ctx context.Context) {
	var wg sync.WaitGroup

	for i := 0; i < p.workers; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for {
				select {
				case <-ctx.Done():
					return

				case job, ok := <-p.jobs:
					if !ok {
						return
					}

					if err := p.mux.Process(ctx, job); err != nil {
						log.Printf("job %s failed: %v", job.ID, err)
					}
				}
			}
		}()
	}

	<-ctx.Done()
	close(p.jobs)
	wg.Wait()
}

func (p *WorkerPool) Submit(ctx context.Context, job *Job) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case p.jobs <- job:
		return nil
	}
}
