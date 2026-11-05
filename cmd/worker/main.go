package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Prateet-Github/flowq/internal/broker"
	"github.com/Prateet-Github/flowq/internal/handler"
	"github.com/Prateet-Github/flowq/internal/job"
	"github.com/Prateet-Github/flowq/internal/worker"

	"github.com/redis/go-redis/v9"
)

func main() {
	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer rdb.Close()

	redisBroker := broker.NewRedisBroker(rdb)

	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("Redis connection failed: %v", err)
	}

	go worker.StartRecovery(ctx, redisBroker, 5*time.Second)

	mux := handler.NewServeMux()
	mux.Handle("send_email", handler.HandlerFunc(func(ctx context.Context, job *job.Job) error {
		log.Printf("Processing job %s: %s", job.ID, job.Type)
		log.Printf("Payload: %s", job.Payload)

		select {
		case <-time.After(5 * time.Second):
			log.Printf("Finished job %s", job.ID)
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}))

	pool := worker.NewWorkerPool(10, redisBroker, mux, "default")

	log.Println("FlowQ worker started")
	pool.Start(ctx)
	log.Println("FlowQ worker stopped")
}
