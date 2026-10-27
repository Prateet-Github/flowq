package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Prateet-Github/flowq/internal/broker"
	"github.com/Prateet-Github/flowq/internal/queue"
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

	go queue.StartRecovery(ctx, redisBroker, 5*time.Second)

	mux := queue.NewServeMux()
	mux.Handle("send_email", queue.HandlerFunc(func(ctx context.Context, job *queue.Job) error {
		log.Printf("Processing job %s: %s", job.ID, job.Type)
		log.Printf("Payload: %s", job.Payload)
		return nil
	}))

	pool := queue.NewWorkerPool(10, redisBroker, mux, "default")

	log.Println("FlowQ worker started")
	pool.Start(ctx)
	log.Println("FlowQ worker stopped")
}
