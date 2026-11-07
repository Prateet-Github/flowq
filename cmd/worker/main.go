package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Prateet-Github/flowq/internal/broker"
	"github.com/Prateet-Github/flowq/internal/config"
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

	cfg := config.Load()

	rdb := redis.NewClient(&redis.Options{
		Addr: cfg.RedisAddr,
		DB:   cfg.RedisDB,
	})
	defer rdb.Close()

	redisBroker := broker.NewRedisBroker(
		rdb,
		broker.WithLease(cfg.Lease),
	)

	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("Redis connection failed: %v", err)
	}

	go worker.StartRecovery(ctx, redisBroker, cfg.RecoveryInterval)

	mux := handler.NewServeMux()
	mux.Handle("send_email", handler.HandlerFunc(func(
		ctx context.Context,
		j *job.Job,
	) error {
		log.Printf("Processing job %s: %s", j.ID, j.Type)
		log.Printf("Payload: %s", j.Payload)

		select {
		case <-time.After(5 * time.Second):
			log.Printf("Finished job %s", j.ID)
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}))

	pool := worker.NewWorkerPool(
		cfg.Workers,
		redisBroker,
		mux,
		cfg.QueueName,
	)

	log.Printf(
		"FlowQ worker started: Redis=%s DB=%d Workers=%d Queue=%s Lease=%s Recovery=%s",
		cfg.RedisAddr,
		cfg.RedisDB,
		cfg.Workers,
		cfg.QueueName,
		cfg.Lease,
		cfg.RecoveryInterval,
	)

	pool.Start(ctx)
	log.Println("FlowQ worker stopped")
}
