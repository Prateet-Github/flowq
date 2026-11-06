package main

import (
	"context"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Prateet-Github/flowq/internal/broker"
	"github.com/Prateet-Github/flowq/internal/config"
	"github.com/Prateet-Github/flowq/internal/handler"
	"github.com/Prateet-Github/flowq/internal/job"
	"github.com/Prateet-Github/flowq/internal/retry"
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

	cfg := config.LoadWithDefaults(config.Config{
		RedisAddr:        "localhost:6379",
		RedisDB:          14,
		Workers:          8,
		QueueName:        "default",
		Lease:            3 * time.Second,
		RecoveryInterval: time.Second,
	})

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

	mux.Handle("chaos_job", handler.HandlerFunc(func(
		ctx context.Context,
		j *job.Job,
	) error {
		// Random processing duration: 50–150ms.
		delay := time.Duration(50+rand.Intn(101)) * time.Millisecond

		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return ctx.Err()
		}

		// Fail approximately 5% of first attempts.
		if j.Attempts == 0 && rand.Intn(100) < 5 {
			return retry.ErrRetryable
		}

		// Record successful processing in Redis.
		if err := rdb.HIncrBy(
			ctx,
			"chaos:done",
			j.ID,
			1,
		).Err(); err != nil {
			return err
		}

		return nil
	}))

	pool := worker.NewWorkerPool(
		cfg.Workers,
		redisBroker,
		mux,
		cfg.QueueName,
	)

	log.Printf(
		"FlowQ chaos worker started: Redis=%s DB=%d Workers=%d Queue=%s Lease=%s Recovery=%s",
		cfg.RedisAddr,
		cfg.RedisDB,
		cfg.Workers,
		cfg.QueueName,
		cfg.Lease,
		cfg.RecoveryInterval,
	)

	pool.Start(ctx)
	log.Println("FlowQ chaos worker stopped")
}
