package main

import (
	"context"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/Prateet-Github/flowq/internal/broker"
	"github.com/Prateet-Github/flowq/internal/handler"
	"github.com/Prateet-Github/flowq/internal/job"
	"github.com/Prateet-Github/flowq/internal/retry"
	"github.com/Prateet-Github/flowq/internal/worker"
	"github.com/redis/go-redis/v9"
)

func getEnv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func getIntEnv(key string, fallback int) int {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}

	n, err := strconv.Atoi(value)
	if err != nil || n < 0 {
		log.Fatalf("invalid %s: %q", key, value)
	}

	return n
}

func getDurationEnv(key string, fallback time.Duration) time.Duration {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}

	d, err := time.ParseDuration(value)
	if err != nil || d <= 0 {
		log.Fatalf("invalid %s: %q", key, value)
	}

	return d
}

func main() {
	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	redisAddr := getEnv("REDIS_ADDR", "localhost:6379")
	redisDB := getIntEnv("REDIS_DB", 14)
	workers := getIntEnv("WORKERS", 8)
	queueName := getEnv("QUEUE", "default")
	lease := getDurationEnv("LEASE", 3*time.Second)
	recoveryInterval := getDurationEnv(
		"RECOVERY_INTERVAL",
		time.Second,
	)

	if workers <= 0 {
		log.Fatal("WORKERS must be greater than zero")
	}

	rdb := redis.NewClient(&redis.Options{
		Addr: redisAddr,
		DB:   redisDB,
	})
	defer rdb.Close()

	redisBroker := broker.NewRedisBroker(
		rdb,
		broker.WithLease(lease),
	)

	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("Redis connection failed: %v", err)
	}

	go worker.StartRecovery(ctx, redisBroker, recoveryInterval)

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
		workers,
		redisBroker,
		mux,
		queueName,
	)

	log.Printf(
		"FlowQ chaos worker started: Redis=%s DB=%d Workers=%d Queue=%s Lease=%s Recovery=%s",
		redisAddr,
		redisDB,
		workers,
		queueName,
		lease,
		recoveryInterval,
	)

	pool.Start(ctx)
	log.Println("FlowQ chaos worker stopped")
}
