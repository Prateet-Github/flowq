package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/Prateet-Github/flowq/internal/broker"
	"github.com/Prateet-Github/flowq/internal/handler"
	"github.com/Prateet-Github/flowq/internal/job"
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
	redisDB := getIntEnv("REDIS_DB", 0)
	workers := getIntEnv("WORKERS", 10)
	queueName := getEnv("QUEUE", "default")
	lease := getDurationEnv("LEASE", 30*time.Second)
	recoveryInterval := getDurationEnv(
		"RECOVERY_INTERVAL",
		5*time.Second,
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
	mux.Handle("send_email", handler.HandlerFunc(func(
		ctx context.Context,
		job *job.Job,
	) error {
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

	pool := worker.NewWorkerPool(
		workers,
		redisBroker,
		mux,
		queueName,
	)

	log.Printf(
		"FlowQ worker started: Redis=%s DB=%d Workers=%d Queue=%s Lease=%s Recovery=%s",
		redisAddr,
		redisDB,
		workers,
		queueName,
		lease,
		recoveryInterval,
	)

	pool.Start(ctx)
	log.Println("FlowQ worker stopped")
}
