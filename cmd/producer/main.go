package main

import (
	"context"
	"log"

	"os"
	"strconv"

	"github.com/Prateet-Github/flowq/internal/broker"
	"github.com/Prateet-Github/flowq/internal/job"
	"github.com/Prateet-Github/flowq/internal/queue"
	"github.com/redis/go-redis/v9"
)

func main() {
	ctx := context.Background()

	redisDB := 0
	if value := os.Getenv("REDIS_DB"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed < 0 {
			log.Fatalf("invalid REDIS_DB: %q", value)
		}
		redisDB = parsed
	}

	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}

	rdb := redis.NewClient(&redis.Options{
		Addr: redisAddr,
		DB:   redisDB,
	})
	defer rdb.Close()

	redisBroker := broker.NewRedisBroker(rdb)
	client := queue.NewClient(redisBroker)

	job := job.NewJob("send_email", []byte(`{"to":"test@example.com"}`))

	if err := client.Enqueue(ctx, job); err != nil {
		log.Fatal(err)
	}

	log.Printf("Enqueued job: %s", job.ID)
}
