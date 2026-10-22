package main

import (
	"context"
	"log"

	"github.com/Prateet-Github/flowq/internal/broker"
	"github.com/Prateet-Github/flowq/internal/queue"
	"github.com/redis/go-redis/v9"
)

func main() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer rdb.Close()

	redisBroker := broker.NewRedisBroker(rdb)
	client := queue.NewClient(redisBroker)

	job := queue.NewJob("send_email", []byte(`{"to":"test@example.com"}`))

	if err := client.Enqueue(ctx, job); err != nil {
		log.Fatal(err)
	}

	log.Printf("Enqueued job: %s", job.ID)
}
