package broker

import (
	"context"
	"fmt"
	"testing"

	"github.com/Prateet-Github/flowq/internal/queue"
	"github.com/redis/go-redis/v9"
)

func BenchmarkRedisEnqueue(b *testing.B) {
	ctx := context.Background()

	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer client.Close()

	if err := client.Ping(ctx).Err(); err != nil {
		b.Fatalf("Redis is unavailable: %v", err)
	}

	redisBroker := NewRedisBroker(client)

	job := queue.NewJob(
		"benchmark",
		[]byte(`{"message":"benchmark payload"}`),
	)
	job.Queue = "benchmark"

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		job.ID = fmt.Sprintf("benchmark-%d", i)

		if err := redisBroker.Enqueue(ctx, job); err != nil {
			b.Fatalf("enqueue failed: %v", err)
		}
	}

	b.StopTimer()

	if err := client.Del(ctx, "{q}:pending:benchmark").Err(); err != nil {
		b.Fatalf("cleanup failed: %v", err)
	}
}
