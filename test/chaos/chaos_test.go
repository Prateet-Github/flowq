package chaos

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strconv"
	"testing"
	"time"

	"github.com/Prateet-Github/flowq/internal/broker"
	"github.com/Prateet-Github/flowq/internal/job"
	"github.com/redis/go-redis/v9"
)

const composeFile = "../../docker-compose.chaos.yml"

const (
	totalJobs = 10_000
	doneKey   = "chaos:done"
)

func runDocker(t *testing.T, args ...string) []byte {
	t.Helper()

	cmd := exec.Command("docker", args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf(
			"docker %v failed: %v\n%s",
			args,
			err,
			output,
		)
	}

	return output
}

func enqueueJobs(
	t *testing.T,
	ctx context.Context,
	rdb *redis.Client,
) {
	t.Helper()

	redisBroker := broker.NewRedisBroker(rdb)

	for i := 1; i <= totalJobs; i++ {
		j := job.NewJob("chaos_job", []byte(`{}`))
		j.ID = fmt.Sprintf("chaos-%05d", i)
		j.MaxRetry = 10
		j.Queue = "default"

		if err := redisBroker.Enqueue(ctx, j); err != nil {
			t.Fatalf("enqueue job %s: %v", j.ID, err)
		}
	}

	log.Printf("Enqueued %d chaos jobs", totalJobs)
}

func waitForCompletion(
	t *testing.T,
	ctx context.Context,
	rdb *redis.Client,
) {
	t.Helper()

	deadline := time.NewTimer(90 * time.Second)
	defer deadline.Stop()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		count, err := rdb.HLen(ctx, doneKey).Result()
		if err != nil {
			t.Fatalf("read completion count: %v", err)
		}

		log.Printf("Completed jobs: %d/%d", count, totalJobs)

		if count == totalJobs {
			return
		}

		select {
		case <-deadline.C:
			t.Fatalf(
				"completion timeout: %d/%d jobs",
				count,
				totalJobs,
			)
		case <-ctx.Done():
			t.Fatal("context cancelled while waiting for completion")
		case <-ticker.C:
		}
	}
}

func TestChaos(t *testing.T) {
	// Generate a reproducible seed, or reuse one supplied for replay.
	seed := time.Now().UnixNano()

	if value := os.Getenv("CHAOS_SEED"); value != "" {
		parsed, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			t.Fatalf("invalid CHAOS_SEED %q: %v", value, err)
		}
		seed = parsed
	}

	log.Printf("Chaos test seed: %d", seed)

	compose := func(args ...string) []byte {
		t.Helper()

		command := append(
			[]string{"compose", "-f", composeFile},
			args...,
		)

		return runDocker(t, command...)
	}

	// Register cleanup before starting containers.
	t.Cleanup(func() {
		if t.Failed() {
			log.Println("Chaos test failed; collecting container logs")

			cmd := exec.Command(
				"docker",
				"compose",
				"-f",
				composeFile,
				"logs",
				"--no-color",
			)

			output, err := cmd.CombinedOutput()
			if err != nil {
				log.Printf("failed to collect logs: %v", err)
			}

			log.Printf("Container logs:\n%s", output)
		}

		log.Println("Stopping chaos environment")

		cmd := exec.Command(
			"docker",
			"compose",
			"-f",
			composeFile,
			"down",
			"-v",
		)

		output, err := cmd.CombinedOutput()
		if err != nil {
			log.Printf("compose down failed: %v\n%s", err, output)
		}
	})

	log.Println("Starting chaos environment")

	compose(
		"up",
		"-d",
		"--build",
		"--scale", "worker=3",
		"--wait",
	)

	// Connect to the isolated chaos Redis database.
	ctx, cancel := context.WithTimeout(
		context.Background(),
		3*time.Minute,
	)
	defer cancel()

	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   14,
	})
	defer rdb.Close()

	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Fatalf("Redis connection failed: %v", err)
	}

	if err := rdb.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("failed to flush Redis DB 14: %v", err)
	}

	log.Println("Redis DB 14 flushed")

	// Enqueue 10,000 deterministic jobs.
	enqueueJobs(t, ctx, rdb)

	// Wait until all jobs complete.
	waitForCompletion(t, ctx, rdb)

	// Verify every expected job ID exists.
	results, err := rdb.HGetAll(ctx, doneKey).Result()
	if err != nil {
		t.Fatal(err)
	}

	for i := 1; i <= totalJobs; i++ {
		id := fmt.Sprintf("chaos-%05d", i)

		if _, exists := results[id]; !exists {
			t.Errorf("job missing from chaos:done: %s", id)
		}
	}

	log.Printf("Verified all %d job IDs", totalJobs)
}
