package chaos

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"strconv"
	"strings"
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
		t.Fatalf("docker %v failed: %v\n%s", args, err, output)
	}

	return output
}

// Returns IDs of currently running worker containers.
func runningWorkers(t *testing.T) []string {
	t.Helper()

	output := runDocker(
		t,
		"compose", "-f", composeFile,
		"ps", "-q", "--status", "running", "worker",
	)

	return strings.Fields(string(output))
}

func killWorker(t *testing.T, workerID string) {
	t.Helper()

	log.Printf("SIGKILL worker %s", workerID)
	runDocker(t, "kill", "--signal=KILL", workerID)
}

func startWorker(t *testing.T, workerID string) {
	t.Helper()

	log.Printf("Restarting worker %s", workerID)
	runDocker(t, "start", workerID)
}

// Selects a running worker deterministically using the supplied RNG.
func randomWorker(t *testing.T, rng *rand.Rand) string {
	t.Helper()

	workers := runningWorkers(t)
	if len(workers) == 0 {
		t.Fatal("no running worker containers found")
	}

	return workers[rng.Intn(len(workers))]
}

func enqueueJobs(t *testing.T, ctx context.Context, rdb *redis.Client) {
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

func waitForCompletion(t *testing.T, ctx context.Context, rdb *redis.Client) {
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
			t.Fatalf("completion timeout: %d/%d jobs", count, totalJobs)
		case <-ctx.Done():
			t.Fatal("context cancelled while waiting for completion")
		case <-ticker.C:
		}
	}
}

func assertEmpty(t *testing.T, key, dataType string, count int64) {
	t.Helper()

	if count != 0 {
		t.Errorf("expected %s %q to be empty, got %d", dataType, key, count)
	}
}

func scanKeys(
	ctx context.Context,
	rdb *redis.Client,
	pattern string,
) ([]string, error) {
	var keys []string
	var cursor uint64

	for {
		batch, nextCursor, err := rdb.Scan(ctx, cursor, pattern, 100).Result()
		if err != nil {
			return nil, err
		}

		keys = append(keys, batch...)
		cursor = nextCursor

		if cursor == 0 {
			return keys, nil
		}
	}
}

func assertQueueEmpty(t *testing.T, ctx context.Context, rdb *redis.Client) {
	t.Helper()

	pending, err := rdb.LLen(ctx, "{q}:pending:default").Result()
	if err != nil {
		t.Fatalf("check pending queue: %v", err)
	}
	assertEmpty(t, "{q}:pending:default", "list", pending)

	active, err := rdb.ZCard(ctx, "{q}:active").Result()
	if err != nil {
		t.Fatalf("check active queue: %v", err)
	}
	assertEmpty(t, "{q}:active", "sorted set", active)

	delayed, err := rdb.ZCard(ctx, "{q}:delayed").Result()
	if err != nil {
		t.Fatalf("check delayed queue: %v", err)
	}
	assertEmpty(t, "{q}:delayed", "sorted set", delayed)

	dlq, err := rdb.LLen(ctx, "{q}:dlq").Result()
	if err != nil {
		t.Fatalf("check dead-letter queue: %v", err)
	}
	assertEmpty(t, "{q}:dlq", "list", dlq)

	jobKeys, err := scanKeys(ctx, rdb, "{q}:job:*")
	if err != nil {
		t.Fatalf("scan job records: %v", err)
	}
	if len(jobKeys) != 0 {
		t.Errorf(
			"expected no leftover job records, found %d: %v",
			len(jobKeys),
			jobKeys[:min(len(jobKeys), 10)],
		)
	}

	retryKeys, err := scanKeys(ctx, rdb, "{q}:retry:*")
	if err != nil {
		t.Fatalf("scan retry records: %v", err)
	}
	if len(retryKeys) != 0 {
		t.Errorf(
			"expected no leftover retry records, found %d: %v",
			len(retryKeys),
			retryKeys[:min(len(retryKeys), 10)],
		)
	}
}

func reportDuplicates(t *testing.T, results map[string]string) {
	t.Helper()

	var totalExecutions int64
	var duplicateExecutions int64

	for id, value := range results {
		count, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			t.Errorf("invalid execution count for %s: %q", id, value)
			continue
		}

		totalExecutions += count
		if count > 1 {
			duplicateExecutions += count - 1
		}
	}

	log.Printf(
		"Execution report: %d successful executions, %d duplicate executions",
		totalExecutions,
		duplicateExecutions,
	)
}

func runRandomCrashes(
	t *testing.T,
	rng *rand.Rand,
	duration time.Duration,
) map[string]int {
	t.Helper()

	kills := make(map[string]int)
	deadline := time.Now().Add(duration)

	log.Printf("Starting random worker crashes for %s", duration)

	for time.Now().Before(deadline) {
		workerID := randomWorker(t, rng)

		killWorker(t, workerID)
		kills[workerID]++

		// Restart the same worker after a deterministic random delay.
		restartDelay := time.Duration(rng.Intn(2001)) * time.Millisecond
		log.Printf("Worker %s restart delay: %s", workerID, restartDelay)

		time.Sleep(restartDelay)
		startWorker(t, workerID)

		// Wait 1–3 seconds before the next crash.
		crashInterval := time.Duration(rng.Intn(3)+1) * time.Second
		log.Printf("Next worker crash in %s", crashInterval)

		time.Sleep(crashInterval)
	}

	log.Printf("Random crash phase finished: %d kills", totalKills(kills))

	return kills
}

func totalKills(kills map[string]int) int {
	total := 0
	for _, count := range kills {
		total += count
	}
	return total
}

func ensureWorkersRunning(t *testing.T, compose func(...string) []byte) {
	t.Helper()

	compose("up", "-d", "--scale", "worker=3", "--wait")

	workers := runningWorkers(t)
	if len(workers) != 3 {
		t.Fatalf("expected 3 running workers after recovery, got %d", len(workers))
	}

	log.Printf("All %d workers are running", len(workers))
}

func terminateWorkerGracefully(t *testing.T, rng *rand.Rand) {
	t.Helper()

	workers := runningWorkers(t)
	if len(workers) == 0 {
		t.Fatal("no running workers available for SIGTERM test")
	}

	workerID := workers[rng.Intn(len(workers))]

	log.Printf("Sending SIGTERM to worker %s", workerID)

	runDocker(t, "kill", "--signal=TERM", workerID)

	// Wait until the worker has exited.
	runDocker(t, "wait", workerID)

	log.Printf("Worker %s exited; restarting it", workerID)

	runDocker(t, "start", workerID)

	log.Printf("Worker %s restarted after graceful shutdown", workerID)
}

func TestChaos(t *testing.T) {
	seed := time.Now().UnixNano()

	if value := os.Getenv("CHAOS_SEED"); value != "" {
		parsed, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			t.Fatalf("invalid CHAOS_SEED %q: %v", value, err)
		}
		seed = parsed
	}

	log.Printf("Chaos test seed: %d", seed)

	rng := rand.New(rand.NewSource(seed))

	compose := func(args ...string) []byte {
		t.Helper()

		command := append([]string{"compose", "-f", composeFile}, args...)
		return runDocker(t, command...)
	}

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

	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Minute,
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

	// Enqueue jobs before introducing worker failures.
	enqueueJobs(t, ctx, rdb)

	// Randomly kill and restart workers while jobs are processing.
	kills := runRandomCrashes(t, rng, 40*time.Second)

	// Test graceful worker shutdown.
	terminateWorkerGracefully(t, rng)

	// Ensure all three workers are running again.
	ensureWorkersRunning(t, compose)

	// Wait for outstanding jobs and recovered leases.
	waitForCompletion(t, ctx, rdb)

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

	log.Println("Waiting for leases and acknowledgements to settle")
	time.Sleep(6 * time.Second)

	assertQueueEmpty(t, ctx, rdb)
	reportDuplicates(t, results)

	log.Println("Worker kill report:")
	for workerID, count := range kills {
		log.Printf("Worker %s: killed %d times", workerID, count)
	}
	log.Printf("Total worker kills: %d", totalKills(kills))
}
