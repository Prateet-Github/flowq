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

	initialJobPercent = 70
	trickleBatches    = 10

	leaseDuration = 3 * time.Second
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

func randomWorker(t *testing.T, rng *rand.Rand) string {
	t.Helper()

	workers := runningWorkers(t)
	if len(workers) == 0 {
		t.Fatal("no running worker containers found")
	}

	return workers[rng.Intn(len(workers))]
}

func enqueueJobsRange(
	t *testing.T,
	ctx context.Context,
	rdb *redis.Client,
	start, end int,
) {
	t.Helper()

	redisBroker := broker.NewRedisBroker(rdb)

	for i := start; i <= end; i++ {
		j := job.NewJob("chaos_job", []byte(`{}`))
		j.ID = fmt.Sprintf("chaos-%05d", i)
		j.MaxRetry = 10
		j.Queue = "default"

		if err := redisBroker.Enqueue(ctx, j); err != nil {
			t.Fatalf("enqueue job %s: %v", j.ID, err)
		}
	}

	if end >= start {
		log.Printf("Enqueued jobs %d–%d", start, end)
	}
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

func assertQueueEmpty(
	t *testing.T,
	ctx context.Context,
	rdb *redis.Client,
) {
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

func totalKills(kills map[string]int) int {
	total := 0
	for _, count := range kills {
		total += count
	}
	return total
}

// Runs random crashes while progressively enqueueing the remaining jobs.
func runRandomCrashesWithTrickle(
	t *testing.T,
	ctx context.Context,
	rdb *redis.Client,
	rng *rand.Rand,
	duration time.Duration,
	nextJob int,
) map[string]int {
	t.Helper()

	kills := make(map[string]int)
	deadline := time.Now().Add(duration)

	remainingJobs := totalJobs - nextJob + 1
	batchSize := (remainingJobs + trickleBatches - 1) / trickleBatches

	log.Printf("Starting random worker crashes for %s", duration)
	log.Printf(
		"Trickle enqueue: %d remaining jobs in batches of approximately %d",
		remainingJobs,
		batchSize,
	)

	for time.Now().Before(deadline) {
		workerID := randomWorker(t, rng)

		killWorker(t, workerID)
		kills[workerID]++

		restartDelay := time.Duration(rng.Intn(2001)) * time.Millisecond
		log.Printf("Worker %s restart delay: %s", workerID, restartDelay)

		time.Sleep(restartDelay)
		startWorker(t, workerID)

		// Enqueue another batch while chaos is ongoing.
		if nextJob <= totalJobs {
			endJob := min(nextJob+batchSize-1, totalJobs)

			enqueueJobsRange(t, ctx, rdb, nextJob, endJob)
			nextJob = endJob + 1
		}

		crashInterval := time.Duration(rng.Intn(3)+1) * time.Second
		log.Printf("Next worker crash in %s", crashInterval)

		time.Sleep(crashInterval)
	}

	// Ensure every remaining job is enqueued even if the crash window
	// ended before all trickle batches were submitted.
	if nextJob <= totalJobs {
		log.Printf("Enqueueing remaining jobs after crash window")
		enqueueJobsRange(t, ctx, rdb, nextJob, totalJobs)
	}

	log.Printf("Random crash phase finished: %d kills", totalKills(kills))

	return kills
}

func ensureWorkersRunning(
	t *testing.T,
	compose func(...string) []byte,
) {
	t.Helper()

	compose("up", "-d", "--scale", "worker=3", "--wait")

	workers := runningWorkers(t)
	if len(workers) != 3 {
		t.Fatalf("expected 3 running workers, got %d", len(workers))
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

	runDocker(t, "wait", workerID)

	log.Printf("Worker %s exited; restarting it", workerID)
	runDocker(t, "start", workerID)
}

// Pauses one worker beyond its configured lease duration.
func pauseAndResumeWorker(
	t *testing.T,
	rng *rand.Rand,
) {
	t.Helper()

	workerID := randomWorker(t, rng)

	log.Printf("Pausing worker %s", workerID)
	runDocker(t, "pause", workerID)

	pauseDuration := leaseDuration + 2*time.Second
	log.Printf("Keeping worker paused for %s", pauseDuration)
	time.Sleep(pauseDuration)

	log.Printf("Unpausing worker %s", workerID)
	runDocker(t, "unpause", workerID)
}

// Stops all workers simultaneously, waits, and restarts the same containers.
func killAllWorkersAndRestart(t *testing.T) {
	t.Helper()

	workers := runningWorkers(t)
	if len(workers) != 3 {
		t.Fatalf("expected 3 running workers before full shutdown, got %d", len(workers))
	}

	log.Printf("Killing all %d workers", len(workers))

	for _, workerID := range workers {
		killWorker(t, workerID)
	}

	log.Printf("All workers stopped; waiting 3 seconds")
	time.Sleep(3 * time.Second)

	for _, workerID := range workers {
		startWorker(t, workerID)
	}

	log.Printf("All original worker containers restarted")

	// Verify the original three workers are running again.
	workers = runningWorkers(t)
	if len(workers) != 3 {
		t.Fatalf("expected 3 running workers after restart, got %d", len(workers))
	}
}

// Waits for completion, allows leases to settle, and verifies Redis state.
func finalizeChaosTest(
	t *testing.T,
	ctx context.Context,
	rdb *redis.Client,
) {
	t.Helper()

	waitForCompletion(t, ctx, rdb)

	log.Printf("Verified %d completed job IDs", totalJobs)

	log.Println("Waiting for leases and acknowledgements to settle")
	time.Sleep(2 * leaseDuration)

	results, err := rdb.HGetAll(ctx, doneKey).Result()
	if err != nil {
		t.Fatalf("read execution results: %v", err)
	}

	if len(results) != totalJobs {
		t.Errorf("expected %d unique completed jobs, got %d", totalJobs, len(results))
	}

	for i := 1; i <= totalJobs; i++ {
		id := fmt.Sprintf("chaos-%05d", i)
		if _, exists := results[id]; !exists {
			t.Errorf("job missing from chaos:done: %s", id)
		}
	}

	assertQueueEmpty(t, ctx, rdb)
	reportDuplicates(t, results)
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

	// Enqueue the initial 70% of jobs.
	initialJobs := totalJobs * initialJobPercent / 100
	enqueueJobsRange(t, ctx, rdb, 1, initialJobs)

	// Random crashes with the remaining 30% enqueued in batches.
	kills := runRandomCrashesWithTrickle(
		t,
		ctx,
		rdb,
		rng,
		40*time.Second,
		initialJobs+1,
	)

	// Graceful shutdown test.
	terminateWorkerGracefully(t, rng)
	ensureWorkersRunning(t, compose)

	// Pause/unpause test.
	pauseAndResumeWorker(t, rng)

	// Full worker outage and recovery test.
	killAllWorkersAndRestart(t)

	// Final completion and Redis cleanup assertions.
	finalizeChaosTest(t, ctx, rdb)

	log.Println("Worker kill report:")
	for workerID, count := range kills {
		log.Printf("Worker %s: killed %d times", workerID, count)
	}
	log.Printf("Total random worker kills: %d", totalKills(kills))
}
