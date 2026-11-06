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

	"github.com/redis/go-redis/v9"
)

const composeFile = "../../docker-compose.chaos.yml"

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
		15*time.Second,
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

	log.Println("Chaos environment ready; Redis DB 14 flushed")

	_ = fmt.Sprintf("seed=%d", seed) // Seed will drive orchestrator randomness in the next stage.
}
