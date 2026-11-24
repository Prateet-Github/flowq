# FlowQ

A Redis-backed job queue written in Go, built from scratch with Lua-powered atomic operations and lease-based job processing.

## Features

- Concurrent worker pool
- Atomic job claiming using Redis Lua scripts
- At-least-once job delivery
- Lease-based job recovery after worker crashes
- Automatic lease renewal
- Lease ownership validation using unique tokens
- Graceful worker shutdown
- Configurable retry handling with exponential backoff and full jitter
- Delayed retry scheduling
- Dead Letter Queue (DLQ)
- Panic recovery and job execution timeouts
- Redis-backed job persistence
- Chaos testing for worker failures and recovery

## Architecture

```text
Producer
   |
   v
Queue Client
   |
   v
Redis Broker
   |
   +---- Pending Jobs
   |
   +---- Active Jobs (Leases)
   |
   +---- Scheduled Jobs
   |
   +---- Dead Letter Queue
             |
             v
       Worker Pool
             |
             v
       Job Handlers
```

## Tech Stack

- **Language:** Go
- **Storage / Broker:** Redis
- **Atomic Operations:** Lua
- **Containerization:** Docker & Docker Compose

## Benchmark

*Apple M2 · Single-job Redis RPUSH benchmark*

| Metric | Result |
|---|---:|
| Enqueue throughput | ~8,159 jobs/sec |
| Latency | ~120 µs/op |
| Allocations | 11 allocs/op |

*These are enqueue-only benchmark results, not end-to-end processing throughput.*


## Delivery Guarantee

FlowQ provides **at-least-once delivery**.

Jobs can be executed more than once if a worker crashes or loses its lease before acknowledging completion. Lease-based recovery makes unfinished jobs available for processing again.
