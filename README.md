# FlowQ

A Redis backed job queue written in Go.

## Features

- Concurrent worker pool
- Atomic job claiming with Lua scripts
- Lease based job recovery
- Lease renewal and acknowledgement
- Graceful shutdown
- Redis backed persistence

## Stack

- Go
- Redis
- Lua
- Docker

## Benchmark

*Apple M2 · Single-job Redis RPUSH benchmark*

| Metric | Result |
|---|---:|
| Enqueue throughput | ~8,313 jobs/sec |
| Latency | ~120.25 µs/op |
| Allocations | 11 allocs/op |

## Run

```bash
docker compose up -d redis
go run ./cmd/worker
go run ./cmd/producer
```
