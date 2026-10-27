# FlowQ

A Redis-backed job queue built from scratch in Go.

### Features
- Concurrent worker pool
- Atomic job claiming with Lua scripts
- Lease-based job recovery
- Lease renewal and acknowledgement
- Graceful shutdown
- Redis-backed persistence

### Stack
Go · Redis · Lua · Docker

### Benchmark

| Metric | Result |
|---|---:|
| Enqueue throughput | ~8,135 jobs/sec |
| Latency | ~122.9 µs/op |
| Allocations | 11 allocs/op |

*Apple M2 · Single-job Redis RPUSH benchmark*

### Run

```bash
docker compose up -d redis
go run ./cmd/worker
go run ./cmd/producer