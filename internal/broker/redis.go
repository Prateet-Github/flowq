package broker

import (
	_ "embed"

	"time"

	"github.com/redis/go-redis/v9"
)

//go:embed scripts/dequeue.lua
var dequeueLua string

//go:embed scripts/ack.lua
var ackLua string

//go:embed scripts/recover.lua
var recoverLua string

//go:embed scripts/renew.lua
var renewLua string

//go:embed scripts/release.lua
var releaseLua string

//go:embed scripts/retry.lua
var retryLua string

//go:embed scripts/promote_retries.lua
var promoteRetriesLua string

//go:embed scripts/dlq.lua
var dlqLua string

//go:embed scripts/worker_heartbeat.lua
var workerHeartbeatLua string

//go:embed scripts/enqueue_scheduled.lua
var enqueueScheduledLua string

var (
	dequeueScript          = redis.NewScript(dequeueLua)
	ackScript              = redis.NewScript(ackLua)
	recoverScript          = redis.NewScript(recoverLua)
	renewScript            = redis.NewScript(renewLua)
	releaseScript          = redis.NewScript(releaseLua)
	retryScript            = redis.NewScript(retryLua)
	promoteRetriesScript   = redis.NewScript(promoteRetriesLua)
	dlqScript              = redis.NewScript(dlqLua)
	workerHeartbeatScript  = redis.NewScript(workerHeartbeatLua)
	enqueueScheduledScript = redis.NewScript(enqueueScheduledLua)
)

type RedisBroker struct {
	client *redis.Client
	lease  time.Duration
}

type RedisBrokerOption func(*RedisBroker)

func NewRedisBroker(
	client *redis.Client,
	opts ...RedisBrokerOption,
) *RedisBroker {
	b := &RedisBroker{
		client: client,
		lease:  30 * time.Second,
	}

	for _, opt := range opts {
		opt(b)
	}

	return b
}
