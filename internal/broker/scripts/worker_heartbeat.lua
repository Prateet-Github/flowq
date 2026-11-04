local worker_key = KEYS[1]

local timestamp = ARGV[1]
local in_flight = ARGV[2]
local ttl = tonumber(ARGV[3])

redis.call(
    "HSET",
    worker_key,
    "last_seen", timestamp,
    "in_flight", in_flight
)

redis.call("EXPIRE", worker_key, ttl)

return 1