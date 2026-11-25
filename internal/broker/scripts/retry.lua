local active_key = KEYS[1]
local scheduled_key = KEYS[2]

local job_id = ARGV[1]
local job_prefix = ARGV[2]
local job_data = ARGV[3]
local lease_token = ARGV[4]
local delay_ms = tonumber(ARGV[5])

local job_key = job_prefix .. job_id

local current_token = redis.call("HGET", job_key, "lease_token")

if not current_token or current_token ~= lease_token then
    return 0
end

local score = redis.call("ZSCORE", active_key, job_id)

if not score then
    return 0
end

local redis_time = redis.call("TIME")
local now = tonumber(redis_time[1]) * 1000
    + math.floor(tonumber(redis_time[2]) / 1000)

local scheduled_at = now + delay_ms

redis.call("HSET", job_key,
    "data", job_data,
    "state", "scheduled"
)

redis.call("HDEL", job_key, "lease_token")

redis.call("ZADD", scheduled_key, scheduled_at, job_id)
redis.call("ZREM", active_key, job_id)

return 1