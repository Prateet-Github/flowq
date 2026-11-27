local pending_key = KEYS[1]
local scheduled_key = KEYS[2]
local job_key = KEYS[3]

local job_id = ARGV[1]
local job_data = ARGV[2]
local mode = ARGV[3]
local value = tonumber(ARGV[4])

local redis_time = redis.call("TIME")
local now = tonumber(redis_time[1]) * 1000
    + math.floor(tonumber(redis_time[2]) / 1000)

local due_at

if mode == "delay" then
    due_at = now + value
elseif mode == "at" then
    due_at = value
else
    return redis.error_reply("invalid scheduling mode")
end

if due_at <= now then
    redis.call("RPUSH", pending_key, job_data)
    return 0
end

redis.call("HSET", job_key,
    "data", job_data,
    "state", "scheduled"
)

redis.call("ZADD", scheduled_key, due_at, job_id)

return 1