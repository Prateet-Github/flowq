local active_key = KEYS[1]
local job_prefix = ARGV[1]
local job_id = ARGV[2]
local lease_token = ARGV[3]
local lease_duration_ms = tonumber(ARGV[4])

local job_key = job_prefix .. job_id

local current_token = redis.call("HGET", job_key, "lease_token")

if not current_token or current_token ~= lease_token then
    return 0
end

if not redis.call("ZSCORE", active_key, job_id) then
    return 0
end

local redis_time = redis.call("TIME")
local now = tonumber(redis_time[1]) * 1000
    + math.floor(tonumber(redis_time[2]) / 1000)

local lease_until = now + lease_duration_ms

redis.call("ZADD", active_key, lease_until, job_id)

return 1