local active_key = KEYS[1]
local dlq_key = KEYS[2]

local job_id = ARGV[1]
local job_prefix = ARGV[2]
local lease_token = ARGV[3]

local job_key = job_prefix .. job_id

local current_token = redis.call("HGET", job_key, "lease_token")

if not current_token or current_token ~= lease_token then
    return 0
end

local score = redis.call("ZSCORE", active_key, job_id)

if not score then
    return 0
end

local job_data = redis.call("HGET", job_key, "data")

if not job_data then
    return 0
end

redis.call("RPUSH", dlq_key, job_data)
redis.call("ZREM", active_key, job_id)
redis.call("DEL", job_key)

return 1