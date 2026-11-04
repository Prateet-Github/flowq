local active_key = KEYS[1]
local delayed_key = KEYS[2]

local job_id = ARGV[1]
local job_prefix = ARGV[2]
local job_data = ARGV[3]
local lease_token = ARGV[4]
local retry_at = tonumber(ARGV[5])

local job_key = job_prefix .. job_id

local current_token = redis.call("HGET", job_key, "lease_token")

if not current_token or current_token ~= lease_token then
    return 0
end

local score = redis.call("ZSCORE", active_key, job_id)

if not score then
    return 0
end

redis.call("SET", "{q}:retry:" .. job_id, job_data)
redis.call("ZADD", delayed_key, retry_at, job_id)
redis.call("ZREM", active_key, job_id)
redis.call("DEL", job_key)

return 1