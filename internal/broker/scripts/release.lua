local job_key = KEYS[1]
local active_key = KEYS[2]

local job_id = ARGV[1]
local lease_token = ARGV[2]

local current_token = redis.call("HGET", job_key, "lease_token")

if not current_token or current_token ~= lease_token then
    return 0
end

if not redis.call("ZSCORE", active_key, job_id) then
    return 0
end

local job_data = redis.call("HGET", job_key, "data")

if not job_data then
    return 0
end

local job = cjson.decode(job_data)
local queue_name = job.queue or "default"
local pending_key = "{q}:pending:" .. queue_name

redis.call("RPUSH", pending_key, job_data)
redis.call("ZREM", active_key, job_id)
redis.call("DEL", job_key)

return 1