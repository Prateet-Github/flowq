local pending_key = KEYS[1]
local active_key = KEYS[2]
local job_prefix = ARGV[1]
local lease_until = tonumber(ARGV[2])
local lease_token = ARGV[3]

local job_data = redis.call("LPOP", pending_key)

if not job_data then
    return nil
end

local job = cjson.decode(job_data)
local job_key = job_prefix .. job.id

redis.call("HSET", job_key,
    "data", job_data,
    "state", "active",
    "lease_token", lease_token
)

redis.call("ZADD", active_key, lease_until, job.id)

return job_data