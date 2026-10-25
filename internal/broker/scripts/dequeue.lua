local pending_key = KEYS[1]
local active_key = KEYS[2]
local job_prefix = ARGV[1]
local lease_until = tonumber(ARGV[2])

-- get the next pending job
local job_data = redis.call("LPOP", pending_key)

if not job_data then
    return nil
end

local job = cjson.decode(job_data)
local job_key = job_prefix .. job.id

-- persist the job so it can be recovered later
redis.call("HSET", job_key,
    "data", job_data,
    "state", "active"
)

-- register the job as active until its lease expires
redis.call("ZADD", active_key, lease_until, job.id)

return job_data