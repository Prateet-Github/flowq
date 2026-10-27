local active_key = KEYS[1]
local job_prefix = ARGV[1]
local now = tonumber(ARGV[2])

-- find jobs whose leases have expired
local expired_jobs = redis.call(
    "ZRANGEBYSCORE",
    active_key,
    "-inf",
    now,
    "LIMIT",
    0,
    100
)

local recovered = 0

for _, job_id in ipairs(expired_jobs) do
    local job_key = job_prefix .. job_id
    local job_data = redis.call("HGET", job_key, "data")

    if job_data then
        local job = cjson.decode(job_data)
        local pending_key = "{q}:pending:" .. job.queue

        redis.call("HSET", job_key, "state", "pending")
        redis.call("RPUSH", pending_key, job_data)
    end

    redis.call("ZREM", active_key, job_id)
    recovered = recovered + 1
end

return recovered