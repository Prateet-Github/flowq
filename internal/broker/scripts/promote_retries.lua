local delayed_key = KEYS[1]

local now = tonumber(ARGV[1])
local limit = tonumber(ARGV[2])

local due_jobs = redis.call(
    "ZRANGEBYSCORE",
    delayed_key,
    "-inf",
    now,
    "LIMIT",
    0,
    limit
)

local promoted = 0

for _, job_id in ipairs(due_jobs) do
    local retry_key = "{q}:retry:" .. job_id
    local job_data = redis.call("GET", retry_key)

    if job_data then
        local job = cjson.decode(job_data)
        local queue_name = job.queue or "default"
        local pending_key = "{q}:pending:" .. queue_name

        redis.call("RPUSH", pending_key, job_data)
        redis.call("DEL", retry_key)
        redis.call("ZREM", delayed_key, job_id)

        promoted = promoted + 1
    else
        redis.call("ZREM", delayed_key, job_id)
    end
end

return promoted