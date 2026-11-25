local scheduled_key = KEYS[1]
local job_prefix = ARGV[1]
local limit = tonumber(ARGV[2])

local redis_time = redis.call("TIME")
local now = tonumber(redis_time[1]) * 1000
    + math.floor(tonumber(redis_time[2]) / 1000)

local due_jobs = redis.call(
    "ZRANGEBYSCORE",
    scheduled_key,
    "-inf",
    now,
    "LIMIT",
    0,
    limit
)

local promoted = 0

for _, job_id in ipairs(due_jobs) do
    local job_key = job_prefix .. job_id
    local job_data = redis.call("HGET", job_key, "data")

    if job_data then
        local job = cjson.decode(job_data)
        local queue_name = job.queue

        if not queue_name or string.match(queue_name, "^%s*$") then
            queue_name = "default"
        end

        local pending_key = "{q}:pending:" .. queue_name

        redis.call("RPUSH", pending_key, job_data)
        redis.call("HSET", job_key, "state", "pending")
        redis.call("ZREM", scheduled_key, job_id)

        promoted = promoted + 1
    else
        redis.call("ZREM", scheduled_key, job_id)
    end
end

return promoted