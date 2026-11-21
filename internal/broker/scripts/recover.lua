local active_key = KEYS[1]
local dlq_key = KEYS[2]
local job_prefix = ARGV[1]
local now = tonumber(ARGV[2])

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

        job.attempts = (job.attempts or 0) + 1

        -- Preserve the original JSON representation.
        -- Only replace the attempts field.
        local updated_data, attempts_count = string.gsub(
            job_data,
            '"attempts"%s*:%s*%-?%d+',
            '"attempts":' .. string.format("%.0f", job.attempts),
            1
        )

        if attempts_count == 0 then
            return redis.error_reply(
                "recovery failed: job JSON has no attempts field"
            )
        end

        if job.attempts > (job.max_retry or 0) then
            redis.call("RPUSH", dlq_key, updated_data)
            redis.call("DEL", job_key)
        else
            local queue_name = job.queue

            if not queue_name or string.match(queue_name, "^%s*$") then
                queue_name = "default"

                -- Update the existing empty queue field.
                local updated_json, queue_count = string.gsub(
                    updated_data,
                    '"queue"%s*:%s*""',
                    '"queue":"default"',
                    1
                )

                if queue_count == 0 then
                    -- Handle legacy JSON without a queue field.
                    updated_data = string.gsub(
                        updated_data,
                        "(%s*})$",
                        ',"queue":"default"%1',
                        1
                    )
                else
                    updated_data = updated_json
                end
            end

            local pending_key = "{q}:pending:" .. queue_name

            redis.call("HSET", job_key,
                "data", updated_data,
                "state", "pending"
            )

            redis.call("HDEL", job_key, "lease_token")
            redis.call("RPUSH", pending_key, updated_data)
        end

        recovered = recovered + 1
    end

    redis.call("ZREM", active_key, job_id)
end

return recovered
