local delayed_key = KEYS[1]

local job_id = ARGV[1]
local job_data = ARGV[2]
local retry_at = tonumber(ARGV[3])

redis.call("ZADD", delayed_key, retry_at, job_id)
redis.call("SET", "{q}:retry:" .. job_id, job_data)

return 1