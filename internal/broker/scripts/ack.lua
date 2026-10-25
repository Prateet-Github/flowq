local active_key = KEYS[1]
local job_key = ARGV[1] .. ARGV[2]

redis.call("ZREM", active_key, ARGV[2])
redis.call("DEL", job_key)

return 1