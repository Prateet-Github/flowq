local job_id = ARGV[1]
local lease_until = ARGV[2]

-- renew only if the job is still active
if redis.call("ZSCORE", KEYS[1], job_id) then
    redis.call("ZADD", KEYS[1], lease_until, job_id)
    return 1
end

return 0