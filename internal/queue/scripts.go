package queue

import "github.com/redis/go-redis/v9"

var scriptPromoteJob = redis.NewScript(`
local removed = redis.call('ZREM', KEYS[1], ARGV[1])
if removed == 0 then
  return 0
end
redis.call('RPUSH', KEYS[2], ARGV[1])
redis.call('HSET',  KEYS[3], 'status', 'QUEUED')
return 1
`)

var scriptCompleteJob = redis.NewScript(`
redis.call('LREM',   KEYS[1], 1, ARGV[1])
redis.call('HSET',   KEYS[2], 'status', 'COMPLETED', 'completed_at', ARGV[3], 'worker_id', '')
redis.call('RPUSH',  KEYS[3], ARGV[2])
redis.call('EXPIRE', KEYS[2], ARGV[4])
redis.call('EXPIRE', KEYS[3], ARGV[4])
redis.call('INCR',   KEYS[4])
return 1
`)

var scriptScheduleRetry = redis.NewScript(`
redis.call('LREM',  KEYS[1], 1, ARGV[1])
redis.call('HSET',  KEYS[2], 'status', 'FAILED', 'retry_count', ARGV[3], 'last_error', ARGV[4])
redis.call('RPUSH', KEYS[3], ARGV[2])
redis.call('INCR',  KEYS[5])
redis.call('ZADD',  KEYS[4], ARGV[5], ARGV[1])
return 1
`)

var scriptMoveToDead = redis.NewScript(`
redis.call('LREM',   KEYS[1], 1, ARGV[1])
redis.call('HSET',   KEYS[2], 'status', 'DEAD', 'last_error', ARGV[3])
redis.call('RPUSH',  KEYS[3], ARGV[1])
redis.call('RPUSH',  KEYS[4], ARGV[2])
redis.call('INCR',   KEYS[5])
redis.call('EXPIRE', KEYS[2], ARGV[4])
redis.call('EXPIRE', KEYS[4], ARGV[4])
return 1
`)

var scriptRecoverJob = redis.NewScript(`
if redis.call('EXISTS', KEYS[2]) == 1 then
  return 0
end

local removed = redis.call('LREM', KEYS[1], 1, ARGV[1])
if removed == 0 then
  return 0
end

redis.call('RPUSH', KEYS[3], ARGV[1])
redis.call('HSET', KEYS[4], 'status', ARGV[2], 'worker_id', '')
return 1
`)

var scriptMarkInProgress = redis.NewScript(`
local metadata_type = redis.call('TYPE', KEYS[1]).ok
local history_type = redis.call('TYPE', KEYS[2]).ok

if metadata_type ~= 'none' and metadata_type ~= 'hash' then
  return redis.error_reply('job metadata key must be a hash')
end

if history_type ~= 'none' and history_type ~= 'list' then
  return redis.error_reply('job history key must be a list')
end

redis.call('HSET', KEYS[1],
  'status', ARGV[1],
  'worker_id', ARGV[2],
  'started_at', ARGV[3]
)
redis.call('RPUSH', KEYS[2], ARGV[4])
return 1
`)
