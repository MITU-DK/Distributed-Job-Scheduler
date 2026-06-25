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
