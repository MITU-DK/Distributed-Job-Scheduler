// Package queue owns all Redis key strings.
// NOTHING ELSE in this codebase hardcodes a Redis key string.
// Every key is defined here as a constant or helper function.
//
// Why centralise keys?
//   - A typo in a key string is a silent bug: data goes to the wrong place.
//   - If you ever version your keys (e.g. "v2:jobs:queue:p1"), you change one line.
//   - `redis-cli KEYS jobs:queue:*` returns exactly the queues you expect.
package queue

import "fmt"

// Priority queues (Redis Lists)
// Job IDs are pushed in with RPUSH, atomically moved out with LMOVE (RPOPLPUSH).
// p1 = highest priority — dequeue loop---> checks this first.
const (
	QueueP1 = "jobs:queue:p1" // highest priority
	QueueP2 = "jobs:queue:p2"
	QueueP3 = "jobs:queue:p3" // lowest priority
)

// PriorityQueues is the ordered slice used by the dequeue loop.
// Always check index 0 (p1, highest) before index 1, then index 2.
// This implements strict priority scheduling. See dequeue.go for starvation notes.
var PriorityQueues = []string{QueueP1, QueueP2, QueueP3}

// In-progress tracking (Redis List(queue can insert or remove items from either end), one per worker)
// LMOVE atomically moves a job ID from a priority queue to this list.
// If the worker crashes, the job ID is still here — recovery re-enqueues it.
const keyInProgressFmt = "jobs:inprogress:%s" //"jobs:inprogress:{worker_id}"

// Scheduled jobs (Redis Sorted Set)
// Score = Unix timestamp when the job should run.  Member = job ID.
// The scheduler uses ZRANGEBYSCORE 0 {now} to find ready jobs.
const KeyScheduled = "jobs:scheduled"

// Job metadata (Redis Hash(field -> value), one per job)
// Stores every field of the Job struct as individual hash fields.
// Why Hash and not a JSON string?
//   - Atomic field updates: HSET jobs:meta:{id} status IN_PROGRESS worker_id w1
//   - No read-modify-write cycle needed (avoids race conditions)
//   - A full JSON string requires---> GET → unmarshal → modify → marshal → SET
const keyMetaFmt = "jobs:meta:%s" // "jobs:meta:{job_id}"

// Dead letter queue (Redis List)
// Jobs that have exhausted all retries land here.
// RPUSH to append, LRANGE 0 -1 to inspect all dead jobs.
const KeyDead = "jobs:dead"

// Retry queue (Redis Sorted Set(stores unique elements))
// Score = Unix timestamp when the retry should be attempted.   Member = job ID.
// Same range-query mechanism as KeyScheduled.
const KeyRetry = "jobs:retry"

// Job history (Redis List, one per job)
// Each entry is a JSON string describing a status transition.
// RPUSH events in order so LRANGE 0 -1 returns chronological history.
const keyHistoryFmt = "jobs:history:%s" //"jobs:history:{job_id}"

// ── Metrics counters (Redis Strings)
// INCR is atomic; O(1). These counters never reset.
const (
	KeyMetricProcessed = "metrics:jobs:processed"
	KeyMetricFailed    = "metrics:jobs:failed"
	KeyMetricDead      = "metrics:jobs:dead"
)

// Worker heartbeats (Redis Strings with TTL)
// Each worker refreshes its key every HeartbeatInterval seconds.
// TTL = HeartbeatTTL seconds. If the key expires, the worker is considered dead.
const keyHeartbeatFmt = "workers:heartbeat:%s" // "workers:heartbeat:{worker_id}"

// KeyHeartbeatPattern is used with SCAN to find all worker heartbeat keys. (active workers.)
const KeyHeartbeatPattern = "workers:heartbeat:*"

// KeyInProgressPattern is used by the recovery process to find all in-progress queues.
const KeyInProgressPattern = "jobs:inprogress:*"

// Helper functions
// Use these instead of fmt.Sprintf directly.
// Rationale: a typo in a format string is a hard-to-find bug.
// Centralising the Sprintf call makes the key derivable from one place.

func MetaKey(jobID string) string          { return fmt.Sprintf(keyMetaFmt, jobID) }
func InProgressKey(workerID string) string { return fmt.Sprintf(keyInProgressFmt, workerID) }
func HeartbeatKey(workerID string) string  { return fmt.Sprintf(keyHeartbeatFmt, workerID) }
func HistoryKey(jobID string) string       { return fmt.Sprintf(keyHistoryFmt, jobID) }

// QueueKey returns the Redis list key for a given priority (1, 2, or 3).
// Exported so the API retry handler and other packages can re-enqueue jobs
// without importing the private queueKey function from enqueue.go.
func QueueKey(priority int) string {
	switch priority {
	case 1:
		return QueueP1
	case 2:
		return QueueP2
	case 3:
		return QueueP3
	default:
		return QueueP2 // defensive fallback — should never happen after validation
	}
}

