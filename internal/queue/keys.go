// All Redis key strings.(centrailsed)
package queue

import "fmt"

// Priority queues (Redis Lists)--> Job IDs pushed with RPUSH, atomically moved out with LMOVE (RPOPLPUSH).
const (
	QueueP1 = "jobs:queue:p1" // highest priority ,dequeue loop---> checks this first.
	QueueP2 = "jobs:queue:p2"
	QueueP3 = "jobs:queue:p3" // lowest priority
)

var PriorityQueues = []string{QueueP1, QueueP2, QueueP3} // used by the dequeue loop. to loop by priority order (p1 -> p2 -> p3).  Strict priority scheduling. See dequeue.go for starvation notes.

// Redis stores currently running jobs in a separate list for each worker.
const keyInProgressFmt = "jobs:inprogress:%s" //"jobs:inprogress:{worker_id}"

const KeyScheduled = "jobs:scheduled" //redis sorted set,score= unix timestamp

// Job metadata (Redis Hash(field -> value), one per job)
const keyMetaFmt = "jobs:meta:%s" // "jobs:meta:{job_id}"

const KeyDead = "jobs:dead" // RPUSH to append, LRANGE 0 -1 to inspect all dead jobs.

const KeyRetry = "jobs:retry" // Member = job ID.Same range-query mechanism as KeyScheduled.

// Job history (Redis List, one per job)
// Each entry is a JSON string describing a status transition. RPUSH events in order so LRANGE 0 -1 returns chronological history.
const keyHistoryFmt = "jobs:history:%s" //"jobs:history:{job_id}"

// Metrics counters (Redis Strings).INCR is atomic; O(1). These counters never reset.
const (
	KeyMetricProcessed = "metrics:jobs:processed"
	KeyMetricFailed    = "metrics:jobs:failed"
	KeyMetricDead      = "metrics:jobs:dead"
)

// Redis Strings with TTL
const keyHeartbeatFmt = "workers:heartbeat:%s" // "workers:heartbeat:{worker_id}"
const KeyHeartbeatPattern = "workers:heartbeat:*"

const KeyInProgressPattern = "jobs:inprogress:*" //recovery process to find all in-progress queues.

func MetaKey(jobID string) string          { return fmt.Sprintf(keyMetaFmt, jobID) }
func InProgressKey(workerID string) string { return fmt.Sprintf(keyInProgressFmt, workerID) }
func HeartbeatKey(workerID string) string  { return fmt.Sprintf(keyHeartbeatFmt, workerID) }
func HistoryKey(jobID string) string       { return fmt.Sprintf(keyHistoryFmt, jobID) }

func QueueKey(priority int) string {
	switch priority {
	case 1:
		return QueueP1
	case 2:
		return QueueP2
	case 3:
		return QueueP3
	default:
		return QueueP2 // defensive fallback -- should never happen after validation
	}
}
