// Package queue ---> job completion and failure paths.

//	CompleteJob — called by a worker after successful execution.
//
// Flow:
//  1. Remove the job ID from the worker's in-progress list (LREM, by value).
//  2. Update the Redis hash metadata.
//  3. Append a history event (status transition log).
//  4. Increment the appropriate metric counter.
package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/mitudk/distributed-job-scheduler/internal/job"
	"github.com/redis/go-redis/v9"
)

// completionEvent is serialised into the jobs:history:{id} list on every Terminal transition.
// Re-uses the same shape as HistoryEvent in dequeue.go for a consistent audit trail.
type completionEvent struct {
	From     job.Status `json:"from"`
	To       job.Status `json:"to"`
	At       int64      `json:"at"`
	WorkerID string     `json:"worker_id,omitempty"`
	Error    string     `json:"error,omitempty"` // Only set on failure events.
}

// CompleteJob marks a job as COMPLETED after successful execution.
//
// ATOMICITY: scriptCompleteJob (Lua) performs all 5 steps in one unbreakable Redis operation:
//   1. LREM  jobs:inprogress:{workerID} 1 {jobID}   — remove from in-progress list
//   2. HSET  jobs:meta:{id} status COMPLETED ...     — update metadata
//   3. RPUSH jobs:history:{id} {event_json}          — append history event
//   4. EXPIRE jobs:meta:{id}     604800              — 7-day TTL
//   5. EXPIRE jobs:history:{id}  604800              — 7-day TTL
//
// The metric counter (metrics:jobs:processed INCR) is intentionally kept outside
// the Lua script via a fire-and-forget pipeline. A missed metric increment is
// acceptable; a missed job-completion acknowledgement is not.
func CompleteJob(ctx context.Context, rdb *redis.Client, j *job.Job, workerID string) error {
	now := time.Now().Unix()

	// Build history event.
	event := completionEvent{
		From:     j.Status,
		To:       job.StatusCompleted,
		At:       now,
		WorkerID: workerID,
	}
	eventJSON, _ := json.Marshal(event) // known types — cannot fail

	const ttlSeconds = 7 * 24 * 60 * 60 // 604800

	// Atomically: LREM inprogress + HSET COMPLETED + RPUSH history + EXPIRE x2.
	if err := scriptCompleteJob.Run(ctx, rdb,
		[]string{
			InProgressKey(workerID), // KEYS[1]
			MetaKey(j.ID),           // KEYS[2]
			HistoryKey(j.ID),        // KEYS[3]
		},
		j.ID,                        // ARGV[1]
		string(eventJSON),           // ARGV[2]
		now,                         // ARGV[3] completed_at
		ttlSeconds,                  // ARGV[4] TTL
	).Err(); err != nil {
		slog.Error("complete_script_failed", "job_id", j.ID, "error", err)
		return fmt.Errorf("complete script for job %s: %w", j.ID, err)
	}

	// Increment the processed counter separately.
	// This is a best-effort metric — a failure here does NOT affect job correctness.
	if err := rdb.Incr(ctx, KeyMetricProcessed).Err(); err != nil {
		slog.Warn("complete_metric_incr_failed", "job_id", j.ID, "error", err)
	}

	slog.Info("job_completed", "job_id", j.ID, "worker_id", workerID, "completed_at", now)
	return nil
}

