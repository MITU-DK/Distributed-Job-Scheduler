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

// on every Terminal transition, same shape as HistoryEvent in dequeue.go for a consistent audit trail.
type completionEvent struct {
	From     job.Status `json:"from"`
	To       job.Status `json:"to"`
	At       int64      `json:"at"`
	WorkerID string     `json:"worker_id,omitempty"`
	Error    string     `json:"error,omitempty"` // Only set on failure events.
}

func CompleteJob(ctx context.Context, rdb *redis.Client, j *job.Job, workerID string) error {
	now := time.Now().Unix()
	event := completionEvent{
		From:     j.Status,
		To:       job.StatusCompleted,
		At:       now,
		WorkerID: workerID,
	}
	eventJSON, _ := json.Marshal(event) //cannot fail

	const ttlSeconds = 7 * 24 * 60 * 60 // 604800

	// Atomically: LREM inprogress + HSET COMPLETED + RPUSH history + EXPIRE (meta,history) + INCR processed.
	if err := scriptCompleteJob.Run(ctx, rdb,
		[]string{
			InProgressKey(workerID), MetaKey(j.ID), HistoryKey(j.ID), KeyMetricProcessed}, // KEYS[1..4]
		j.ID, string(eventJSON), // ARGV[1,2]
		now,        // ARGV[3] completed_at
		ttlSeconds, // ARGV[4] TTL
	).Err(); err != nil {
		slog.Error("complete_script_failed", "job_id", j.ID, "error", err)
		return fmt.Errorf("complete script for job %s: %w", j.ID, err)
	}

	slog.Info("job_completed", "job_id", j.ID, "worker_id", workerID, "completed_at", now)
	return nil
}
