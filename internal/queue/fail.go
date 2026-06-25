// FailJob flow:
//  1. Remove the job ID from the worker's in-progress list (LREM, by value).
//  2. If retries remain: schedules a retry via ZADD jobs:retry (exponential backoff + jitter).
//  3. If retries exhausted: marks the job DEAD and pushes it to the dead-letter queue.
package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"time"

	"github.com/mitudk/distributed-job-scheduler/internal/job"
	"github.com/redis/go-redis/v9"
)

// Two outcomes based on retry budget:
//
//	A. Retries remain (j.RetryCount < j.MaxRetries):
//	   - Update metadata: status=FAILED, retry_count incremented, last_error recorded.
//	   - Append history event.
//	   - Schedule retry via ZADD jobs:retry {retryAt} {jobID}.
//	     retryAt = now + exponentialBackoff + jitter
//
//	B. Retries exhausted (j.RetryCount >= j.MaxRetries):
//	   - Update metadata: status=DEAD, last_error recorded.
//	   - Append history event.
//	   - RPUSH jobs:dead {jobID}   (dead-letter queue for operator inspection).
//	   - Set 7-day TTL on meta and history.
//
// Exponential backoff formula: delay = retryBaseDelaySeconds * 2^RetryCount
//
//	jitter = random 0-4 seconds (uniform)
//	retryAt = now + delay + jitter
//
// Why jitter is critical (thundering herd prevention):
//
//	Without jitter: 1000 jobs fail simultaneously. All scheduled to retry at T+10.
//	At T+10, all 1000 hit Redis at once → Redis becomes bottleneck → cascade failure.
//	With jitter (0–4s): retries spread across T+10 to T+14 → Redis handles them smoothly.
func FailJob(ctx context.Context, rdb *redis.Client, j *job.Job, workerID string, execErr error, retryBaseDelaySeconds int) error {
	now := time.Now().Unix()
	errMsg := execErr.Error()

	// 1. Remove from in-progress list.
	if _, err := rdb.LRem(ctx, InProgressKey(workerID), 1, j.ID).Result(); err != nil {
		slog.Warn("fail_lrem_failed",
			"job_id", j.ID,
			"worker_id", workerID,
			"error", err,
		)
	}

	if j.RetryCount < j.MaxRetries {
		return scheduleRetry(ctx, rdb, j, workerID, errMsg, now, retryBaseDelaySeconds)
	}
	return moveToDead(ctx, rdb, j, workerID, errMsg, now)
}

func scheduleRetry(ctx context.Context, rdb *redis.Client, j *job.Job, workerID, errMsg string, now int64, retryBaseDelaySeconds int) error {
	newRetryCount := j.RetryCount + 1

	backoff := time.Duration(retryBaseDelaySeconds) * time.Second * (1 << uint(j.RetryCount))
	jitter := time.Duration(rand.Intn(5)) * time.Second
	retryAt := now + int64((backoff + jitter).Seconds())

	event := completionEvent{
		From:     j.Status,
		To:       job.StatusFailed,
		At:       now,
		WorkerID: workerID,
		Error:    errMsg,
	}
	eventJSON, _ := json.Marshal(event)

	// scriptScheduleRetry atomically:
	//   1. LREM  inprogress list    — remove the job from the worker's in-progress list
	//   2. HSET  metadata hash      — status=FAILED, retry_count++, last_error
	//   3. RPUSH history list       — append status-transition event
	//   4. INCR  metrics:jobs:failed
	//   5. ZADD  jobs:retry score   — schedule retry at retryAt timestamp
	// All 5 steps in ONE Redis operation. Crash-safe.
	if err := scriptScheduleRetry.Run(ctx, rdb,
		[]string{
			InProgressKey(workerID), // KEYS[1]
			MetaKey(j.ID),           // KEYS[2]
			HistoryKey(j.ID),        // KEYS[3]
			KeyRetry,                // KEYS[4]
			KeyMetricFailed,         // KEYS[5]
		},
		j.ID,                          // ARGV[1]
		string(eventJSON),             // ARGV[2]
		newRetryCount,                 // ARGV[3]
		errMsg,                        // ARGV[4]
		retryAt,                       // ARGV[5] score for ZADD
	).Err(); err != nil {
		slog.Error("fail_retry_script_failed", "job_id", j.ID, "error", err)
		return fmt.Errorf("fail retry script for job %s: %w", j.ID, err)
	}

	slog.Info("job_failed_scheduled_retry",
		"job_id", j.ID,
		"worker_id", workerID,
		"retry_count", newRetryCount,
		"max_retries", j.MaxRetries,
		"retry_at", retryAt,
		"error", errMsg,
	)
	return nil
}

func moveToDead(ctx context.Context, rdb *redis.Client, j *job.Job, workerID, errMsg string, now int64) error {
	event := completionEvent{
		From:     j.Status,
		To:       job.StatusDead,
		At:       now,
		WorkerID: workerID,
		Error:    errMsg,
	}
	eventJSON, _ := json.Marshal(event)

	const ttlSeconds = 7 * 24 * 60 * 60 // 604800

	// scriptMoveToDead atomically:
	//   1. LREM  inprogress list    — remove the job from the worker's in-progress list
	//   2. HSET  metadata hash      — status=DEAD, last_error
	//   3. RPUSH jobs:dead          — push to dead-letter queue for operator inspection
	//   4. RPUSH history list       — append status-transition event
	//   5. INCR  metrics:jobs:dead
	//   6. EXPIRE metadata  604800  — 7-day TTL
	//   7. EXPIRE history   604800  — 7-day TTL
	if err := scriptMoveToDead.Run(ctx, rdb,
		[]string{
			InProgressKey(workerID), // KEYS[1]
			MetaKey(j.ID),           // KEYS[2]
			KeyDead,                 // KEYS[3]
			HistoryKey(j.ID),        // KEYS[4]
			KeyMetricDead,           // KEYS[5]
		},
		j.ID,              // ARGV[1]
		string(eventJSON), // ARGV[2]
		errMsg,            // ARGV[3]
		ttlSeconds,        // ARGV[4] TTL
	).Err(); err != nil {
		slog.Error("fail_dead_script_failed", "job_id", j.ID, "error", err)
		return fmt.Errorf("dead script for job %s: %w", j.ID, err)
	}

	slog.Warn("job_dead",
		"job_id", j.ID,
		"worker_id", workerID,
		"retry_count", j.RetryCount,
		"max_retries", j.MaxRetries,
		"error", errMsg,
	)
	return nil
}

