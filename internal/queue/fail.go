// FailJob flow:
//  1. Remove job ID from  worker's in-progress list (LREM, by value).
//  2. If retries remain: schedules a retry via ZADD jobs:retry (exponential backoff + jitter).
//  3. If exhausted: marks  job DEAD and pushes it to  dead-letter queue.
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

func FailJob(ctx context.Context, rdb *redis.Client, j *job.Job, workerID string, execErr error, retryBaseDelaySeconds int) error {
	now := time.Now().Unix()
	errMsg := execErr.Error()

	// 1. Remove from in-progress list.
	if _, err := rdb.LRem(ctx, InProgressKey(workerID), 1, j.ID).Result(); err != nil {

		slog.Warn("fail_lrem_failed", "job_id", j.ID, "worker_id", workerID, "error", err)
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

	// remove job from  worker's in-progress list
	// status=FAILED, retry_count++, last_error
	// append status-transition event
	//  metrics:jobs:failed
	// schedule retry at retryAt timestamp

	if err := scriptScheduleRetry.Run(ctx, rdb,
		[]string{
			InProgressKey(workerID),
			MetaKey(j.ID),
			HistoryKey(j.ID),
			KeyRetry,
			KeyMetricFailed,
		},
		j.ID,
		string(eventJSON),
		newRetryCount,
		errMsg,
		retryAt, // score for ZADD
	).Err(); err != nil {
		slog.Error("fail_retry_script_failed", "job_id", j.ID, "error", err)
		return fmt.Errorf("fail retry script for job %s: %w", j.ID, err)
	}

	slog.Info("job_failed_scheduled_retry",
		"job_id", j.ID, "worker_id", workerID, "retry_count", newRetryCount, "max_retries", j.MaxRetries, "retry_at", retryAt, "error", errMsg,
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

	//remove  job from  worker's in-progress list
	// status=DEAD, last_error
	// push to dead-letter queue for operator inspection
	//  append status-transition event
	// INCR  metrics:jobs:dead
	// EXPIRE metadata & history 604800  — 7-day TTL
	if err := scriptMoveToDead.Run(ctx, rdb,
		[]string{
			InProgressKey(workerID),
			MetaKey(j.ID),
			KeyDead,
			HistoryKey(j.ID),
			KeyMetricDead,
		},
		j.ID,
		string(eventJSON),
		errMsg,
		ttlSeconds, //TTL
	).Err(); err != nil {
		slog.Error("fail_dead_script_failed", "job_id", j.ID, "error", err)
		return fmt.Errorf("dead script for job %s: %w", j.ID, err)
	}

	slog.Warn("job_dead", "job_id", j.ID, "worker_id", workerID, "retry_count", j.RetryCount, "max_retries", j.MaxRetries, "error", errMsg)
	return nil

}
