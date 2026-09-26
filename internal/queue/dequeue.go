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

// written to jobs:history:{id} on every status transition.
type HistoryEvent struct {
	From     job.Status `json:"from"`
	To       job.Status `json:"to"`
	At       int64      `json:"at"`
	WorkerID string     `json:"worker_id,omitempty"`
}

func Dequeue(ctx context.Context, rdb *redis.Client, workerID string) (*job.Job, error) {

	inProgressKey := InProgressKey(workerID)

	for {
		select {
		case <-ctx.Done(): // Check cancellation before every poll attempt.
			return nil, ctx.Err()
		default: // if ctx is not cancelled, continue to the next iteration. Just exit the select block immediately!
		}

		for _, qKey := range PriorityQueues { // Try each priority queue from highest to lowest.

			jobID, err := rdb.LMove(ctx, qKey, inProgressKey, "LEFT", "RIGHT").Result() //LMOVE source destination LEFT RIGHT
			if err == redis.Nil {
				continue // This queue is empty, try next.
			}
			if err != nil {
				return nil, fmt.Errorf("dequeue lmove from %s: %w", qKey, err)
			}

			fields, err := rdb.HGetAll(ctx, MetaKey(jobID)).Result() // Found a job ID. Fetch its full metadata.
			if err != nil {
				return nil, fmt.Errorf("dequeue hgetall for job %s: %w", jobID, err)
			}
			if len(fields) == 0 { // Metadata missing — job was orphaned (should not happen, but handle defensively).
				slog.Error("dequeue_missing_metadata", "job_id", jobID, "action", "discarding from inprogress")
				rdb.LRem(ctx, inProgressKey, 1, jobID) //nolint:errcheck
				continue
			}

			j, err := job.FromHash(fields)
			if err != nil {
				return nil, fmt.Errorf("dequeue parse job %s: %w", jobID, err)
			}

			// UPADATING METADATA (status to IN_PROGRESS + record who picked it up + write history).
			now := time.Now().Unix()
			event := HistoryEvent{From: j.Status, To: job.StatusInProgress, At: now, WorkerID: workerID}
			eventJSON, _ := json.Marshal(event)

			if err := scriptMarkInProgress.Run(ctx, rdb, []string{MetaKey(j.ID), HistoryKey(j.ID)},
				string(job.StatusInProgress), workerID, now, string(eventJSON)).Err(); err != nil {
				slog.Warn("dequeue_status_update_failed",
					"job_id", j.ID, "error", err, "note", "recovery will handle this")
			}

			j.Status = job.StatusInProgress
			j.WorkerID = workerID
			j.StartedAt = now

			return j, nil
		}

		select { // All queues empty — wait 500ms then try again.
		case <-time.After(500 * time.Millisecond):
			// Continue polling.
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// Called by the scheduler loop. Drains all ready retry jobs in batches of 100 per round-trip.
func DequeueRetryReady(ctx context.Context, rdb *redis.Client) ([]string, error) {
	var allPromoted []string

	for {
		now := float64(time.Now().Unix())
		ids, err := rdb.ZRangeArgs(ctx, redis.ZRangeArgs{
			Key:     KeyRetry,
			Start:   0,
			Stop:    now,
			ByScore: true,
			Count:   100, // 100 per round-trip; loop until 0 returned
		}).Result()
		if err != nil {
			return allPromoted, fmt.Errorf("retry dequeue zrangebyscore: %w", err)
		}
		if len(ids) == 0 {
			break
		}

		for _, id := range ids {
			priority, err := rdb.HGet(ctx, MetaKey(id), "priority").Int()
			if err != nil {
				slog.Error("retry_priority_read_failed", "job_id", id, "error", err)
				continue
			}
			result, err := scriptPromoteJob.Run(ctx, rdb, []string{KeyRetry, QueueKey(priority), MetaKey(id)}, id).Int()
			if err != nil {
				slog.Error("retry_promote_script_failed", "job_id", id, "error", err)
				continue
			}
			if result == 0 {
				slog.Warn("retry_promote_already_claimed", "job_id", id)
				continue
			}
			allPromoted = append(allPromoted, id)
		}

		if len(ids) < 100 {
			break // fetched fewer than a full batch — sorted set is now empty
		}
	}

	return allPromoted, nil
}

// Drains all ready scheduled jobs in batches of 100 per round-trip.
func DequeueScheduledReady(ctx context.Context, rdb *redis.Client) ([]string, error) {
	var allPromoted []string

	for {
		now := float64(time.Now().Unix())
		ids, err := rdb.ZRangeArgs(ctx, redis.ZRangeArgs{
			Key:     KeyScheduled,
			Start:   0,
			Stop:    now,
			ByScore: true,
			Count:   100,
		}).Result()
		if err != nil {
			return allPromoted, fmt.Errorf("scheduled dequeue zrangebyscore: %w", err)
		}
		if len(ids) == 0 {
			break
		}

		for _, id := range ids {
			priority, err := rdb.HGet(ctx, MetaKey(id), "priority").Int()
			if err != nil {
				slog.Error("scheduled_priority_read_failed", "job_id", id, "error", err)
				continue
			}
			result, err := scriptPromoteJob.Run(ctx, rdb, []string{KeyScheduled, QueueKey(priority), MetaKey(id)}, id).Int()
			if err != nil {
				slog.Error("scheduled_promote_script_failed", "job_id", id, "error", err)
				continue
			}
			if result == 0 {
				slog.Warn("scheduled_promote_already_claimed", "job_id", id)
				continue
			}
			allPromoted = append(allPromoted, id)
		}

		if len(ids) < 100 {
			break
		}
	}

	return allPromoted, nil
}
