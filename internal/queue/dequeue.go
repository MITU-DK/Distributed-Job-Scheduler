// WHY NOT BLPOP ALONE?
//
//	Worker: BLPOP jobs:queue:p1       → gets job ID "abc"
//	Worker: starts processing job "abc"
//	Worker: CRASHES mid-execution
//	Result: job "abc" is GONE from Redis forever.
//	        No worker will ever pick it up again. This violates the at-least-once guarantee.
//
// BLPOP is a blocking list pop command in Redis. It removes and returns the first (head) element of a list.
//  If the list is empty, the connection blocks and waits until an element is pushed to the list or a specified timeout expires

// WHY LMOVE (RPOPLPUSH) IS SAFE:
//
//	Worker: LMOVE jobs:queue:p1 jobs:inprogress:worker-1 RIGHT LEFT
//	        → atomically moves "abc" from p1 to inprogress list
//	        → "abc" STILL EXISTS in Redis (just in a different list)
//	Worker: starts processing "abc"
//	Worker: CRASHES
//	Result: "abc" is still in jobs:inprogress:worker-1. The recovery process re-enqueues it.
//	        At-least-once guarantee preserved.
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

// HistoryEvent is written to jobs:history:{id} on every status transition.
type HistoryEvent struct {
	From     job.Status `json:"from"`
	To       job.Status `json:"to"`
	At       int64      `json:"at"`
	WorkerID string     `json:"worker_id,omitempty"` // why empty value is allowed here?
	// empty value is allowed here because of the recovery process.
	// If worker crashes, the job is moved back to the queue and the worker_id is not set. so worker_id is empty.
	// also in the case of scheduled jobs, worker_id is not set because the job is not picked up by any worker yet.
	// In both cases , the job is not processed , so the worker_id is not set.
	// if the job is completed successfully, the worker_id is set to the worker_id of the worker that completed the job.
}

// Dequeue atomically moves a job from the highest-available priority queue into the worker's in-progress list,
// then updates its status to IN_PROGRESS.
//
// Priority: STRICT — P1 checked before P2 before P3.
// Starvation note: if P1 always has jobs, P3 jobs may be delayed indefinitely.

// Mitigations (not implemented here): weighted random selection or aging.
// This is intentional — documenting the tradeoff is more valuable than implementing weighted random without understanding it.
//
// The function polls (with a 500ms sleep) until a job is found or ctx is cancelled.
// The sleep uses select so graceful shutdown can interrupt it instantly.
func Dequeue(ctx context.Context, rdb *redis.Client, workerID string) (*job.Job, error) {

	inProgressKey := InProgressKey(workerID)

	for {
		select {
		case <-ctx.Done(): // Check cancellation before every poll attempt.
			return nil, ctx.Err()
		default: // if ctx is not cancelled, continue to the next iteration. Just exit the select block immediately!
		}
		// Try each priority queue from highest to lowest.
		for _, qKey := range PriorityQueues {
			// syntax---> LMOVE source destination LEFT RIGHT
			// = pop from the LEFT of source, push to the RIGHT of destination (atomically).
			jobID, err := rdb.LMove(ctx, qKey, inProgressKey, "LEFT", "RIGHT").Result()
			if err == redis.Nil {
				continue // This queue is empty, try next.
			}
			if err != nil {
				return nil, fmt.Errorf("dequeue lmove from %s: %w", qKey, err)
			}

			// Found a job ID. Fetch its full metadata.
			fields, err := rdb.HGetAll(ctx, MetaKey(jobID)).Result()
			if err != nil {
				return nil, fmt.Errorf("dequeue hgetall for job %s: %w", jobID, err)
			}
			if len(fields) == 0 {
				// Metadata missing — job was orphaned (should not happen, but handle defensively).
				slog.Error("dequeue_missing_metadata", "job_id", jobID, "action", "discarding from inprogress")
				rdb.LRem(ctx, inProgressKey, 1, jobID) //nolint:errcheck
				continue
			}

			j, err := job.FromHash(fields)
			if err != nil {
				return nil, fmt.Errorf("dequeue parse job %s: %w", jobID, err)
			}

			// UPADATING METADATA (status to IN_PROGRESS + record who picked it up + write history).
			// Using a pipeline (not MULTI/EXEC) for performance. Failure mode:
			//   - If pipeline fails, status stays QUEUED but job is in inprogress list.
			//   - Recovery process handles this: any job in inprogress is re-enqueued.
			//   - Deliberate tradeoff: pipeline saves a round-trip on the hot path.
			now := time.Now().Unix()
			event := HistoryEvent{From: j.Status, To: job.StatusInProgress, At: now, WorkerID: workerID}
			eventJSON, _ := json.Marshal(event)

			pipe := rdb.Pipeline()
			pipe.HSet(ctx, MetaKey(j.ID), "status", string(job.StatusInProgress), "worker_id", workerID, "started_at", now)
			pipe.RPush(ctx, HistoryKey(j.ID), string(eventJSON))
			if _, err := pipe.Exec(ctx); err != nil {
				slog.Warn("dequeue_status_update_failed",
					"job_id", j.ID, "error", err, "note", "recovery will handle this")
			}

			j.Status = job.StatusInProgress
			j.WorkerID = workerID
			j.StartedAt = now

			return j, nil
		}

		// All queues empty — wait 500ms then try again.
		// select allows graceful shutdown to interrupt the sleep instantly.
		select {
		case <-time.After(500 * time.Millisecond):
			// Continue polling.
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// DequeueRetryReady fetches jobs from the retry sorted set whose retry time has arrived.
// Called by the scheduler loop. Limited to 10 per tick to avoid blocking the loop.
//
// ATOMICITY: We use scriptPromoteJob (Lua) to perform ZREM + RPUSH + HSET in a single
// Redis operation. This eliminates the crash-between-steps orphan bug that existed
// when these were separate Go calls:
//
//	Old flow: ZREM → <crash here> → RPUSH (job lost forever)
//	New flow: Lua script executes all three atomically — crash is irrelevant.
func DequeueRetryReady(ctx context.Context, rdb *redis.Client) ([]string, error) {
	now := float64(time.Now().Unix())

	// Fetch up to 10 job IDs whose retry time has passed.
	ids, err := rdb.ZRangeArgs(ctx, redis.ZRangeArgs{
		Key:     KeyRetry,
		Start:   0,
		Stop:    now,
		ByScore: true,
		Count:   10,
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("retry dequeue zrangebyscore: %w", err)
	}

	promoted := ids[:0] // same backing array, avoids allocation
	for _, id := range ids {
		priority, err := rdb.HGet(ctx, MetaKey(id), "priority").Int()
		if err != nil {
			slog.Error("retry_priority_read_failed", "job_id", id, "error", err)
			continue
		}
		// scriptPromoteJob atomically:
		//   1. ZREM  KeyRetry id          — claim the job; returns 0 if another scheduler already took it
		//   2. RPUSH jobs:queue:pX id     — place it in the correct priority queue
		//   3. HSET  jobs:meta:{id} status QUEUED
		result, err := scriptPromoteJob.Run(ctx, rdb,
			[]string{KeyRetry, queueKey(priority), MetaKey(id)}, id).Int()
		if err != nil {
			slog.Error("retry_promote_script_failed", "job_id", id, "error", err)
			continue
		}
		if result == 0 {
			// Another scheduler instance already claimed this job (idempotency).
			slog.Warn("retry_promote_already_claimed", "job_id", id)
			continue
		}
		promoted = append(promoted, id)
	}

	return promoted, nil
}

// DequeueScheduledReady fetches jobs from the scheduled sorted set whose run time has arrived.
// Called by the scheduler loop. Limited to 10 per tick to avoid blocking the loop.
//
// ATOMICITY: Same scriptPromoteJob Lua script as DequeueRetryReady, targeting KeyScheduled.
func DequeueScheduledReady(ctx context.Context, rdb *redis.Client) ([]string, error) {
	now := float64(time.Now().Unix())

	ids, err := rdb.ZRangeArgs(ctx, redis.ZRangeArgs{
		Key:     KeyScheduled,
		Start:   0,
		Stop:    now,
		ByScore: true,
		Count:   10,
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("scheduled dequeue zrangebyscore: %w", err)
	}

	promoted := ids[:0]
	for _, id := range ids {
		priority, err := rdb.HGet(ctx, MetaKey(id), "priority").Int()
		if err != nil {
			slog.Error("scheduled_priority_read_failed", "job_id", id, "error", err)
			continue
		}

		// scriptPromoteJob atomically: ZREM + RPUSH + HSET status=QUEUED.
		result, err := scriptPromoteJob.Run(ctx, rdb,
			[]string{KeyScheduled, queueKey(priority), MetaKey(id)}, id).Int()
		if err != nil {
			slog.Error("scheduled_promote_script_failed", "job_id", id, "error", err)
			continue
		}
		if result == 0 {
			slog.Warn("scheduled_promote_already_claimed", "job_id", id)
			continue
		}
		promoted = append(promoted, id)
	}

	return promoted, nil
}
