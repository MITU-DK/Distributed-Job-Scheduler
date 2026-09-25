package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/mitudk/distributed-job-scheduler/internal/job"
	"github.com/redis/go-redis/v9"
)

// validates the job, assigns system fields(like uuid,etc for job), then writes it to Redis.
// For immediate jobs: stores metadata + pushes ID to priority queue (MULTI/EXEC).
// For scheduled jobs: stores metadata + adds ID to sorted set (MULTI/EXEC).
// (MULTI/EXEC).( ensures both commands succeed or neither does. but if one command fails, the other will not be rolled back. so must validate before writing to redis.)
func Enqueue(ctx context.Context, rdb *redis.Client, j *job.Job) (string, error) {
	// 1. Validate
	if err := validateJob(j); err != nil {
		return "", fmt.Errorf("enqueue validation: %w", err)
	}
	// MaxRetries is set by the caller (handler applies the default when not specified).
	// Do not override here — a value of 0 means "never retry" and must be preserved.

	j.ID = uuid.New().String() // 3. Assign job id
	j.RetryCount = 0
	j.EnqueuedAt = time.Now().Unix()
	now := time.Now().Unix() // 4. Decide: immediate or scheduled

	if j.ScheduledAt > 0 && j.ScheduledAt > now { // Future job -> scheduled set
		j.Status = job.StatusScheduled
		if err := enqueueScheduled(ctx, rdb, j); err != nil {
			return "", err
		}
		slog.Info("job_scheduled", "job_id", j.ID, "run_at", j.ScheduledAt)
	} else {
		if j.ScheduledAt > 0 && j.ScheduledAt <= now { // Immediate (or past scheduled_at -> treat as immediate)
			slog.Warn("scheduled_at_in_past_treating_as_immediate", "job_id", j.ID, "scheduled_at", j.ScheduledAt)
			j.ScheduledAt = 0
		}
		j.Status = job.StatusQueued
		if err := enqueueImmediate(ctx, rdb, j); err != nil {
			return "", err
		}
		slog.Info("job_enqueued", "job_id", j.ID, "priority", j.Priority, "queue", QueueKey(j.Priority))
	}

	return j.ID, nil
}

// writes metadata and pushes the job ID to the correct priority queue.
func enqueueImmediate(ctx context.Context, rdb *redis.Client, j *job.Job) error {
	pipe := rdb.TxPipeline() // MULTI/EXEC
	pipe.HSet(ctx, MetaKey(j.ID), job.ToHash(j))
	pipe.RPush(ctx, QueueKey(j.Priority), j.ID)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("enqueue immediate (job %s): %w", j.ID, err)
	}
	return nil
}

// writes metadata and adds the job ID to the scheduled sorted set.
func enqueueScheduled(ctx context.Context, rdb *redis.Client, j *job.Job) error {
	pipe := rdb.TxPipeline() // MULTI/EXEC
	pipe.HSet(ctx, MetaKey(j.ID), job.ToHash(j))
	pipe.ZAdd(ctx, KeyScheduled, redis.Z{
		Score:  float64(j.ScheduledAt),
		Member: j.ID,
	})
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("enqueue scheduled (job %s): %w", j.ID, err)
	}
	return nil
}

// checks all fields before any Redis writes happen. Fail-fast: do not touch Redis if the input is garbage.
func validateJob(j *job.Job) error {
	if j.Name == "" {
		return fmt.Errorf("name is required")
	}
	if len(j.Name) > 100 {
		return fmt.Errorf("name too long (max 100 chars)")
	}
	if j.Priority < 1 || j.Priority > 3 {
		return fmt.Errorf("priority must be 1, 2, or 3 (got %d)", j.Priority)
	}
	if len(j.Payload) > 0 && !json.Valid(j.Payload) {
		return fmt.Errorf("payload is not valid JSON")
	}
	if j.MaxRetries < 0 {
		return fmt.Errorf("max_retries must be >= 0")
	}
	return nil
}
