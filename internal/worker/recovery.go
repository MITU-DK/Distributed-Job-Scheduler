// background loop (default 60s).
// Recovery algorithm (per dead worker):
//
//  1. SCAN jobs:inprogress:*(workerid)          -> find all worker inprogress lists
//  2. For each list, check worker heartbeat  -> if EXISTS, worker is alive; skip it. ELSE
//  3. LRANGE inprogress list 0 -1    -> read all job IDs in the dead worker's list
//  4. For each job ID:
//     - HGET jobs:meta:{id} priority -> read its priority
//     - Pipeline: RPUSH jobs:queue:pX {id} + HSET status QUEUED
//  5. DEL jobs:inprogress:{workerID}  -> clean up the dead list
package worker

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/mitudk/distributed-job-scheduler/config"
	"github.com/mitudk/distributed-job-scheduler/internal/queue"
	"github.com/redis/go-redis/v9"
)

type RecoveryScanner struct {
	rdb *redis.Client
	cfg *config.Config
}

// creates a RecoveryScanner with injected dependencies.
func NewRecoveryScanner(rdb *redis.Client, cfg *config.Config) *RecoveryScanner {
	return &RecoveryScanner{rdb: rdb, cfg: cfg}
}

// starts recovery scan loop and blocks until ctx is cancelled.
func (r *RecoveryScanner) Run(ctx context.Context) {
	interval := time.Duration(r.cfg.RecoveryIntervalSeconds) * time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	slog.Info("recovery_scanner_started", "interval_seconds", r.cfg.RecoveryIntervalSeconds)

	r.scan(ctx) // Run an initial scan immediately on startup. recover preiously crashed jobs immediately.

	for {
		select {
		case <-ticker.C:
			r.scan(ctx)
		case <-ctx.Done():
			slog.Info("recovery_scanner_stopped")
			return
		}
	}
}

// Performs one full pass over all inprogress keys and recovers dead workers.
func (r *RecoveryScanner) scan(ctx context.Context) {
	slog.Info("recovery_scan_started")

	// Step 1: SCAN all in-progress keys (paginates so it doesn't stall Redis)
	var cursor uint64
	seen := make(map[string]struct{}) // deduplication (SCAN can return duplicates)

	for {
		keys, nextCursor, err := r.rdb.Scan(ctx, cursor, queue.KeyInProgressPattern, 100).Result()
		if err != nil {
			slog.Error("recovery_scan_failed", "error", err)
			return
		}
		for _, k := range keys {
			seen[k] = struct{}{} //empty struct value.
		}
		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}

	if len(seen) == 0 {
		slog.Info("recovery_scan_complete", "dead_workers_found", 0)
		return
	}

	deadCount := 0
	for inProgressKey := range seen {
		// Step 2: Extract workerID from key
		workerID := workerIDFromKey(inProgressKey)
		if workerID == "" {
			slog.Warn("recovery_malformed_inprogress_key", "key", inProgressKey)
			continue
		}

		// Skip our own inprogress list-- we are alive.
		if workerID == r.cfg.WorkerID {
			continue
		}

		// Step 3: Check heartbeat
		exists, err := r.rdb.Exists(ctx, queue.HeartbeatKey(workerID)).Result() // EXISTS returns 1 if the key is present, 0 if expired/missing.
		if err != nil {
			slog.Error("recovery_heartbeat_check_failed", "worker_id", workerID, "error", err)
			continue
		}
		if exists > 0 { // Heartbeat is alive
			continue
		}

		// Step 4: Worker is dead-- rescue its jobs
		slog.Warn("recovery_dead_worker_found", "dead_worker_id", workerID, "inprogress_key", inProgressKey)
		deadCount++

		if err := r.recoverWorker(ctx, workerID, inProgressKey); err != nil {
			slog.Error("recovery_worker_failed", "dead_worker_id", workerID, "error", err)
		}
	}

	slog.Info("recovery_scan_complete", "dead_workers_found", deadCount)
}

// Rescues all jobs from a single dead worker's in-progress list.
func (r *RecoveryScanner) recoverWorker(ctx context.Context, workerID, inProgressKey string) error {
	// Read all job IDs from the dead worker's inprogress list.
	jobIDs, err := r.rdb.LRange(ctx, inProgressKey, 0, -1).Result()
	if err != nil {
		return fmt.Errorf("lrange inprogress for dead worker %s: %w", workerID, err)
	}

	if len(jobIDs) == 0 {
		// Worker died but had no jobs in flight — just clean up the empty key.
		r.rdb.Del(ctx, inProgressKey) //nolint:errcheck
		slog.Info("recovery_worker_had_no_jobs", "dead_worker_id", workerID)
		return nil
	}

	recoveredCount := 0
	for _, id := range jobIDs {
		priority, err := r.rdb.HGet(ctx, queue.MetaKey(id), "priority").Int() // Read priority from job metadata.
		if err != nil {
			slog.Error("recovery_priority_read_failed", "job_id", id, "dead_worker_id", workerID, "error", err)
			continue
		}

		// Atomically claim, re-enqueue, and update the job status. If another recovery scanner already claimed this job, claimed is false.
		claimed, err := queue.RecoverJob(ctx, r.rdb, id, workerID, priority)
		if err != nil {
			slog.Error("recovery_reenqueue_failed", "job_id", id, "dead_worker_id", workerID, "error", err)
			continue
		}
		if !claimed {
			slog.Info("recovery_job_already_claimed", "job_id", id, "dead_worker_id", workerID)
			continue
		}

		slog.Info("recovery_job_recovered", "job_id", id, "dead_worker_id", workerID, "priority", priority)
		recoveredCount++
	}

	// Step 5: Delete the in-progress list only after every job was claimed. o/w leave it for next retry.
	remaining, err := r.rdb.LLen(ctx, inProgressKey).Result()
	if err != nil {
		slog.Warn("recovery_inprogress_length_failed", "dead_worker_id", workerID, "error", err)
	} else if remaining == 0 {
		if err := r.rdb.Del(ctx, inProgressKey).Err(); err != nil {
			slog.Warn("recovery_del_inprogress_failed", "dead_worker_id", workerID, "error", err)
		}
	} else {
		slog.Warn("recovery_jobs_remaining", "dead_worker_id", workerID, "jobs_remaining", remaining)
	}

	slog.Warn("recovery_worker_rescued", "dead_worker_id", workerID, "jobs_recovered", recoveredCount, "jobs_total", len(jobIDs))
	return nil
}

func workerIDFromKey(key string) string {
	parts := strings.SplitN(key, ":", 3) //  "jobs:inprogress:worker-uuid-1234" split into at most 3 parts
	if len(parts) < 3 {
		return ""
	}
	return parts[2]
}
