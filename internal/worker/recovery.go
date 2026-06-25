// Package worker — recovery.go
//
// The RecoveryScanner is a background loop that runs on a slow interval (default 60s).
// Its job is to rescue jobs from workers that have crashed without shutting down cleanly.
//
// How it knows a worker has crashed:
//
//	Every live worker writes a heartbeat key to Redis every 10s with a 30s TTL.
//	Key: workers:heartbeat:{workerID}   Value: Unix timestamp   TTL: 30s
//
//	If a worker is killed with SIGKILL (kill -9), its server loses power, or it panics,the heartbeat goroutine stops writing.
//
// After 30s, Redis automatically expires the key.
//
//	The RecoveryScanner checks: does this in-progress list have a live heartbeat key?
//	If not → the worker is dead → rescue its jobs.
//
// Recovery algorithm (per dead worker):
//
//  1. SCAN jobs:inprogress:*          → find all worker inprogress lists
//  2. For each list, check heartbeat  → if EXISTS, worker is alive; skip it
//  3. LRANGE inprogress list 0 -1    → read all job IDs in the dead worker's list
//  4. For each job ID:
//     - HGET jobs:meta:{id} priority → read its priority
//     - Pipeline: RPUSH jobs:queue:pX {id} + HSET status QUEUED
//  5. DEL jobs:inprogress:{workerID}  → clean up the dead list
//
// Why not process orphaned jobs from a single central authority?
//
//	In a distributed system, there is no single authority. Any live worker can act as the recovery agent.
//
// Because the recovery algorithm is idempotent (re-enqueuing the same job twice is harmless — the second worker will just execute it again,
//
//	and at-least-once delivery is our guarantee), multiple workers running recovery
//	simultaneously is safe. The duplicate will just execute the job an extra time.
package worker

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/mitudk/distributed-job-scheduler/config"
	"github.com/mitudk/distributed-job-scheduler/internal/job"
	"github.com/mitudk/distributed-job-scheduler/internal/queue"
	"github.com/redis/go-redis/v9"
)

// RecoveryScanner scans for dead workers and re-enqueues their abandoned jobs.
type RecoveryScanner struct {
	rdb *redis.Client
	cfg *config.Config
}

// NewRecoveryScanner creates a RecoveryScanner with injected dependencies.
func NewRecoveryScanner(rdb *redis.Client, cfg *config.Config) *RecoveryScanner {
	return &RecoveryScanner{rdb: rdb, cfg: cfg}
}

// Run starts the recovery scan loop and blocks until ctx is cancelled.
// Call this in a goroutine from main(). It will stop cleanly on SIGTERM.
func (r *RecoveryScanner) Run(ctx context.Context) {
	interval := time.Duration(r.cfg.RecoveryIntervalSeconds) * time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	slog.Info("recovery_scanner_started", "interval_seconds", r.cfg.RecoveryIntervalSeconds)

	// Run an initial scan immediately on startup.
	// Why? If this process was previously the only worker and crashed, we want to recover jobs as fast as possible, not wait 60s for the first tick.
	r.scan(ctx)

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

// scan performs one full pass over all inprogress keys and recovers dead workers.
func (r *RecoveryScanner) scan(ctx context.Context) {
	slog.Info("recovery_scan_started")

	// Step 1: SCAN all in-progress keys
	// SCAN is non-blocking (unlike KEYS). It paginates through the keyspace in small batches so it doesn't stall Redis.
	var cursor uint64
	seen := make(map[string]struct{}) // deduplication (SCAN can return duplicates)

	for {
		keys, nextCursor, err := r.rdb.Scan(ctx, cursor, queue.KeyInProgressPattern, 100).Result()
		if err != nil {
			slog.Error("recovery_scan_failed", "error", err)
			return
		}
		for _, k := range keys {
			seen[k] = struct{}{} // Why struct{} ? - It takes 0 bytes of memory, making it the most efficient way to store a set in Go.
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
		// Key format: jobs:inprogress:{workerID}. We split on ":" and take the third part.
		workerID := workerIDFromKey(inProgressKey)
		if workerID == "" {
			slog.Warn("recovery_malformed_inprogress_key", "key", inProgressKey)
			continue
		}

		// Skip our own inprogress list — we are clearly still alive.
		if workerID == r.cfg.WorkerID {
			continue
		}

		// Step 3: Check heartbeat
		// EXISTS returns 1 if the key is present, 0 if expired/missing.
		exists, err := r.rdb.Exists(ctx, queue.HeartbeatKey(workerID)).Result()
		if err != nil {
			slog.Error("recovery_heartbeat_check_failed", "worker_id", workerID, "error", err)
			continue
		}
		if exists > 0 { // Heartbeat is alive — this worker is healthy, do not touch its list
			continue
		}

		// Step 4: Worker is dead — rescue its jobs
		slog.Warn("recovery_dead_worker_found", "dead_worker_id", workerID, "inprogress_key", inProgressKey)
		deadCount++

		if err := r.recoverWorker(ctx, workerID, inProgressKey); err != nil {
			slog.Error("recovery_worker_failed", "dead_worker_id", workerID, "error", err)
		}
	}

	slog.Info("recovery_scan_complete", "dead_workers_found", deadCount)
}

// recoverWorker rescues all jobs from a single dead worker's in-progress list.
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
		// Read priority from job metadata.
		priority, err := r.rdb.HGet(ctx, queue.MetaKey(id), "priority").Int()
		if err != nil {
			slog.Error("recovery_priority_read_failed", "job_id", id, "dead_worker_id", workerID, "error", err)
			continue
		}

		// Atomically push back to queue + update status to QUEUED.
		pipe := r.rdb.Pipeline()
		pipe.RPush(ctx, queue.QueueKey(priority), id)
		pipe.HSet(ctx, queue.MetaKey(id), "status", string(job.StatusQueued), "worker_id", "") // Clear the dead worker's ID from the job.
		if _, err := pipe.Exec(ctx); err != nil {
			slog.Error("recovery_reenqueue_failed", "job_id", id, "dead_worker_id", workerID, "error", err)
			continue
		}

		slog.Info("recovery_job_recovered", "job_id", id, "dead_worker_id", workerID, "priority", priority)
		recoveredCount++
	}

	// Step 5: Delete the now-empty inprogress list
	// Even if some individual jobs failed to re-enqueue, we delete the list so we
	// don't keep re-attempting them on every recovery scan tick.
	if err := r.rdb.Del(ctx, inProgressKey).Err(); err != nil {
		slog.Warn("recovery_del_inprogress_failed", "dead_worker_id", workerID, "error", err)
	}

	slog.Warn("recovery_worker_rescued", "dead_worker_id", workerID, "jobs_recovered", recoveredCount, "jobs_total", len(jobIDs))
	return nil
}

// workerIDFromKey extracts the workerID from a key of the form "jobs:inprogress:{workerID}".
// The workerID itself can contain colons (UUID v4 does not, but user-supplied IDs might).
// We take everything after the second colon to be safe.
func workerIDFromKey(key string) string {
	// "jobs:inprogress:worker-uuid-1234"
	//       ^         ^
	//     idx0      idx1  → everything after is the workerID
	parts := strings.SplitN(key, ":", 3) // split into at most 3 parts
	if len(parts) < 3 {
		return ""
	}
	return parts[2]
}
