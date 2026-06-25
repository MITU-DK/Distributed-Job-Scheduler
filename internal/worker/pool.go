// Package worker — pool.go
//
// Pool manages a fixed number of goroutines that dequeue and execute jobs.
// Each goroutine runs an independent loop: dequeue → execute → ack/fail.
//
// Responsibilities of Pool:
//  1. Launch N worker goroutines (N = cfg.WorkerConcurrency).
//  2. Run the heartbeat goroutine (proves this worker is alive to the recovery system).
//  3. Implement graceful shutdown: when ctx is cancelled, stop accepting new jobs and wait for all in-flight jobs to finish before returning.
//
// Why sync.WaitGroup?
//
//	Each goroutine calls wg.Add(1) before starting and wg.Done() when it exits.
//	pool.Run blocks on wg.Wait() after the ctx is cancelled.--->This guarantees no goroutine is abandoned mid-job when the process receives SIGTERM.
package worker

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/mitudk/distributed-job-scheduler/config"
	"github.com/mitudk/distributed-job-scheduler/internal/queue"
	"github.com/redis/go-redis/v9"
)

// Pool is the top-level worker component--->Create one per worker process with New(), then call Run(ctx).
type Pool struct {
	rdb      *redis.Client
	cfg      *config.Config
	registry *Registry
}

// New, constructs a Pool with all its dependencies injected.
// No global state — every value the pool needs is passed in explicitly.
func New(rdb *redis.Client, cfg *config.Config, registry *Registry) *Pool {
	return &Pool{
		rdb:      rdb,
		cfg:      cfg,
		registry: registry,
	}
}

// Run starts the pool and blocks until ctx is cancelled AND all in-flight jobs finish.
//
//	 Call pattern in main():
//		pool.Run(ctx)   ← blocks here
//		// ctx cancelled by SIGTERM handler
//		// Run returns only after all goroutines exit cleanly
func (p *Pool) Run(ctx context.Context) {
	var wg sync.WaitGroup

	// 1. Heartbeat goroutine
	// Writes workers:heartbeat:{workerID} EX {TTL} every HeartbeatInterval seconds.
	// If this process dies, the key expires within TTL seconds.
	// The recovery scanner checks for expired heartbeats to find dead workers.
	wg.Add(1)
	go func() {
		defer wg.Done()
		p.runHeartbeat(ctx)
	}()

	// 2. Worker goroutines
	// Each goroutine independently dequeues and executes jobs.
	// They share the same Redis client (go-redis handles connection pooling internally).
	for i := 0; i < p.cfg.WorkerConcurrency; i++ {
		wg.Add(1)
		go func(workerNum int) {
			defer wg.Done()
			p.runWorker(ctx, workerNum)
		}(i + 1)
	}

	slog.Info("worker_pool_started", "worker_id", p.cfg.WorkerID, "concurrency", p.cfg.WorkerConcurrency)

	// Block until context is cancelled (SIGTERM received by main).
	<-ctx.Done()
	slog.Info("worker_pool_draining", "worker_id", p.cfg.WorkerID, "note", "waiting for in-flight jobs to finish")

	// Block here until every goroutine (heartbeat + workers) calls wg.Done().
	// This is how graceful shutdown works: the process doesn't exit until
	// every job currently executing has completed or been safely handled.
	wg.Wait()
	slog.Info("worker_pool_stopped", "worker_id", p.cfg.WorkerID)
}

// runWorker is the hot loop for one worker goroutine.-->It polls Redis for jobs until ctx is cancelled.
func (p *Pool) runWorker(ctx context.Context, workerNum int) {
	slog.Info("worker_goroutine_started", "worker_id", p.cfg.WorkerID, "goroutine", workerNum)

	for {
		// Dequeue blocks internally (500ms sleep between polls). & Returns immediately with ctx.Err() when ctx is cancelled.
		j, err := queue.Dequeue(ctx, p.rdb, p.cfg.WorkerID)
		if err != nil {
			// ctx.Err() means graceful shutdown was requested — exit cleanly.
			if ctx.Err() != nil {
				slog.Info("worker_goroutine_exiting", "worker_id", p.cfg.WorkerID, "goroutine", workerNum)
				return
			}
			// Any other error (Redis connectivity issue) — log and keep trying.
			slog.Error("worker_dequeue_error", "worker_id", p.cfg.WorkerID, "goroutine", workerNum, "error", err)
			// Brief pause, just for logging error, before retrying to avoid hammering Redis on repeated errors.
			select {
			case <-time.After(2 * time.Second):
			case <-ctx.Done():
				return
			}
			continue
		}

		//Execute
		executor, ok := p.registry.Lookup(j.Name)
		if !ok {
			// No executor registered for this job type.
			// This is a configuration error, not a transient failure, so we don't want it to silently retry forever.
			slog.Error("worker_no_executor", "job_id", j.ID, "job_name", j.Name, "worker_id", p.cfg.WorkerID)

			// Fail the job immediately with a clear, descriptive error.
			// Use ackCtx (detached from the worker lifecycle) so this write succeeds even during shutdown.
			execErr := fmt.Errorf("no executor registered for job type: %q", j.Name)

			ackCtx, ackCancel := context.WithTimeout(context.Background(), 5*time.Second)

			if err := queue.FailJob(ackCtx, p.rdb, j, p.cfg.WorkerID, execErr, p.cfg.RetryBaseDelaySeconds); err != nil {
				slog.Error("worker_fail_job_error", "job_id", j.ID, "error", err)
			}
			ackCancel()
			continue
		}

		slog.Info("worker_job_started", "job_id", j.ID, "job_name", j.Name, "priority", j.Priority, "retry_count", j.RetryCount, "worker_id", p.cfg.WorkerID, "goroutine", workerNum)

		// Execute is called with the pool's ctx, so if SIGTERM arrives while a job is running, the executor receives the cancellation signal.
		// Well-written executors respect ctx and return early.
		execErr := executor.Execute(ctx, j)

		// ── Acknowledgement ───────────────────────────────────────────────────────
		// CRITICAL: We must NOT use the worker `ctx` here.
		//
		// Problem: if SIGTERM arrives and cancels `ctx` while the job is still
		// running (or just finished), the CompleteJob/FailJob Redis call will fail
		// instantly with context.Canceled — even though the job completed successfully.
		// Result: the job stays in the inprogress list and gets re-run by recovery.
		//
		// Fix: use a fresh, independent context with a short timeout (5s) that is
		// completely detached from the worker lifecycle. This guarantees the Redis
		// ack write succeeds regardless of when SIGTERM arrives.
		ackCtx, ackCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer ackCancel()

		if execErr == nil {
			if err := queue.CompleteJob(ackCtx, p.rdb, j, p.cfg.WorkerID); err != nil {
				slog.Error("worker_complete_job_error", "job_id", j.ID, "error", err)
			}
		} else {
			// ctx cancelled means SIGTERM interrupted Execute — executor returned early.
			// The job is still in the inprogress list — recovery will re-enqueue it safely.
			// We do NOT call FailJob here, so the job doesn't lose a retry attempt.
			if ctx.Err() != nil {
				slog.Info("worker_job_interrupted_by_shutdown", "job_id", j.ID, "worker_id", p.cfg.WorkerID,
					"note", "job remains in inprogress list; recovery will re-enqueue",
				)
				return
			}
			slog.Warn("worker_job_failed", "job_id", j.ID, "job_name", j.Name, "worker_id", p.cfg.WorkerID, "error", execErr)

			// Fail the job — use ackCtx (detached) so this write succeeds even during shutdown.
			if err := queue.FailJob(ackCtx, p.rdb, j, p.cfg.WorkerID, execErr, p.cfg.RetryBaseDelaySeconds); err != nil {
				slog.Error("worker_fail_job_error", "job_id", j.ID, "error", err)
			}
		}
	}
}

// runHeartbeat writes the heartbeat key to Redis on a fixed interval.
// The key has a TTL of HeartbeatTTLSeconds (default 30s).
// If this goroutine stops writing (because the process died), the key expires and the recovery scanner declares this worker dead.
func (p *Pool) runHeartbeat(ctx context.Context) {

	p.writeHeartbeat(ctx) // Write immediately on startup so the worker appears alive right away.

	ticker := time.NewTicker(time.Duration(p.cfg.WorkerHeartbeatIntervalSeconds) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			p.writeHeartbeat(ctx)
		case <-ctx.Done(): // Ctx cancelled (SIGTERM). Stop writing. The TTL will expire naturally.
			slog.Info("worker_heartbeat_stopped", "worker_id", p.cfg.WorkerID)
			return
		}
	}
}

// writeHeartbeat performs the actual Redis SETEX call.
func (p *Pool) writeHeartbeat(ctx context.Context) {
	ttl := time.Duration(p.cfg.WorkerHeartbeatTTLSeconds) * time.Second
	now := time.Now().Unix()

	if err := p.rdb.Set(ctx, queue.HeartbeatKey(p.cfg.WorkerID), now, ttl).Err(); err != nil {
		// Log but don't crash. The recovery scanner has a grace period (TTL) before
		// declaring a worker dead, so a single missed heartbeat is harmless.
		slog.Warn("worker_heartbeat_write_failed", "worker_id", p.cfg.WorkerID, "error", err)
	}
} // Set(ctx, key, value, expiration)
