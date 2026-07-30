// Responsibilities of Pool:
//  1. Launch N (cfg.WorkerConcurrency) worker goroutines. Each goroutine runs an independent loop: dequeue → execute → ack/fail.
//  2. Run the heartbeat goroutine
//  3. Implement graceful shutdown: when ctx is cancelled, stop accepting new jobs and wait for all in-flight jobs to finish before returning.
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

// Pool top-level worker component--->Create one per worker process with New(), then call Run(ctx).
type Pool struct {
	rdb      *redis.Client
	cfg      *config.Config
	registry *Registry
}

func New(rdb *redis.Client, cfg *config.Config, registry *Registry) *Pool {
	return &Pool{
		rdb:      rdb,
		cfg:      cfg,
		registry: registry,
	}
}

// Run starts the pool and blocks until ctx is cancelled AND all in-flight jobs finish. i.e.  returns only after all goroutines exit cleanly
func (p *Pool) Run(ctx context.Context) {
	var wg sync.WaitGroup
	// 1. Heartbeat goroutine,check on worker every TTL seconds.
	wg.Add(1)
	go func() {
		defer wg.Done()
		p.runHeartbeat(ctx)
	}()

	// 2. Worker goroutines--.> independently dequeues and executes jobs.(share the same Redis client)
	for i := 0; i < p.cfg.WorkerConcurrency; i++ {
		wg.Add(1)
		go func(workerNum int) {
			defer wg.Done()
			p.runWorker(ctx, workerNum)
		}(i + 1)
	}

	slog.Info("worker_pool_started", "worker_id", p.cfg.WorkerID, "concurrency", p.cfg.WorkerConcurrency)

	<-ctx.Done() // Block until context is cancelled
	slog.Info("worker_pool_draining", "worker_id", p.cfg.WorkerID, "note", "waiting for in-flight jobs to finish")

	wg.Wait() // Block here until every goroutine (heartbeat + workers) called wg.Done().
	slog.Info("worker_pool_stopped", "worker_id", p.cfg.WorkerID)
}

// Hot loop for one worker goroutine.-->It polls Redis for jobs until ctx is cancelled.
func (p *Pool) runWorker(ctx context.Context, workerNum int) {
	slog.Info("worker_goroutine_started", "worker_id", p.cfg.WorkerID, "goroutine", workerNum)

	for {
		// Dequeue blocks internally (500ms sleep between polls).
		j, err := queue.Dequeue(ctx, p.rdb, p.cfg.WorkerID)
		if err != nil {
			if ctx.Err() != nil { //graceful shutdown was requested — exit cleanly
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
		if !ok { // No executor registered for this job type.
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

		execErr := executor.Execute(ctx, j)

		ackCtx, ackCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer ackCancel()

		if execErr == nil {
			if err := queue.CompleteJob(ackCtx, p.rdb, j, p.cfg.WorkerID); err != nil {
				slog.Error("worker_complete_job_error", "job_id", j.ID, "error", err)
			}
		} else {
			if ctx.Err() != nil { //SIGTERM interrupted Execute — executor returned early.job still in the inprogress list — recovery will re-enqueue it automatically.
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

// (default 30s).
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

// Redis SETEX call. Set(ctx, key, value, expiration)
func (p *Pool) writeHeartbeat(ctx context.Context) {
	ttl := time.Duration(p.cfg.WorkerHeartbeatTTLSeconds) * time.Second
	now := time.Now().Unix()

	if err := p.rdb.Set(ctx, queue.HeartbeatKey(p.cfg.WorkerID), now, ttl).Err(); err != nil {
		slog.Warn("worker_heartbeat_write_failed", "worker_id", p.cfg.WorkerID, "error", err) //log it,but dont crash the worker. Heartbeat failures are non-fatal.
	} //one missed heartbeat is not a big deal — the next one will succeed if Redis is healthy.
}
