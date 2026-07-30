// background loop(default 1s).
//  1. jobs:scheduled -- API submitted with a future scheduled_at timestamp.
//  2. jobs:retry   -- previously failed and waiting for their backoff to expire.
package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/mitudk/distributed-job-scheduler/config"
	"github.com/mitudk/distributed-job-scheduler/internal/queue"
	"github.com/redis/go-redis/v9"
)

type Scheduler struct {
	rdb *redis.Client
	cfg *config.Config
}

func NewScheduler(rdb *redis.Client, cfg *config.Config) *Scheduler {
	return &Scheduler{rdb: rdb, cfg: cfg}
}

// starts scheduler tick loop and blocks until ctx is cancelled.
func (s *Scheduler) Run(ctx context.Context) {
	interval := time.Duration(s.cfg.SchedulerIntervalMs) * time.Millisecond
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	slog.Info("scheduler_started", "interval_ms", s.cfg.SchedulerIntervalMs)

	for {
		select {
		case <-ticker.C:
			s.tick(ctx)
		case <-ctx.Done():
			slog.Info("scheduler_stopped")
			return
		}
	}
}

// Scan both sorted sets and promotes any ready jobs.
func (s *Scheduler) tick(ctx context.Context) {
	retryIDs, err := queue.DequeueRetryReady(ctx, s.rdb) //whose backoff delay has expired.
	if err != nil {
		slog.Error("scheduler_retry_tick_failed", "error", err)
	} else if len(retryIDs) > 0 {
		slog.Info("scheduler_promoted_retry_jobs", "count", len(retryIDs), "job_ids", retryIDs)
	}

	scheduledIDs, err := queue.DequeueScheduledReady(ctx, s.rdb) //scheduled-ready jobs
	if err != nil {
		slog.Error("scheduler_scheduled_tick_failed", "error", err)
	} else if len(scheduledIDs) > 0 {
		slog.Info("scheduler_promoted_scheduled_jobs", "count", len(scheduledIDs), "job_ids", scheduledIDs)
	}
}
