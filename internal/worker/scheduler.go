// Package worker — scheduler.go
//
// The Scheduler is a background loop that runs on a fast interval (default 1s).
// It checks two Redis sorted sets and promotes ready jobs into the active queues:
//
//  1. jobs:scheduled — jobs the API submitted with a future scheduled_at timestamp.
//  2. jobs:retry     — jobs that previously failed and are waiting for their backoff to expire.
//
// Why a separate Scheduler goroutine?
//
//	The worker goroutines in pool.go ONLY watch the priority queues (jobs:queue:p1/p2/p3).
//	They never look at the sorted sets. This keeps the dequeue loop simple and fast.
//	The Scheduler is the "bridge" that moves jobs from sorted sets → priority queues
//	at the right moment, so the workers can pick them up as normal.
//
// Why a sorted set for scheduling?
//
//	Sorted sets use a score (Unix timestamp). ZRANGEBYSCORE 0 {now} returns all jobs
//	whose score (scheduled time) is in the past. This is an O(log N) range query —
//	much faster than scanning every job and comparing timestamps manually.
package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/mitudk/distributed-job-scheduler/config"
	"github.com/mitudk/distributed-job-scheduler/internal/queue"
	"github.com/redis/go-redis/v9"
)

// Scheduler ticks on a fixed interval and promotes ready jobs into the active queues.
type Scheduler struct {
	rdb *redis.Client
	cfg *config.Config
}

// NewScheduler creates a Scheduler with injected dependencies.
func NewScheduler(rdb *redis.Client, cfg *config.Config) *Scheduler {
	return &Scheduler{rdb: rdb, cfg: cfg}
}

// Run -->starts the scheduler tick loop and blocks until ctx is cancelled.
// Call this in a goroutine from main(). It will stop cleanly on SIGTERM.
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

// tick performs one scan of both sorted sets and promotes any ready jobs.
// Each call is O(log N + M) where M = number of jobs promoted this tick (capped at 10 per set).
func (s *Scheduler) tick(ctx context.Context) {
	// 1. Promote retry-ready jobs
	// These are jobs that previously failed and whose backoff delay has expired.
	retryIDs, err := queue.DequeueRetryReady(ctx, s.rdb)
	if err != nil {
		slog.Error("scheduler_retry_tick_failed", "error", err)
	} else if len(retryIDs) > 0 {
		slog.Info("scheduler_promoted_retry_jobs", "count", len(retryIDs), "job_ids", retryIDs)
	}

	//2. Promote scheduled-ready jobs
	// These are jobs the API submitted with a future scheduled_at timestamp.
	scheduledIDs, err := queue.DequeueScheduledReady(ctx, s.rdb)
	if err != nil {
		slog.Error("scheduler_scheduled_tick_failed", "error", err)
	} else if len(scheduledIDs) > 0 {
		slog.Info("scheduler_promoted_scheduled_jobs", "count", len(scheduledIDs), "job_ids", scheduledIDs)
	}
}
