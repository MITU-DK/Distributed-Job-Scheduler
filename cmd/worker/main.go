// cmd/worker/main.go — Worker process entry point.
//
// Responsibilities:
//  1. Load configuration from environment variables & Connect to Redis.
//  2. Build the executor registry (maps job names → executor implementations).
//  3. Start the worker pool (dequeue → execute → ack/fail + heartbeat).
//  4. Start the scheduler (moves scheduled + retry jobs into the active queues).
//  5. Start the recovery scanner (rescues jobs from crashed workers).
//  6. Listen for SIGTERM / SIGINT and trigger graceful shutdown.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mitudk/distributed-job-scheduler/config"
	"github.com/mitudk/distributed-job-scheduler/internal/store"
	"github.com/mitudk/distributed-job-scheduler/internal/worker"
)

func main() {
	// Structured JSON logging
	// All logs are JSON so they can be ingested by log aggregators (Datadog, CloudWatch) without custom parsing.
	// slog is the standard library structured logger (Go 1.21+).
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	//Configuration-->Fail-fast: if any required env var is missing or invalid, exit now.
	// A misconfigured worker connecting to the wrong Redis is worse than no worker.
	cfg, err := config.Load()
	if err != nil {
		slog.Error("config_load_failed", "error", err)
		os.Exit(1)
	}
	//Redis connection
	rdb, err := store.NewClient(cfg)
	if err != nil {
		slog.Error("redis_connect_failed", "error", err)
		os.Exit(1)
	}
	defer rdb.Close()

	slog.Info("worker_starting", "worker_id", cfg.WorkerID, "concurrency", cfg.WorkerConcurrency, "redis_addr", cfg.RedisAddr)

	// Executor registry---> Register one executor per job type (job.Name maps to executor).
	// Adding a new job type in the future = register one line here.
	// The pool is completely unaware of what these executors do.
	registry := worker.NewRegistry()
	registry.Register("email", &worker.EmailExecutor{})
	registry.Register("backup", &worker.BackupExecutor{})
	registry.Register("cleanup", &worker.CleanupExecutor{})
	registry.Register("sleep", &worker.SleepExecutor{})

	// FailingExecutor is only registered when ENABLE_FAILING_EXECUTOR=true.
	// This keeps the fault-tolerance test executor completely out of production.
	if os.Getenv("ENABLE_FAILING_EXECUTOR") == "true" {
		slog.Warn("failing_executor_enabled", "note", "for testing only — DO NOT USE IN PRODUCTION")
		registry.Register("failing", &worker.FailingExecutor{Message: "injected test failure"})
	}

	// Graceful shutdown context
	// ctx is passed down into the pool, every worker goroutine, and the heartbeat.
	// When SIGTERM arrives, cancel() is called, and ctx.Done() fires everywhere.
	// Every goroutine that respects ctx will stop cleanly.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Signal handler
	// SIGTERM is sent by Docker / Kubernetes when stopping a container.
	// SIGINT is sent when you press Ctrl+C locally.
	// Using a buffered channel (size 1) so the OS can deliver the signal even if
	// our goroutine hasn't reached the <-sigCh line yet.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	pool := worker.New(rdb, cfg, registry)

	// Scheduler
	// Runs every SchedulerIntervalMs (default 1s).
	// Moves jobs from jobs:scheduled and jobs:retry into the active priority queues.
	// Fire-and-forget: it respects ctx so it stops cleanly on SIGTERM.
	scheduler := worker.NewScheduler(rdb, cfg)
	go scheduler.Run(ctx)

	// Recovery Scanner
	// Runs every RecoveryIntervalSeconds (default 60s).
	// Detects dead workers (missing heartbeat key) and re-enqueues their abandoned jobs.
	// Fire-and-forget: it also respects ctx.
	recovery := worker.NewRecoveryScanner(rdb, cfg)
	go recovery.Run(ctx)

	// Run() is called in a separate goroutine because it blocks until all workers finish draining.
	// We need to be able to receive the signal concurrently.
	done := make(chan struct{})
	go func() {
		pool.Run(ctx) //Start the pool (non-blocking, runs in its own goroutines)
		close(done)   // signals that all goroutines have exited
	}()

	// Wait for shutdown signal
	sig := <-sigCh
	slog.Info("worker_shutdown_signal", "signal", sig.String())

	//Cancel the context tells the pool, all worker goroutines, and the heartbeat to stop accepting new work
	//and exit as soon as their current operation finishes.
	cancel()

	// Wait for pool to fully drain with a hard timeout.
	// If workers don't finish within the timeout, we exit anyway (better a slightly dirty shutdown than a container that won't stop).
	timeout := time.Duration(cfg.GracefulShutdownTimeoutSeconds) * time.Second
	select {
	case <-done:
		slog.Info("worker_shutdown_complete", "worker_id", cfg.WorkerID)
	case <-time.After(timeout):
		slog.Warn("worker_shutdown_timeout",
			"worker_id", cfg.WorkerID,
			"timeout_seconds", cfg.GracefulShutdownTimeoutSeconds,
			"note", "some in-flight jobs may be re-enqueued by recovery",
		)
	}
}
