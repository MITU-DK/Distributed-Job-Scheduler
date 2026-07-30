// Responsibilities:
//  1. Load configuration & Connect to Redis.
//  2. Build the executor registry (maps job names -> executor implementations).
//  3. Start the worker pool (dequeue -> execute -> ack/fail + heartbeat).
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
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	cfg, err := config.Load()
	if err != nil {
		slog.Error("config_load_failed", "error", err)
		os.Exit(1)
	}

	rdb, err := store.NewRedisClient(cfg)
	if err != nil {
		slog.Error("redis_connect_failed", "error", err)
		os.Exit(1)
	}
	defer rdb.Close()

	slog.Info("worker_starting", "worker_id", cfg.WorkerID, "concurrency", cfg.WorkerConcurrency, "redis_addr", cfg.RedisAddr)

	// Executor registry---> Register one executor per job type (job Name maps to executor).
	registry := worker.NewRegistry()
	registry.Register("email", &worker.EmailExecutor{})
	registry.Register("backup", &worker.BackupExecutor{})
	registry.Register("cleanup", &worker.CleanupExecutor{})
	registry.Register("sleep", &worker.SleepExecutor{})

	// Register FailingExecutor
	if os.Getenv("ENABLE_FAILING_EXECUTOR") == "true" {
		slog.Warn("failing_executor_enabled", "note", "for testing only")
		registry.Register("failing", &worker.FailingExecutor{Message: "injected test failure"})
	}

	// Graceful shutdown context passed to worker goroutine and heartbeat
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Signal handler
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	pool := worker.New(rdb, cfg, registry)

	// Scheduler Run every SchedulerIntervalMs (default 1s).
	scheduler := worker.NewScheduler(rdb, cfg)
	go func() {
		scheduler.Run(ctx)
	}()
	// Recovery Scanner Runs every RecoveryIntervalSeconds (default 60s).
	recovery := worker.NewRecoveryScanner(rdb, cfg)
	go func() {
		recovery.Run(ctx)
	}()

	// Run() called in a separate goroutine becoz it blocks until all workers finish draining.
	// We need to be able to receive the signal concurrently.
	done := make(chan struct{})
	go func() {
		pool.Run(ctx) //Start the pool (non-blocking, runs in its own goroutines)
		close(done)   // signals that all goroutines have exited
	}()

	// Wait for shutdown signal
	sig := <-sigCh
	slog.Info("worker_shutdown_signal", "signal", sig.String())

	//tells the pool,worker goroutines,heartbeat to stop accepting new work and exit as soon as their current operation finishes.
	cancel()

	// Wait for pool to fully drain with a hard timeout.
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
