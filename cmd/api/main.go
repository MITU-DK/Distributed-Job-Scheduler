// Responsibilities:
//  1. Load configuration (fail fast if invalid)
//  2. Connect to Redis (fail fast if not)
//  3. Wire dependencies together (Server, Handler, Metrics)
//  4. Start the HTTP server
//  5. Handle graceful shutdown on SIGTERM / SIGINT
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/mitudk/distributed-job-scheduler/config"
	"github.com/mitudk/distributed-job-scheduler/internal/api"
	"github.com/mitudk/distributed-job-scheduler/internal/store"
)

func main() {
	// 1. Structured JSON logger.
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	// 2. Load configuration.
	cfg, err := config.Load()
	if err != nil {
		slog.Error("config_load_failed", "error", err)
		os.Exit(1)
	}
	slog.Info("config_loaded",
		"redis_addr", cfg.RedisAddr,
		"api_port", cfg.APIPort,
	)

	// 3. Connect to Redis.
	rdb, err := store.NewRedisClient(cfg)
	if err != nil {
		slog.Error("redis_connect_failed", "error", err)
		os.Exit(1)
	}
	defer rdb.Close()
	slog.Info("redis_connected", "addr", cfg.RedisAddr)

	// 4. Build the HTTP server (routes + middleware wired inside api.NewServer).
	srv := api.NewServer(cfg, rdb)

	// 5. Graceful shutdown:
	// Create a cancellable root context,on SIGTERM/SIGINT, call cancel() which signals srv.Serve() to start shutdown.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	// Run the server in a goroutine so we can listen for signals concurrently.
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Serve(ctx)
	}()

	// Block until either a signal or a server error.
	select {
	case sig := <-sigCh:
		slog.Info("shutdown_signal_received", "signal", sig.String())
		cancel() // Tell the server to start graceful shutdown.
		if err := <-errCh; err != nil {
			slog.Error("server_shutdown_error", "error", err)
			os.Exit(1)
		}
	case err := <-errCh:
		slog.Error("server_error", "error", err)
		os.Exit(1)
	}

	slog.Info("api_shutdown_complete")
}
