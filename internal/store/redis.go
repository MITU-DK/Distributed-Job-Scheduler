// Package store owns the Redis connection.
// All other packages receive a *redis.Client from here — they never create their own.
// This centralises connection management and makes the ping-on-startup check unavoidable.
package store

import (
	"context"
	"fmt"
	"time"

	"github.com/mitudk/distributed-job-scheduler/config"
	"github.com/redis/go-redis/v9"
)

// NewClient creates a Redis client, pings Redis to confirm connectivity,
// and returns the client or an error.
//
// Why ping-on-startup?
//   - Without a timeout, if Redis is unreachable, Ping blocks indefinitely
//     (or until the OS TCP timeout, which can be several minutes).
//   - 5 seconds is generous: if Redis cannot respond in 5s, something is seriously wrong.
//   - Fail-fast here prevents confusing errors deep in the call stack later.
//
// Why go-redis?
//   - It is the official, context-aware Redis client for Go.
//   - Every operation accepts context.Context, enabling graceful cancellation.
//   - It manages a connection pool automatically (default: 10 × GOMAXPROCS connections).
//     For a worker with 5 goroutines, this is more than sufficient.
func NewClient(cfg *config.Config) (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	})

	// Give Redis 5 seconds to respond before we give up.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis ping failed (addr=%s): %w", cfg.RedisAddr, err)
	}

	return client, nil
}
