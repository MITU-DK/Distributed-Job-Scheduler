// other packages receive a *redis.Client from here, never create their own,centralising connection management.
package store

import (
	"context"
	"fmt"
	"time"

	"github.com/mitudk/distributed-job-scheduler/config"
	"github.com/redis/go-redis/v9"
)

// NewClient creates a Redis client, pings to confirm connectivity and returns client or an error.
func NewRedisClient(cfg *config.Config) (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	})

	// Give Redis 5 seconds to respond before we give up,Without a timeout, if Redis is unreachable, Ping blocks indefinitely
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil { //the ping-on-startup
		return nil, fmt.Errorf("redis ping failed (addr=%s): %w", cfg.RedisAddr, err)
	}

	return client, nil
}
