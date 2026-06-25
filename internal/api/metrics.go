// Package api — metrics and health endpoints.
//
// Separated from handlers.go because these endpoints have different dependencies (they don't need config, only Redis)
// and a different concern (observability, not job management).
//
// Endpoints:
//
//	GET /health   → handleHealth   (liveness probe)
//	GET /metrics  → handleMetrics  (queue depths, counters)
package api

import (
	"log/slog"
	"net/http"

	"github.com/mitudk/distributed-job-scheduler/internal/queue"
	"github.com/redis/go-redis/v9"
)

// Metrics holds the Redis client for observability handlers.
type Metrics struct {
	rdb *redis.Client
}

type healthResponse struct { //JSON body for GET /health.
	Status string `json:"status"` // "ok" or "degraded"
	Redis  string `json:"redis"`  // "ok" or error message
}

type metricsResponse struct { //JSON body for GET /metrics.

	QueueDepthP1 int64 `json:"queue_depth_p1"` // queue depths (computed at request time via LLEN — O(1) in Redis).
	QueueDepthP2 int64 `json:"queue_depth_p2"`
	QueueDepthP3 int64 `json:"queue_depth_p3"`

	ScheduledCount int64 `json:"scheduled_count"` // Scheduled and retry sets (ZCARD — O(1) in Redis).
	RetryCount     int64 `json:"retry_count"`

	DeadCount int64 `json:"dead_count"` // Dead letter queue depth (LLEN — O(1)).

	TotalProcessed int64 `json:"total_processed"` // Lifetime counters (INCR'd by workers — already stored in Redis).
	TotalFailed    int64 `json:"total_failed"`
	TotalDead      int64 `json:"total_dead"`

	// Active workers: count of live heartbeat keys in Redis. Workers write  SET workers:heartbeat:{id} EX 30  every 10 seconds.
	// If the key is missing, the worker is considered dead.
	ActiveWorkers int64 `json:"active_workers"`
}

// handleHealth handles GET /health
// Used by Docker / Kubernetes as a liveness probe.
// Returns 200 if the server is running and can reach Redis., 503 if Redis is unreachable.
func (m *Metrics) handleHealth(w http.ResponseWriter, r *http.Request) {
	redisStatus := "ok"
	httpStatus := http.StatusOK

	if err := m.rdb.Ping(r.Context()).Err(); err != nil {
		redisStatus = err.Error()
		httpStatus = http.StatusServiceUnavailable
		slog.Warn("health_check_redis_failed", "error", err)
	}

	status := "ok"
	if httpStatus != http.StatusOK {
		status = "degraded"
	}
	// Response: {"status": "ok", "redis": "ok"}
	writeJSON(w, httpStatus, healthResponse{Status: status, Redis: redisStatus})
}

// handleMetrics handles GET /metrics--->Returns live queue depths and lifetime counters.
// All Redis reads are O(1) — this endpoint does not scan or iterate.
// Response: {"queue_depth_p1": 5, "total_processed": 1042, ...}
func (m *Metrics) handleMetrics(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	pipe := m.rdb.Pipeline()

	// Queue depths.
	p1Cmd := pipe.LLen(ctx, queue.QueueP1)
	p2Cmd := pipe.LLen(ctx, queue.QueueP2)
	p3Cmd := pipe.LLen(ctx, queue.QueueP3)

	// Sorted set counts.
	scheduledCmd := pipe.ZCard(ctx, queue.KeyScheduled)
	retryCmd := pipe.ZCard(ctx, queue.KeyRetry)

	// Dead letter queue.
	deadCmd := pipe.LLen(ctx, queue.KeyDead)

	// Metric counters.
	processedCmd := pipe.Get(ctx, queue.KeyMetricProcessed)
	failedCmd := pipe.Get(ctx, queue.KeyMetricFailed)
	deadMetricCmd := pipe.Get(ctx, queue.KeyMetricDead)

	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		slog.Error("metrics_pipeline_failed", "error", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "failed to fetch metrics", "")
		return
	}

	// Count active workers by scanning heartbeat keys.
	// SCAN is preferred over KEYS in production — it does not block the Redis server.
	// Since SCAN can return duplicate keys if the dataset is mutating, we use a map to deduplicate the keys before counting them.
	uniqueWorkers := make(map[string]struct{})
	var cursor uint64
	for {
		keys, nextCursor, err := m.rdb.Scan(ctx, cursor, queue.KeyHeartbeatPattern, 100).Result()
		if err != nil {
			slog.Warn("metrics_scan_heartbeats_failed", "error", err)
			break
		}
		for _, k := range keys {
			uniqueWorkers[k] = struct{}{}
		}
		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}
	activeWorkers := int64(len(uniqueWorkers))

	// toInt64 safely parses a string counter command result (0 if key does not exist yet).
	toInt64 := func(cmd *redis.StringCmd) int64 {
		n, _ := cmd.Int64()
		return n
	}

	writeJSON(w, http.StatusOK, metricsResponse{
		QueueDepthP1:   p1Cmd.Val(),
		QueueDepthP2:   p2Cmd.Val(),
		QueueDepthP3:   p3Cmd.Val(),
		ScheduledCount: scheduledCmd.Val(),
		RetryCount:     retryCmd.Val(),
		DeadCount:      deadCmd.Val(),
		TotalProcessed: toInt64(processedCmd),
		TotalFailed:    toInt64(failedCmd),
		TotalDead:      toInt64(deadMetricCmd),
		ActiveWorkers:  activeWorkers,
	})
}
