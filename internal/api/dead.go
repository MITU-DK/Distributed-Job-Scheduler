// Returns a paginated list of job IDs from the Dead-Letter Queue (jobs:dead),
// enriched with their full metadata (status, name, priority, error, timestamps).
//
// The Dead-Letter Queue is a Redis List. Jobs land here after exhausting all retries.
// This endpoint lets the dashboard display a "Dead Jobs" table so an operator can
// manually inspect which jobs failed, why, and potentially trigger a retry.
//
// Response shape:
//
//	{
//	  "total": 42,
//	  "jobs": [
//	    { "id": "abc-123", "name": "email", "priority": 1, "last_error": "...", ... },
//	    ...
//	  ]
//	}
package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/mitudk/distributed-job-scheduler/internal/queue"
	"github.com/redis/go-redis/v9"
)

// DeadJobsResponse is the JSON payload returned by GET /jobs/dead.
type DeadJobsResponse struct {
	Total int64     `json:"total"` // Total number of dead jobs in the queue.
	Jobs  []DeadJob `json:"jobs"`  // The page of jobs returned.
}

// DeadJob is a flat summary of a dead job, assembled from its Redis Hash fields.
type DeadJob struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	Priority   string `json:"priority"`
	Status     string `json:"status"`
	RetryCount string `json:"retry_count"`
	MaxRetries string `json:"max_retries"`
	LastError  string `json:"last_error"`
	EnqueuedAt string `json:"enqueued_at"`
}

// handleDeadJobs handles GET /jobs/dead.
//
// Query parameters:
//   - limit  (default: 50)  — max number of dead jobs to return per page.
//   - offset (default: 0)   — number of dead jobs to skip (for pagination).
func (h *Handler) handleDeadJobs(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Parse optional pagination query params.
	limit := parseIntParam(r, "limit", 50)
	offset := parseIntParam(r, "offset", 0)

	resp, err := fetchDeadJobs(ctx, h.rdb, int64(offset), int64(limit))
	if err != nil {
		slog.Error("dead_jobs_fetch_failed", "error", err)
		http.Error(w, `{"error":"failed to fetch dead jobs"}`, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		slog.Error("dead_jobs_encode_failed", "error", err)
	}
}

// fetchDeadJobs reads a page of job IDs from the jobs:dead list,then fetches each job's metadata hash in a single pipeline.
func fetchDeadJobs(ctx context.Context, rdb *redis.Client, offset, limit int64) (*DeadJobsResponse, error) {

	total, err := rdb.LLen(ctx, queue.KeyDead).Result() // LLEN gives us the total count without reading the whole list.
	if err != nil {
		return nil, err
	}

	if total == 0 {
		return &DeadJobsResponse{Total: 0, Jobs: []DeadJob{}}, nil
	}

	// LRANGE start stop — Redis is 0-indexed, stop is inclusive.
	// offset=0, limit=50 → LRANGE 0 49 → returns first 50 elements.
	stop := offset + limit - 1
	ids, err := rdb.LRange(ctx, queue.KeyDead, offset, stop).Result()
	if err != nil {
		return nil, err
	}

	// Fetch all metadata hashes in one pipeline round-trip.
	pipe := rdb.Pipeline()
	cmds := make([]*redis.MapStringStringCmd, len(ids))

	for i, id := range ids {
		cmds[i] = pipe.HGetAll(ctx, queue.MetaKey(id))
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, err
	}

	// Assemble the response list.
	jobs := make([]DeadJob, 0, len(ids))
	for i, id := range ids {
		fields := cmds[i].Val() // returns the actual map[string]string in 'field'.
		if len(fields) == 0 {
			// Metadata expired (7-day TTL) but the ID is still in the dead list.
			// Include a tombstone entry so the operator knows the ID existed.
			jobs = append(jobs, DeadJob{
				ID:     id,
				Status: "METADATA_EXPIRED",
			})
			continue
		}
		jobs = append(jobs, DeadJob{
			ID:         id,
			Name:       fields["name"],
			Priority:   fields["priority"],
			Status:     fields["status"],
			RetryCount: fields["retry_count"],
			MaxRetries: fields["max_retries"],
			LastError:  fields["last_error"],
			EnqueuedAt: fields["enqueued_at"],
		})
	}

	return &DeadJobsResponse{Total: total, Jobs: jobs}, nil
}

// parseIntParam reads an integer query parameter by name.
// If the parameter is missing or invalid, it returns the provided default value.
func parseIntParam(r *http.Request, name string, defaultVal int) int {
	raw := r.URL.Query().Get(name)
	if raw == "" {
		return defaultVal
	}
	val, err := strconv.Atoi(raw)
	if err != nil || val < 0 {
		return defaultVal
	}
	return val
}
