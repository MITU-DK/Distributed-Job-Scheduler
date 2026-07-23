// Returns a paginated list of job IDs from the Dead-Letter Queue (jobs:dead), alongwith full metadata (status, name, priority, error, timestamps).

// Response shape: after all retry failed.
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

// GET /jobs/dead.
type DeadJobsResponse struct {
	Total int64     `json:"total"` // Total number of dead jobs in the queue.
	Jobs  []DeadJob `json:"jobs"`  // The page of jobs returned.
}

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

// Query parameters:limit(default: 50),offset (default: 0)
func (h *Handler) handleDeadJobs(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
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

	total, err := rdb.LLen(ctx, queue.KeyDead).Result() // LLEN--> total count without reading the whole list.
	if err != nil {
		return nil, err
	}

	if total == 0 {
		return &DeadJobsResponse{Total: 0, Jobs: []DeadJob{}}, nil
	}

	stop := offset + limit - 1 //(Redis is 0-indexed, stop is inclusive.)
	ids, err := rdb.LRange(ctx, queue.KeyDead, offset, stop).Result()
	if err != nil {
		return nil, err
	}

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

// if is missing or invalid,return provided default value.
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
