// Package job defines the Job struct and its serialisation helpers for Redis.
// All fields use Unix timestamps (int64) instead of time.Time because
// time.Time does not serialise cleanly to/from Redis hash fields.
package job

import (
	"encoding/json"
	"fmt"
	"strconv"
)

// Job is the core data object of the scheduler.
// Every field is documented with its design rationale below.
type Job struct {
	// ── Identity
	ID string `json:"id"` // UUID v4 -> No sequential IDs, distributed systems safe, unique

	// String (not enum) so new job types can be added without changing the core.
	// The worker's executor registry is a map[string]Executor — fully extensible.
	Name string `json:"name"` // executor type: "send_email", "run_backup", "process_payment", etc

	// Scheduling
	Priority int `json:"priority"` // Priority: 1=highest, 2=medium, 3=lowest.

	// ScheduledAt is Unix timestamp (seconds) for when the job should run.
	// 0 means "run immediately" — the job goes directly to the priority queue.
	// If ScheduledAt is in the past, it is treated as 0 (run immediately).
	ScheduledAt int64 `json:"scheduled_at"`

	// ── Execution parameters

	// Payload is arbitrary JSON for the executor to deserialise.
	// json.RawMessage stores pre-serialised JSON without double-encoding.
	// The scheduler/queue never deserialises payload — only the executor does.
	// This avoids interface{} / map[string]interface{} everywhere.
	Payload json.RawMessage `json:"payload"` //Raw JSON stored as bytes without parsing it.
	//The scheduler does not care what's inside the payload. It just stores and forwards it.

	// MaxRetries is the per-job maximum retry count.
	// 0 = never retry (use for financial transactions or non-idempotent jobs).
	// The global default from config is used when the API caller does not specify.
	MaxRetries int `json:"max_retries"`

	// Status tracking
	Status     Status `json:"status"`
	RetryCount int    `json:"retry_count"` // Incremented BEFORE re-enqueue (see retry.go)
	LastError  string `json:"last_error"`  // Error message from last failed attempt
	WorkerID   string `json:"worker_id"`   // Set to worker UUID when IN_PROGRESS

	// Timestamps (all Unix epoch seconds, int64)
	// Why int64, not time.Time? becoz time.Time does not store cleanly in Redis hash fields.
	// Unix seconds are a single integer — trivial to store, compare, and serialise.
	EnqueuedAt  int64 `json:"enqueued_at"`
	StartedAt   int64 `json:"started_at"`
	CompletedAt int64 `json:"completed_at"`
}

//Redis serialisation
// Redis hashes store all values as strings. We convert every field explicitly.
// Using map[string]interface{} lets go-redis handle the string conversion.

// ToHash converts a Job into the map format expected by Redis HSET.
// Every field maps to the same name as its JSON tag (snake_case).
func ToHash(j *Job) map[string]interface{} {
	return map[string]interface{}{
		"id":           j.ID,
		"name":         j.Name,
		"priority":     strconv.Itoa(j.Priority),
		"scheduled_at": strconv.FormatInt(j.ScheduledAt, 10),
		"payload":      string(j.Payload),
		"max_retries":  strconv.Itoa(j.MaxRetries),
		"status":       string(j.Status),
		"retry_count":  strconv.Itoa(j.RetryCount),
		"last_error":   j.LastError,
		"worker_id":    j.WorkerID,
		"enqueued_at":  strconv.FormatInt(j.EnqueuedAt, 10),
		"started_at":   strconv.FormatInt(j.StartedAt, 10),
		"completed_at": strconv.FormatInt(j.CompletedAt, 10),
	}
}

// FromHash parses a Redis HGETALL result (map[string]string) back into a Job.
// Returns a descriptive error for any malformed field.
// An unknown status string is an error — we must not silently default.
func FromHash(m map[string]string) (*Job, error) {

	j := &Job{}
	var err error

	j.ID = m["id"]
	j.Name = m["name"]
	j.LastError = m["last_error"]
	j.WorkerID = m["worker_id"]

	if j.Priority, err = strconv.Atoi(m["priority"]); err != nil {
		return nil, fmt.Errorf("job %q: invalid priority %q: %w", j.ID, m["priority"], err)
	}
	if j.ScheduledAt, err = strconv.ParseInt(m["scheduled_at"], 10, 64); err != nil {
		return nil, fmt.Errorf("job %q: invalid scheduled_at %q: %w", j.ID, m["scheduled_at"], err)
	}
	if j.MaxRetries, err = strconv.Atoi(m["max_retries"]); err != nil {
		return nil, fmt.Errorf("job %q: invalid max_retries %q: %w", j.ID, m["max_retries"], err)
	}
	if j.RetryCount, err = strconv.Atoi(m["retry_count"]); err != nil {
		return nil, fmt.Errorf("job %q: invalid retry_count %q: %w", j.ID, m["retry_count"], err)
	}

	j.Status, err = ParseStatus(m["status"])
	if err != nil {
		return nil, fmt.Errorf("job %q: %w", j.ID, err)
	}

	if j.EnqueuedAt, err = strconv.ParseInt(m["enqueued_at"], 10, 64); err != nil {
		return nil, fmt.Errorf("job %q: invalid enqueued_at %q: %w", j.ID, m["enqueued_at"], err)
	}
	if j.StartedAt, err = strconv.ParseInt(m["started_at"], 10, 64); err != nil {
		return nil, fmt.Errorf("job %q: invalid started_at %q: %w", j.ID, m["started_at"], err)
	}
	if j.CompletedAt, err = strconv.ParseInt(m["completed_at"], 10, 64); err != nil {
		return nil, fmt.Errorf("job %q: invalid completed_at %q: %w", j.ID, m["completed_at"], err)
	}

	j.Payload = json.RawMessage(m["payload"])

	return j, nil
}
