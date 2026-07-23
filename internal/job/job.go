// Job struct & serialisation  & deserialzation helpers for Redis.
package job

import (
	"encoding/json"
	"fmt"
	"strconv"
)

type Job struct {
	ID          string          `json:"id"`           // UUID v4
	Name        string          `json:"name"`         // executor type: "send_email", "run_backup", "process_payment", etc
	Priority    int             `json:"priority"`     // Priority: 1=highest, 2=medium, 3=lowest.
	ScheduledAt int64           `json:"scheduled_at"` //past or 0 run immeditely.
	Payload     json.RawMessage `json:"payload"`      //Raw JSON stored as bytes without parsing it,for the executor to deserialise.
	MaxRetries  int             `json:"max_retries"`  // 0 = never retry
	Status      Status          `json:"status"`
	RetryCount  int             `json:"retry_count"`
	LastError   string          `json:"last_error"`  // Error message from last failed attempt
	WorkerID    string          `json:"worker_id"`   // Set to worker UUID when IN_PROGRESS
	EnqueuedAt  int64           `json:"enqueued_at"` //(all Unix epoch seconds, int64)
	StartedAt   int64           `json:"started_at"`
	CompletedAt int64           `json:"completed_at"`
}

//Redis serialisation as strings, convert every field explicitly.

// converts a Job into the map format expected by Redis HSET.
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

// HGETALL result (map[string]string) back into a Job.
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
