// Status is a typed string, so it is self-documenting in logs, Redis, and JSON.
// Using iota integers would require a lookup table everywhere; string constants are self-evident.
package job

import "fmt"

// Status represents the lifecycle state of a job. valid state transitions.
// It is stored as a human-readable string in both Redis hashes and JSON API responses.
type Status string

const (
	StatusPending    Status = "PENDING"
	StatusQueued     Status = "QUEUED"
	StatusScheduled  Status = "SCHEDULED"
	StatusInProgress Status = "IN_PROGRESS"
	StatusCompleted  Status = "COMPLETED"
	StatusFailed     Status = "FAILED"
	StatusDead       Status = "DEAD"
)

// State machine:
//
//	PENDING     → QUEUED, SCHEDULED
//	QUEUED      → IN_PROGRESS
//	SCHEDULED   → QUEUED
//	IN_PROGRESS → COMPLETED, FAILED
//	FAILED      → QUEUED (on retry), DEAD (on max retries)
//	DEAD        → QUEUED (on manual retry via API)
//	COMPLETED   → (terminal — no further transitions)
var validTransitions = map[Status][]Status{
	StatusPending:    {StatusQueued, StatusScheduled},
	StatusQueued:     {StatusInProgress},
	StatusScheduled:  {StatusQueued},
	StatusInProgress: {StatusCompleted, StatusFailed},
	StatusFailed:     {StatusQueued, StatusDead},
	StatusDead:       {StatusQueued},
	StatusCompleted:  {}, // terminal state
}

// ValidTransition reports whether transitioning from → to is a legal state change.
// Every component that changes job status must call this guard before writing to Redis.
// If it returns false, the caller must treat it as a bug and log an error —
// silently corrupting job state is worse than crashing.
func ValidTransition(from, to Status) bool {
	allowed, ok := validTransitions[from]
	if !ok {
		return false // unknown source state
	}
	for _, available := range allowed {
		if available == to {
			return true
		}
	}
	return false
}

// ParseStatus converts a raw string (e.g. from Redis HGET) into a Status.
// Returns an error for any string not in the known set.
// This prevents silent corruption: an unknown status string must not be accepted.
func ParseStatus(s string) (Status, error) {
	st := Status(s)
	switch st {
	case StatusPending, StatusQueued, StatusScheduled,
		StatusInProgress, StatusCompleted, StatusFailed, StatusDead:
		return st, nil
	}
	return "", fmt.Errorf("unknown job status: %q", s)
}
