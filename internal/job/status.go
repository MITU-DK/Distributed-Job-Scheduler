package job

import "fmt"

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

// State Transition:
var validTransitions = map[Status][]Status{
	StatusPending:    {StatusQueued, StatusScheduled},
	StatusQueued:     {StatusInProgress},
	StatusScheduled:  {StatusQueued},
	StatusInProgress: {StatusCompleted, StatusFailed},
	StatusFailed:     {StatusQueued, StatusDead},
	StatusDead:       {StatusQueued},
	StatusCompleted:  {}, // terminal state
}

func ValidTransition(from, to Status) bool { //check if a transition from one state to another is valid
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

// converts a raw string (e.g. from Redis HGET) into a Status.
func ParseStatus(s string) (Status, error) {
	st := Status(s)
	switch st {
	case StatusPending, StatusQueued, StatusScheduled,
		StatusInProgress, StatusCompleted, StatusFailed, StatusDead:
		return st, nil
	}
	return "", fmt.Errorf("unknown job status: %q", s)
}
