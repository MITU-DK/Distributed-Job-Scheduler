package queue

import (
	"context"
	"fmt"

	"github.com/mitudk/distributed-job-scheduler/internal/job"
	"github.com/redis/go-redis/v9"
)

// atomically claims a job from a dead worker's in-progress list
// and puts it back into the appropriate priority queue. Only the scanner that successfully removes the job can re-enqueue it.
func RecoverJob(ctx context.Context, rdb *redis.Client, jobID, workerID string, priority int) (bool, error) {
	claimed, err := scriptRecoverJob.Run(ctx, rdb,
		[]string{InProgressKey(workerID), HeartbeatKey(workerID), QueueKey(priority), MetaKey(jobID)}, jobID, string(job.StatusQueued)).Int()
	if err != nil {
		return false, fmt.Errorf("recover job %s: %w", jobID, err)
	}
	return claimed == 1, nil
}
