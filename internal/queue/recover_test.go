package queue

import (
	"context"
	"sync"
	"testing"
)

func TestRecoverJobOnlyOneScannerCanClaim(t *testing.T) {
	flush(t)
	ctx := context.Background()
	workerID := "dead-worker"

	j := newTestJob("recoverable", 1)
	id, err := Enqueue(ctx, testRDB, j, 3)
	if err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}
	if _, err := Dequeue(ctx, testRDB, workerID); err != nil {
		t.Fatalf("dequeue failed: %v", err)
	}

	var wg sync.WaitGroup
	results := make([]bool, 2)
	errors := make([]error, 2)
	for i := range results {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			results[index], errors[index] = RecoverJob(ctx, testRDB, id, workerID, 1)
		}(i)
	}
	wg.Wait()
	for _, err := range errors {
		if err != nil {
			t.Fatalf("recover failed: %v", err)
		}
	}

	if results[0] == results[1] {
		t.Fatalf("expected exactly one scanner to claim the job, got %v", results)
	}

	queued, err := testRDB.LLen(ctx, QueueP1).Result()
	if err != nil {
		t.Fatalf("queue length failed: %v", err)
	}
	if queued != 1 {
		t.Fatalf("expected job to be queued once, got %d copies", queued)
	}

	inProgress, err := testRDB.LLen(ctx, InProgressKey(workerID)).Result()
	if err != nil {
		t.Fatalf("in-progress length failed: %v", err)
	}
	if inProgress != 0 {
		t.Fatalf("expected dead worker list to be empty, got %d jobs", inProgress)
	}
}
