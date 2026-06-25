// Tests for Dequeue use a REAL Redis instance (DB 15).
// Run TestMain (defined in enqueue_test.go) before these tests.
//
// Key behaviors verified:
//  1. Priority ordering — P1 is always dequeued before P2 before P3.
//  2. In-progress tracking — the job ID moves to the worker's inprogress list after Dequeue.
//  3. Metadata update — status becomes IN_PROGRESS, worker_id is set.
//  4. Context cancellation — Dequeue returns immediately when ctx is cancelled.
package queue

import (
	"context"
	"testing"
	"time"

	"github.com/mitudk/distributed-job-scheduler/internal/job"
)

// TestDequeue_PriorityOrdering enqueues one job in each priority, then calls Dequeue
// three times and verifies the order: P1 first, then P2, then P3.
func TestDequeue_PriorityOrdering(t *testing.T) {
	flush(t)
	ctx := context.Background()

	workerID := "test-worker-1"

	// Enqueue in reverse order to prove sorting is NOT insertion-order.
	p3 := newTestJob("low_priority_job", 3)
	p2 := newTestJob("medium_priority_job", 2)
	p1 := newTestJob("high_priority_job", 1)

	id3, _ := Enqueue(ctx, testRDB, p3, 3)
	id2, _ := Enqueue(ctx, testRDB, p2, 3)
	id1, _ := Enqueue(ctx, testRDB, p1, 3)

	// First dequeue must return P1.
	got1, err := Dequeue(ctx, testRDB, workerID)
	if err != nil {
		t.Fatalf("Dequeue 1 failed: %v", err)
	}
	if got1.ID != id1 {
		t.Errorf("expected P1 job (%s) first, got %s", id1, got1.ID)
	}

	// Second dequeue must return P2.
	got2, err := Dequeue(ctx, testRDB, workerID)
	if err != nil {
		t.Fatalf("Dequeue 2 failed: %v", err)
	}
	if got2.ID != id2 {
		t.Errorf("expected P2 job (%s) second, got %s", id2, got2.ID)
	}

	// Third dequeue must return P3.
	got3, err := Dequeue(ctx, testRDB, workerID)
	if err != nil {
		t.Fatalf("Dequeue 3 failed: %v", err)
	}
	if got3.ID != id3 {
		t.Errorf("expected P3 job (%s) third, got %s", id3, got3.ID)
	}
}

// TestDequeue_InProgressList verifies that after Dequeue, the job ID is in the
// worker's inprogress list (not just removed from the priority queue).
// This is the core guarantee of the at-least-once delivery design.
func TestDequeue_InProgressList(t *testing.T) {
	flush(t)
	ctx := context.Background()

	workerID := "test-worker-2"
	j := newTestJob("background_task", 1)
	id, _ := Enqueue(ctx, testRDB, j, 3)

	_, err := Dequeue(ctx, testRDB, workerID)
	if err != nil {
		t.Fatalf("Dequeue failed: %v", err)
	}

	// Job ID must be in the worker's inprogress list.
	members, err := testRDB.LRange(ctx, InProgressKey(workerID), 0, -1).Result()
	if err != nil {
		t.Fatalf("LRange inprogress failed: %v", err)
	}
	found := false
	for _, m := range members {
		if m == id {
			found = true
		}
	}
	if !found {
		t.Errorf("expected job %s in inprogress list %s, got %v", id, InProgressKey(workerID), members)
	}
}

// TestDequeue_StatusUpdatedToInProgress verifies that the Redis metadata hash
// is updated by Dequeue (status = IN_PROGRESS, worker_id = workerID).
func TestDequeue_StatusUpdatedToInProgress(t *testing.T) {
	flush(t)
	ctx := context.Background()

	workerID := "test-worker-3"
	j := newTestJob("resize_image", 2)
	id, _ := Enqueue(ctx, testRDB, j, 3)

	dequeued, err := Dequeue(ctx, testRDB, workerID)
	if err != nil {
		t.Fatalf("Dequeue failed: %v", err)
	}

	// Verify the returned struct has the correct status.
	if dequeued.Status != job.StatusInProgress {
		t.Errorf("returned job status: expected IN_PROGRESS, got %s", dequeued.Status)
	}
	if dequeued.WorkerID != workerID {
		t.Errorf("returned job worker_id: expected %s, got %s", workerID, dequeued.WorkerID)
	}

	// Verify Redis hash is also updated (not just the in-memory struct).
	status, _ := testRDB.HGet(ctx, MetaKey(id), "status").Result()
	if status != string(job.StatusInProgress) {
		t.Errorf("Redis hash status: expected IN_PROGRESS, got %q", status)
	}
	redisWorkerID, _ := testRDB.HGet(ctx, MetaKey(id), "worker_id").Result()
	if redisWorkerID != workerID {
		t.Errorf("Redis hash worker_id: expected %s, got %q", workerID, redisWorkerID)
	}
}

// TestDequeue_ContextCancellation verifies that Dequeue returns immediately
// when the context is cancelled, even if no jobs are in the queue.
// This is the graceful shutdown guarantee.
func TestDequeue_ContextCancellation(t *testing.T) {
	flush(t)

	workerID := "test-worker-cancel"
	ctx, cancel := context.WithCancel(context.Background())

	// Cancel the context after 100ms.
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	_, err := Dequeue(ctx, testRDB, workerID)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected error from cancelled context, got nil")
	}
	// Must return quickly (within 1s) — not wait for the full polling cycle.
	if elapsed > 1*time.Second {
		t.Errorf("Dequeue took too long to cancel: %v (expected < 1s)", elapsed)
	}
}

// TestDequeue_FIFOWithinPriority verifies that within the same priority,
// jobs are dequeued in the order they were enqueued (FIFO).
func TestDequeue_FIFOWithinPriority(t *testing.T) {
	flush(t)
	ctx := context.Background()

	workerID := "test-worker-fifo"

	// Enqueue two P1 jobs in order.
	j1 := newTestJob("first_job", 1)
	j2 := newTestJob("second_job", 1)

	id1, _ := Enqueue(ctx, testRDB, j1, 3)
	id2, _ := Enqueue(ctx, testRDB, j2, 3)

	// First out must be the first enqueued.
	got1, err := Dequeue(ctx, testRDB, workerID)
	if err != nil {
		t.Fatalf("Dequeue 1 failed: %v", err)
	}
	if got1.ID != id1 {
		t.Errorf("FIFO violation: expected %s first, got %s", id1, got1.ID)
	}

	got2, err := Dequeue(ctx, testRDB, workerID)
	if err != nil {
		t.Fatalf("Dequeue 2 failed: %v", err)
	}
	if got2.ID != id2 {
		t.Errorf("FIFO violation: expected %s second, got %s", id2, got2.ID)
	}
}
