// Tests for Enqueue use a REAL Redis instance (DB 15) rather than mocks.
//
// Why a real Redis and not a mock?
//   - go-redis has hundreds of command variations. Mocking it is fragile and verbose.
//   - A real Redis proves that our pipeline, key format, and data types are correct.
//   - We use DB 15 (never used in prod) and FLUSHDB before/after each test.
//     This gives us a clean slate without touching the application data.
//
// How to run:
//
//	docker-compose up -d redis
//	go test ./internal/queue/... -v -run TestEnqueue
package queue

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/mitudk/distributed-job-scheduler/internal/job"
	"github.com/redis/go-redis/v9"
)

// testRDB connects to a local Redis on DB 15 (the dedicated test database).
// Called once in TestMain and shared by all tests via the package-level variable.
var testRDB *redis.Client

// TestMain is the entry point for the test binary.
// It flushes the test database before running tests and again after.
// If Redis is not reachable, all tests skip (not fail) — Redis may not be running in CI.
func TestMain(m *testing.M) {
	testRDB = redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   15, // dedicated test DB — never used by the application
	})

	ctx := context.Background()
	if err := testRDB.Ping(ctx).Err(); err != nil {
		// Redis is not running. Skip all tests gracefully.
		// This avoids failing CI pipelines that don't have Redis.
		return
	}

	// Clean slate before test run.
	testRDB.FlushDB(ctx) //nolint:errcheck

	m.Run()

	// Clean up after test run so we don't leave test data in Redis.
	testRDB.FlushDB(ctx) //nolint:errcheck
	testRDB.Close()      //nolint:errcheck
}

// helper: builds a minimal valid job for tests.
func newTestJob(name string, priority int) *job.Job {
	return &job.Job{
		Name:     name,
		Priority: priority,
		Payload:  json.RawMessage(`{"test":true}`),
	}
}

// flush wipes DB 15 between individual tests that need an empty Redis.
func flush(t *testing.T) {
	t.Helper()
	if err := testRDB.FlushDB(context.Background()).Err(); err != nil {
		t.Fatalf("flush: %v", err)
	}
}

// ────────────────────────────────────────────────────────────────
// Validation tests (no Redis needed — fail before touching Redis)
// ────────────────────────────────────────────────────────────────

func TestEnqueue_Validation_EmptyName(t *testing.T) {
	j := &job.Job{Name: "", Priority: 1, Payload: json.RawMessage(`{}`)}
	_, err := Enqueue(context.Background(), testRDB, j, 3)
	if err == nil {
		t.Fatal("expected error for empty name, got nil")
	}
}

func TestEnqueue_Validation_InvalidPriority(t *testing.T) {
	j := &job.Job{Name: "test", Priority: 9, Payload: json.RawMessage(`{}`)}
	_, err := Enqueue(context.Background(), testRDB, j, 3)
	if err == nil {
		t.Fatal("expected error for invalid priority, got nil")
	}
}

func TestEnqueue_Validation_InvalidJSON(t *testing.T) {
	j := &job.Job{Name: "test", Priority: 1, Payload: json.RawMessage(`not-json`)}
	_, err := Enqueue(context.Background(), testRDB, j, 3)
	if err == nil {
		t.Fatal("expected error for invalid JSON payload, got nil")
	}
}

func TestEnqueue_Validation_NegativeMaxRetries(t *testing.T) {
	j := &job.Job{Name: "test", Priority: 1, MaxRetries: -1, Payload: json.RawMessage(`{}`)}
	_, err := Enqueue(context.Background(), testRDB, j, 3)
	if err == nil {
		t.Fatal("expected error for negative max_retries, got nil")
	}
}

// ────────────────────────────────────────────────────────────────
// Immediate enqueue tests
// ────────────────────────────────────────────────────────────────

func TestEnqueue_Immediate_AppearsInCorrectPriorityQueue(t *testing.T) {
	flush(t)
	ctx := context.Background()

	j := newTestJob("send_email", 2) // priority 2 → jobs:queue:p2
	id, err := Enqueue(ctx, testRDB, j, 3)
	if err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	// The job ID must appear in jobs:queue:p2.
	members, err := testRDB.LRange(ctx, QueueP2, 0, -1).Result()
	if err != nil {
		t.Fatalf("LRANGE failed: %v", err)
	}
	if len(members) != 1 || members[0] != id {
		t.Fatalf("expected [%s] in %s, got %v", id, QueueP2, members)
	}

	// P1 and P3 must be empty.
	if n, _ := testRDB.LLen(ctx, QueueP1).Result(); n != 0 {
		t.Errorf("expected P1 queue empty, got %d items", n)
	}
	if n, _ := testRDB.LLen(ctx, QueueP3).Result(); n != 0 {
		t.Errorf("expected P3 queue empty, got %d items", n)
	}
}

func TestEnqueue_Immediate_MetadataHashIsCorrect(t *testing.T) {
	flush(t)
	ctx := context.Background()

	j := newTestJob("run_backup", 1)
	id, err := Enqueue(ctx, testRDB, j, 3)
	if err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	fields, err := testRDB.HGetAll(ctx, MetaKey(id)).Result()
	if err != nil {
		t.Fatalf("HGetAll failed: %v", err)
	}
	if len(fields) == 0 {
		t.Fatal("expected metadata hash to exist, got empty map")
	}

	// Spot-check the most important fields.
	if fields["status"] != string(job.StatusQueued) {
		t.Errorf("expected status QUEUED, got %q", fields["status"])
	}
	if fields["name"] != "run_backup" {
		t.Errorf("expected name run_backup, got %q", fields["name"])
	}
	if fields["priority"] != "1" {
		t.Errorf("expected priority 1, got %q", fields["priority"])
	}
	if fields["id"] != id {
		t.Errorf("expected id %s, got %q", id, fields["id"])
	}
}

func TestEnqueue_Immediate_DefaultMaxRetriesApplied(t *testing.T) {
	flush(t)
	ctx := context.Background()

	j := newTestJob("process_payment", 1) // MaxRetries not set → default should be applied
	id, err := Enqueue(ctx, testRDB, j, 5) // default = 5
	if err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	maxRetries, err := testRDB.HGet(ctx, MetaKey(id), "max_retries").Result()
	if err != nil {
		t.Fatalf("HGet max_retries failed: %v", err)
	}
	if maxRetries != "5" {
		t.Errorf("expected max_retries=5, got %q", maxRetries)
	}
}

// ────────────────────────────────────────────────────────────────
// Scheduled enqueue tests
// ────────────────────────────────────────────────────────────────

func TestEnqueue_Scheduled_AppearsInSortedSet(t *testing.T) {
	flush(t)
	ctx := context.Background()

	runAt := time.Now().Add(10 * time.Minute).Unix() // 10 minutes in the future
	j := newTestJob("generate_report", 1)
	j.ScheduledAt = runAt

	id, err := Enqueue(ctx, testRDB, j, 3)
	if err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	// Must appear in the scheduled sorted set with the correct score.
	score, err := testRDB.ZScore(ctx, KeyScheduled, id).Result()
	if err != nil {
		t.Fatalf("ZScore failed — job not in scheduled set: %v", err)
	}
	if int64(score) != runAt {
		t.Errorf("expected score %d, got %g", runAt, score)
	}

	// Must NOT appear in any priority queue.
	for _, q := range PriorityQueues {
		if n, _ := testRDB.LLen(ctx, q).Result(); n != 0 {
			t.Errorf("expected queue %s empty for scheduled job, got %d items", q, n)
		}
	}
}

func TestEnqueue_Scheduled_StatusIsScheduled(t *testing.T) {
	flush(t)
	ctx := context.Background()

	j := newTestJob("nightly_cleanup", 2)
	j.ScheduledAt = time.Now().Add(1 * time.Hour).Unix()

	id, err := Enqueue(ctx, testRDB, j, 3)
	if err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	status, err := testRDB.HGet(ctx, MetaKey(id), "status").Result()
	if err != nil {
		t.Fatalf("HGet status failed: %v", err)
	}
	if status != string(job.StatusScheduled) {
		t.Errorf("expected status SCHEDULED, got %q", status)
	}
}

// TestEnqueue_PastScheduledAt verifies that a job with a scheduled_at in the past
// is treated as an immediate job (sent to priority queue, not sorted set).
func TestEnqueue_PastScheduledAt_TreatedAsImmediate(t *testing.T) {
	flush(t)
	ctx := context.Background()

	j := newTestJob("stale_job", 3)
	j.ScheduledAt = time.Now().Add(-5 * time.Minute).Unix() // 5 minutes in the PAST

	id, err := Enqueue(ctx, testRDB, j, 3)
	if err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	// Must appear in P3 queue (immediate path), NOT in the scheduled set.
	members, err := testRDB.LRange(ctx, QueueP3, 0, -1).Result()
	if err != nil {
		t.Fatalf("LRANGE failed: %v", err)
	}
	found := false
	for _, m := range members {
		if m == id {
			found = true
		}
	}
	if !found {
		t.Errorf("expected job %s in P3 queue, got %v", id, members)
	}

	// Must NOT be in scheduled set.
	if _, err := testRDB.ZScore(ctx, KeyScheduled, id).Result(); err != redis.Nil {
		t.Errorf("expected job absent from scheduled set, but ZScore returned: %v", err)
	}
}
