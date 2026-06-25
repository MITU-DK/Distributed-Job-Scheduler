// Package worker — executor.go
//
// The Executor interface is the heart of the job dispatch system.
// It decouples WHAT a job does from HOW the pool manages goroutines.
//
// Why an interface?
//   - The pool only knows how to fetch a job and call Execute(). It has zero knowledge of emails, backups, or any business logic.
//   - Adding a new job type means writing one new struct.
//     The pool, retry logic, and heartbeat are completely untouched.
//   - In tests, we can inject a MockExecutor with deterministic behaviour without spinning up any real infrastructure.
package worker

import (
	"context"
	"fmt"
	"time"

	"github.com/mitudk/distributed-job-scheduler/internal/job"
)

// Executor is the single method every job type must implement.
//
//	ctx — carries the worker's cancellation signal (graceful shutdown).
//	     If ctx is cancelled while Execute is running, the executor should respect it and return ctx.Err() as soon as possible.
//	j   — the full Job struct including Payload for business logic.
type Executor interface {
	Execute(ctx context.Context, j *job.Job) error
}

// Registry maps a job Name to its Executor.
// The pool looks up the executor by job.Name before calling Execute.
// If no executor is registered for a name, the job is failed immediately.
type Registry struct {
	executors map[string]Executor
}

// NewRegistry creates an empty registry.
func NewRegistry() *Registry {
	return &Registry{executors: make(map[string]Executor)}
}

// Register adds an executor for a given job name.
// Call this at startup before the pool starts dequeuing.
func (r *Registry) Register(name string, e Executor) {
	r.executors[name] = e
}

// Lookup returns the Executor for the given job name, or false if not found.
func (r *Registry) Lookup(name string) (Executor, bool) {
	e, ok := r.executors[name]
	return e, ok
}

// ─── Concrete Executors (dummy implementations for development & testing) ───

// EmailExecutor simulates sending an email.
// In a real system this would call an SMTP server or SendGrid API.
type EmailExecutor struct{}

func (e *EmailExecutor) Execute(ctx context.Context, j *job.Job) error {

	select { // Simulate 2-second IO (network call to email provider).
	case <-time.After(2 * time.Second):
		return nil // success
	case <-ctx.Done():
		return ctx.Err() // graceful shutdown interrupted us
	}
}

// BackupExecutor simulates a backup job.
// In a real system this would stream data to S3 or another storage service.
type BackupExecutor struct{}

func (e *BackupExecutor) Execute(ctx context.Context, j *job.Job) error {
	select {
	case <-time.After(3 * time.Second):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// CleanupExecutor simulates a database or filesystem cleanup job.
type CleanupExecutor struct{}

func (e *CleanupExecutor) Execute(ctx context.Context, j *job.Job) error {
	select {
	case <-time.After(1 * time.Second):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// FailingExecutor always returns an error.
// Used ONLY during fault-tolerance testing to drive jobs through the retry
// and dead-letter paths without touching real executors.
//
// NEVER register this in production.
type FailingExecutor struct {
	// Message is the error message returned on every call.
	Message string
}

func (e *FailingExecutor) Execute(ctx context.Context, j *job.Job) error {
	return fmt.Errorf("deliberate failure (test executor): %s", e.Message)
}

// SleepExecutor sleeps for exactly 100ms and succeeds.
// Used exclusively for benchmarking and load testing throughput.
type SleepExecutor struct{}

func (e *SleepExecutor) Execute(ctx context.Context, j *job.Job) error {
	select {
	case <-time.After(100 * time.Millisecond):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
