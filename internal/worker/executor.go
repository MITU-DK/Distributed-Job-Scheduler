// Executor interface -> job dispatch system.
package worker

import (
	"context"
	"fmt"
	"time"

	"github.com/mitudk/distributed-job-scheduler/internal/job"
)

type Executor interface {
	Execute(ctx context.Context, j *job.Job) error
}
type Registry struct {
	executors map[string]Executor
}

// creates an empty registry.
func NewRegistry() *Registry {
	return &Registry{executors: make(map[string]Executor)}
}

// Adds an executor for a given job name. Call this at startup before the pool starts dequeuing.
func (r *Registry) Register(name string, e Executor) {
	r.executors[name] = e
}

// Returns the Executor for the given job name, or false if not found.
func (r *Registry) Lookup(name string) (Executor, bool) {
	e, ok := r.executors[name]
	return e, ok
}

//Concrete Executors (dummy implementations for development & testing)------->

// Simulates sending an email.(production----> An SMTP server or SendGrid API.)
type EmailExecutor struct{}

func (e *EmailExecutor) Execute(ctx context.Context, j *job.Job) error {

	select { // Simulate 2-second IO (network call to email provider).
	case <-time.After(2 * time.Second):
		return nil // success
	case <-ctx.Done():
		return ctx.Err() // graceful shutdown interrupted us
	}
}

// Simulates a backup job.(productino--->stream data to S3 or another storage service.)
type BackupExecutor struct{}

func (e *BackupExecutor) Execute(ctx context.Context, j *job.Job) error {
	select {
	case <-time.After(3 * time.Second):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Simulates a database or filesystem cleanup job.(production--->delete old log files, purge temp data, etc.)
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
// Used ONLY during fault-tolerance testing to drive jobs through the retry and dead-letter paths without touching real executors.
// NEVER register this in production.
type FailingExecutor struct {
	Message string // error message returned on every call.
}

func (e *FailingExecutor) Execute(ctx context.Context, j *job.Job) error {
	return fmt.Errorf("deliberate failure (test executor): %s", e.Message)
}

// Sleeps for exactly 100ms and succeeds.Used exclusively for benchmarking and load testing throughput.
type SleepExecutor struct{}

func (e *SleepExecutor) Execute(ctx context.Context, j *job.Job) error {
	select {
	case <-time.After(100 * time.Millisecond):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
