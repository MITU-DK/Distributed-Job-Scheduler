// Handler struct holding dependencies (Redis, config) and exposing methods for each route, instead global variables.
// Endpoints:
//
//	POST /jobs             handleEnqueue     (immediate job)
//	POST /jobs/schedule   -> handleSchedule    (future-scheduled job)
//	GET  /jobs/{id}       -> handleGetJob      (job status + metadata)
//	GET  /jobs/{id}/history -> handleGetHistory (status transition log)
//	POST /jobs/{id}/retry -> handleRetry       (re-enqueue a FAILED/DEAD job)
package api

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/mitudk/distributed-job-scheduler/config"
	"github.com/mitudk/distributed-job-scheduler/internal/job"
	"github.com/mitudk/distributed-job-scheduler/internal/queue"
	"github.com/redis/go-redis/v9"
)

type Handler struct {
	rdb *redis.Client
	cfg *config.Config
}

type enqueueRequest struct { // JSON body.
	Name       string          `json:"name"`
	Priority   int             `json:"priority"`
	Payload    json.RawMessage `json:"payload"`
	MaxRetries *int            `json:"max_retries"` // pointer so we can distinguish 0 vs not-set
}

type scheduleRequest struct { //same to enqueuerequest with  an additional run_at field.
	enqueueRequest
	RunAt string `json:"run_at"` //"2026-06-20T10:00:00Z"
}

type jobResponse struct {
	JobID  string `json:"job_id"`
	Status string `json:"status"`
	RunAt  string `json:"run_at,omitempty"` // only set for scheduled jobs
}

// Creates job , pushes  to  correct priority queue.
// Request:  {"name": "send_email", "priority": 1, "payload": {"to": "a@b.com"}} & Response: 202 {"job_id": "...", "status": "QUEUED"}
func (h *Handler) handleEnqueue(w http.ResponseWriter, r *http.Request) {
	var req enqueueRequest
	if !decodeBody(w, r, &req) { //req now containt the json-parsed data.
		return
	}

	maxRetries := h.cfg.MaxRetriesDefault
	if req.MaxRetries != nil {
		maxRetries = *req.MaxRetries
	}

	j := &job.Job{
		Name:       req.Name,
		Priority:   req.Priority,
		Payload:    req.Payload,
		MaxRetries: maxRetries,
	}

	id, err := queue.Enqueue(r.Context(), h.rdb, j, h.cfg.MaxRetriesDefault)
	if err != nil {

		slog.Error("enqueue_failed", "error", err, "request_id", getRequestID(r.Context()))

		// If it starts with "enqueue validation:" it's a 400, otherwise 500.
		if isValidationError(err) {
			writeError(w, http.StatusBadRequest, "validation_error", err.Error(), "")
		} else {
			writeError(w, http.StatusInternalServerError, "internal_error", "failed to enqueue job", "")
		}
		return
	}

	writeJSON(w, http.StatusAccepted, jobResponse{JobID: id, Status: string(job.StatusQueued)})
}

// Creates scheduled job & pushed to the sorted set.
// Request:  {"name": "generate_report", "priority": 2, "payload": {}, "run_at": "2026-06-20T10:00:00Z"} & Response: 202 {"job_id": "...", "status": "SCHEDULED", "run_at": "2026-06-20T10:00:00Z"}
func (h *Handler) handleSchedule(w http.ResponseWriter, r *http.Request) {
	var req scheduleRequest
	if !decodeBody(w, r, &req) {
		return
	}

	if req.RunAt == "" {
		writeError(w, http.StatusBadRequest, "missing_field", "run_at is required for scheduled jobs", "run_at")
		return
	}

	// Parse timestamp.
	runAt, err := time.Parse(time.RFC3339, req.RunAt) //time.RFC3339  just a constant string: "2006-01-02T15:04:05Z07:00"
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_run_at", "run_at must be an RFC3339 timestamp (e.g. 2026-06-20T10:00:00Z)", "run_at")
		return
	}

	maxRetries := h.cfg.MaxRetriesDefault
	if req.MaxRetries != nil {
		maxRetries = *req.MaxRetries
	}

	j := &job.Job{
		Name:        req.Name,
		Priority:    req.Priority,
		Payload:     req.Payload,
		MaxRetries:  maxRetries,
		ScheduledAt: runAt.Unix(),
	}

	id, err := queue.Enqueue(r.Context(), h.rdb, j, h.cfg.MaxRetriesDefault)
	if err != nil {
		slog.Error("schedule_enqueue_failed", "error", err, "request_id", getRequestID(r.Context()))
		if isValidationError(err) {
			writeError(w, http.StatusBadRequest, "validation_error", err.Error(), "")
		} else {
			writeError(w, http.StatusInternalServerError, "internal_error", "failed to schedule job", "")
		}
		return
	}

	writeJSON(w, http.StatusAccepted, jobResponse{
		JobID:  id,
		Status: string(job.StatusScheduled),
		RunAt:  req.RunAt,
	})
}

// Returns full job metadata as JSON.
// Response: 200 {full job struct} , 404 if job does not exist
func (h *Handler) handleGetJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeError(w, http.StatusBadRequest, "missing_id", "job id is required", "id")
		return
	}

	fields, err := h.rdb.HGetAll(r.Context(), queue.MetaKey(id)).Result()
	if err != nil {
		slog.Error("get_job_hgetall_failed", "job_id", id, "error", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "failed to fetch job", "")
		return
	}
	if len(fields) == 0 {
		writeError(w, http.StatusNotFound, "not_found", "job not found", "id")
		return
	}

	j, err := job.FromHash(fields)
	if err != nil {
		slog.Error("get_job_parse_failed", "job_id", id, "error", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "failed to parse job", "")
		return
	}

	writeJSON(w, http.StatusOK, j)
}

// Returns the list of status-change events for a job (chronological order).
// Response: 200 [{"from":"QUEUED","to":"IN_PROGRESS","at":1718780000,"worker_id":"..."}],  404 if job does not exist
func (h *Handler) handleGetHistory(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeError(w, http.StatusBadRequest, "missing_id", "job id is required", "id")
		return
	}
	// First check the job exists.
	exists, err := h.rdb.Exists(r.Context(), queue.MetaKey(id)).Result()
	if err != nil {
		slog.Error("get_history_exists_failed", "job_id", id, "error", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "failed to check job", "")
		return
	}
	if exists == 0 {
		writeError(w, http.StatusNotFound, "not_found", "job not found", "id")
		return
	}

	//returns all elements in  list (oldest to newest = chronological). QUEUED-->IN_PROGRESS-->FAILED--->COMPLETED--->DEAD--->RETRIED-->RETRY_FAILED
	events, err := h.rdb.LRange(r.Context(), queue.HistoryKey(id), 0, -1).Result()
	if err != nil {
		slog.Error("get_history_lrange_failed", "job_id", id, "error", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "failed to fetch history", "")
		return
	}

	// convert elements(raw JSON string) ->JSON array of objects, not strings.
	parsed := make([]json.RawMessage, len(events))
	for i, e := range events {
		parsed[i] = json.RawMessage(e)
	}

	writeJSON(w, http.StatusOK, parsed)
}

// Re-enqueues FAILED or DEAD job & resetting its retry count.
// Response: 202 {"job_id": "...", "status": "QUEUED"}, 404 if job does not exist, 409 Conflict if job is not in FAILED or DEAD state
func (h *Handler) handleRetry(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeError(w, http.StatusBadRequest, "missing_id", "job id is required", "id")
		return
	}

	fields, err := h.rdb.HGetAll(r.Context(), queue.MetaKey(id)).Result()
	if err != nil {
		slog.Error("retry_hgetall_failed", "job_id", id, "error", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "failed to fetch job", "")
		return
	}
	if len(fields) == 0 {
		writeError(w, http.StatusNotFound, "not_found", "job not found", "id")
		return
	}

	j, err := job.FromHash(fields)
	if err != nil {
		slog.Error("retry_parse_failed", "job_id", id, "error", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "failed to parse job", "")
		return
	}

	// Only FAILED or DEAD jobs can be manually retried.
	if j.Status != job.StatusFailed && j.Status != job.StatusDead {
		writeError(w, http.StatusConflict, "invalid_state", "only FAILED or DEAD jobs can be retried (current: "+string(j.Status)+")", "")
		return
	}

	// Reset retry count and re-enqueue as an immediate job.
	// DON'T call queue.Enqueue (which assigns a new ID). Instead we use pipeline to update the existing job in place.
	pipe := h.rdb.TxPipeline()

	pipe.HSet(r.Context(), queue.MetaKey(id), "status", string(job.StatusQueued), "retry_count", 0, "last_error", "")
	pipe.RPush(r.Context(), queue.QueueKey(j.Priority), id)

	if _, err := pipe.Exec(r.Context()); err != nil {
		slog.Error("retry_pipeline_failed", "job_id", id, "error", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "failed to retry job", "")
		return
	}

	slog.Info("job_manually_retried", "job_id", id, "previous_status", j.Status)
	writeJSON(w, http.StatusAccepted, jobResponse{JobID: id, Status: string(job.StatusQueued)})
}
