// requestID   — injects a unique request ID into context + response header.
// logger      — logs method, path, status, and duration for every request.
// panicRecovery — catches handler panics and returns 500 instead of crashing.
package api

import (
	"context"
	"log/slog"
	"net/http"
	"runtime/debug"
	"time"

	"github.com/google/uuid"
	"github.com/mitudk/distributed-job-scheduler/config"
)

// contextKey is an unexported type for context keys in this package.
// Using a custom type prevents collisions with context keys from other packages.
type contextKey string

const keyRequestID contextKey = "request_id"

// Why per-request IDs?
// When a request fails, the client can quote the X-Request-ID and you can grep
// your logs for that exact string across 100 log lines to trace the full lifecycle.
// Without request IDs, debugging distributed systems is guesswork.
func requestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := uuid.New().String()
		ctx := context.WithValue(r.Context(), keyRequestID, id)
		w.Header().Set("X-Request-ID", id)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// getRequestID retrieves the request ID from the context.
// Returns an empty string if not set (should not happen in production with requestID middleware).
func getRequestID(ctx context.Context) string {
	id, _ := ctx.Value(keyRequestID).(string)
	return id
}

// responseWriter wraps http.ResponseWriter to capture the status code.
// The stdlib ResponseWriter does not expose the status code after it is written,
// so we intercept WriteHeader to record it for the logger.
type responseWriter struct {
	http.ResponseWriter
	status int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.status = code
	rw.ResponseWriter.WriteHeader(code)
}

// logger logs one structured line per request: method, path, status, duration, request_id.
// This is the access log. Every request is logged — no exceptions.
//
// Why log at INFO and not DEBUG?
// In production systems, access logs are always INFO-level. They are the primary
// audit trail for what the API is doing. Demoting them to DEBUG means you lose
// visibility the moment someone sets the log level to INFO.
func logger(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rw := &responseWriter{ResponseWriter: w, status: http.StatusOK} // status code is set to 200 by default

		next.ServeHTTP(rw, r)

		slog.Info("http_request",
			"method", r.Method,
			"path", r.URL.Path,
			"status", rw.status,
			"duration_ms", time.Since(start).Milliseconds(),
			"request_id", getRequestID(r.Context()),
		)
	})
}

// panicRecovery catches any panic from a handler and returns 500 to the client.
// Without this, a single nil-pointer dereference in a handler brings down the
// entire server process — every client gets connection refused.
//
// Why log the stack trace?
// The panic message alone often does not tell you which line caused it.
// debug.Stack() prints the full goroutine stack, which immediately points you
// to the exact line that panicked. Always log it.
func panicRecovery(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				slog.Error("http_handler_panic",
					"panic", rec,
					"stack", string(debug.Stack()),
					"request_id", getRequestID(r.Context()),
				)

				http.Error(w, `{"error":"internal_server_error"}`, http.StatusInternalServerError)
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// cors wraps a handler and adds the necessary headers to allow cross-origin requests
// from the configured frontend origin. We apply this to the entire API so the dashboard
// can call /jobs, /stats, and /jobs/dead.
func cors(cfg *config.Config) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Allow requests from the configured origin.
			w.Header().Set("Access-Control-Allow-Origin", cfg.DashboardCORSOrigin)

			// Allow these HTTP methods from the browser.
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")

			// Allow these headers to be sent by the browser in requests.
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-Request-ID")

			// Browsers send an OPTIONS "preflight" request before every POST/PUT.
			// We respond immediately with 204 No Content to complete the handshake.
			if r.Method == http.MethodOptions {
				w.WriteHeader(http.StatusNoContent)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}
