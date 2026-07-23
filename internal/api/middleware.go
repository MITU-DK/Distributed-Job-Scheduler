// Middleware--->requestID, logger , panicRecovery
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

type contextKey string

const keyRequestID contextKey = "request_id" //to prevent collisions with other context keys.

// add the request-id
func requestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := uuid.New().String()
		ctx := context.WithValue(r.Context(), keyRequestID, id)
		w.Header().Set("X-Request-ID", id)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// retrieves the request ID from the context.
func getRequestID(ctx context.Context) string {
	id, _ := ctx.Value(keyRequestID).(string)
	return id
}

// wraps http.ResponseWriter to capture the status code.
type responseWriter struct {
	http.ResponseWriter
	status int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.status = code
	rw.ResponseWriter.WriteHeader(code)
}

// logs: method, path, status, duration, request_id.
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

// catches any panic from a handler & returns 500 to client.
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

// add CORS headers only requests from the configured frontend origin, Methods, and Headers. Responds to OPTIONS preflight requests with 204 No Content.
func cors(cfg *config.Config) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

			w.Header().Set("Access-Control-Allow-Origin", cfg.DashboardCORSOrigin)

			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")

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
