// Server owns the http.Server, the router (ServeMux), and graceful shutdown.
// It knows nothing about business logic living in handlers.go.
// Dependency injection pattern:
//
//	All handlers receive their dependencies (config,Redis client) through the
//	Handler struct, not through package-level globals. This makes the code testable and avoids hidden coupling.
package api

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/mitudk/distributed-job-scheduler/config"
	"github.com/redis/go-redis/v9"
)

// Server wraps the stdlib http.Server and owns the router + shutdown logic.
type Server struct {
	httpServer *http.Server
	cfg        *config.Config
}

// New builds the Server, wires all routes, and wraps them in middleware. & Call Serve() to actually start listening.
func New(cfg *config.Config, rdb *redis.Client) *Server {
	h := &Handler{rdb: rdb, cfg: cfg}
	m := &Metrics{rdb: rdb}

	mux := http.NewServeMux()

	//Job routes
	mux.HandleFunc("POST /jobs", h.handleEnqueue)
	mux.HandleFunc("POST /jobs/schedule", h.handleSchedule)
	mux.HandleFunc("GET /jobs/{id}", h.handleGetJob)
	mux.HandleFunc("GET /jobs/{id}/history", h.handleGetHistory)
	mux.HandleFunc("POST /jobs/{id}/retry", h.handleRetry)

	//Observability routes
	mux.HandleFunc("GET /health", m.handleHealth)
	mux.HandleFunc("GET /metrics", m.handleMetrics)

	// Dashboard routes
	mux.HandleFunc("GET /dashboard/jobs/dead", h.handleDeadJobs)

	// Wrap the entire mux in the middleware chain.
	// Order (outermost to innermost): cors → requestID → logger → panicRecovery → mux
	// Why this order?
	//   cors must be outermost to catch OPTIONS requests early.
	//   requestID injects the ID first so every subsequent middleware can log it.
	//   logger records timing, so it must wrap the actual handler.
	//   panicRecovery is innermost — it catches panics from handlers.
	chain := cors(cfg)(requestID(logger(panicRecovery(mux))))

	return &Server{
		httpServer: &http.Server{
			Addr:         fmt.Sprintf(":%d", cfg.APIPort),
			Handler:      chain,
			ReadTimeout:  10 * time.Second,
			WriteTimeout: 30 * time.Second,
			IdleTimeout:  60 * time.Second,
		},
		cfg: cfg,
	}
}

// Serve starts the HTTP server and blocks until ctx is cancelled.
// On cancellation, it initiates a graceful shutdown — waits up to
// GracefulShutdownTimeoutSeconds for in-flight requests to complete.
func (s *Server) Serve(ctx context.Context) error {
	// Run the server in a goroutine so we can listen for ctx cancellation.
	errCh := make(chan error, 1)
	go func() {
		slog.Info("http_server_listening", "addr", s.httpServer.Addr)
		if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
		close(errCh) // closes the channel when the server stops
	}()

	// Wait for either a server error or the parent context being cancelled.
	select {
	case err := <-errCh:
		return fmt.Errorf("http server error: %w", err)
	case <-ctx.Done():
		slog.Info("http_server_shutting_down")
	}

	// Graceful shutdown: wait up to the configured timeout for in-flight requests.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Duration(s.cfg.GracefulShutdownTimeoutSeconds)*time.Second)
	defer cancel()

	if err := s.httpServer.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("http server shutdown: %w", err)
	}

	slog.Info("http_server_stopped")
	return nil
}
