// Package config loads all application configuration from environment variables.
// Every configurable value lives here. No other file should call os.Getenv.
// If configuration is invalid, Load() returns an error and main() calls log.Fatal.
// This is the "fail-fast" pattern: a misconfigured service must not start silently.
package config

import (
	"fmt"
	"os"
	"strconv"

	"github.com/google/uuid"
)

// Config holds all runtime configuration values for the application.
type Config struct {
	RedisAddr     string // Redis connection
	RedisPassword string
	RedisDB       int

	APIPort int // API server

	// Worker settings
	WorkerID          string // UUID v4 — auto-generated if not set via env
	WorkerConcurrency int    // How many goroutines the worker pool spawns

	// Job retry behaviour
	MaxRetriesDefault     int // Used when the job does not specify its own MaxRetries
	RetryBaseDelaySeconds int // Base delay for exponential backoff: delay = base * 2^attempt

	SchedulerIntervalMs int // Scheduler (moves scheduled jobs to queue when their time comes)

	WorkerHeartbeatIntervalSeconds int // Heartbeat (worker writes to Redis to prove it is alive)
	WorkerHeartbeatTTLSeconds      int // Redis TTL on the heartbeat key; must be > interval

	RecoveryIntervalSeconds int // Recovery (scans inprogress queues for stuck jobs)

	LogLevel string // Observability-->DEBUG | INFO | WARN | ERROR

	GracefulShutdownTimeoutSeconds int

	DashboardCORSOrigin string // Which frontend origin is allowed to call the /dashboard endpoints
}

// Load reads every value from the environment, falls back to sensible defaults, and validates the result.
// Call this once at program startup.
func Load() (*Config, error) {
	workerID := getEnv("WORKER_ID", "")
	if workerID == "" {
		// Generate a stable UUID for this process lifetime.
		// Kubernetes users can set WORKER_ID via the Pod name for easier debugging.
		workerID = uuid.New().String()
	}

	cfg := &Config{
		RedisAddr:     getEnv("REDIS_ADDR", "localhost:6379"),
		RedisPassword: getEnv("REDIS_PASSWORD", ""),
		RedisDB:       getEnvInt("REDIS_DB", 0),

		APIPort: getEnvInt("API_PORT", 8080),

		WorkerID:          workerID,
		WorkerConcurrency: getEnvInt("WORKER_CONCURRENCY", 5),

		MaxRetriesDefault:     getEnvInt("MAX_RETRIES_DEFAULT", 3),
		RetryBaseDelaySeconds: getEnvInt("RETRY_BASE_DELAY_SECONDS", 10),

		SchedulerIntervalMs: getEnvInt("SCHEDULER_INTERVAL_MS", 1000),

		WorkerHeartbeatIntervalSeconds: getEnvInt("WORKER_HEARTBEAT_INTERVAL_SECONDS", 10),
		WorkerHeartbeatTTLSeconds:      getEnvInt("WORKER_HEARTBEAT_TTL_SECONDS", 30),

		RecoveryIntervalSeconds: getEnvInt("RECOVERY_INTERVAL_SECONDS", 60),

		LogLevel: getEnv("LOG_LEVEL", "INFO"),

		GracefulShutdownTimeoutSeconds: getEnvInt("GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS", 30),

		DashboardCORSOrigin: getEnv("DASHBOARD_CORS_ORIGIN", "*"),
	}

	if err := validate(cfg); err != nil {
		return nil, err
	}

	return cfg, nil
}

// validate checks that all values are within acceptable bounds.
// Invalid configuration is returned as an error so main() can call log.Fatal.
func validate(cfg *Config) error {
	if cfg.WorkerConcurrency < 1 || cfg.WorkerConcurrency > 100 {
		return fmt.Errorf("WORKER_CONCURRENCY must be between 1 and 100, got %d", cfg.WorkerConcurrency)
	}
	if cfg.APIPort < 1024 || cfg.APIPort > 65535 {
		return fmt.Errorf("API_PORT must be between 1024 and 65535, got %d", cfg.APIPort)
	}
	if cfg.MaxRetriesDefault < 0 {
		return fmt.Errorf("MAX_RETRIES_DEFAULT must be >= 0, got %d", cfg.MaxRetriesDefault)
	}
	if cfg.WorkerHeartbeatTTLSeconds <= cfg.WorkerHeartbeatIntervalSeconds {
		return fmt.Errorf(
			"WORKER_HEARTBEAT_TTL_SECONDS (%d) must be greater than WORKER_HEARTBEAT_INTERVAL_SECONDS (%d)",
			cfg.WorkerHeartbeatTTLSeconds, cfg.WorkerHeartbeatIntervalSeconds,
		)
	}
	return nil
}

// helpers
func getEnv(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}

func getEnvInt(key string, defaultVal int) int {
	v := os.Getenv(key) //read from os environment variables,exist only if passed thorugh terminal or docker compose.
	if v == "" {        //if not passed through terminal or docker compose,use default value
		return defaultVal
	}
	n, err := strconv.Atoi(v) //terminal always passes strings,so convert them to int using strconv.Atoi
	if err != nil {
		return defaultVal // Invalid value → use default. The caller's validate() will catch real problems.
	}
	return n
}
