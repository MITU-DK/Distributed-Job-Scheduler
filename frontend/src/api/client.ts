// api/client.ts
// Base URL and shared types only.
// Each feature area has its own file:
//   api/metrics.ts  — observability reads (metrics + dead jobs)
//   api/jobs.ts     — job management (submit, schedule, lookup, retry)

// In Docker: VITE_API_BASE_URL is set to "/api" in docker-compose (built into the React bundle by Vite).
//   The browser sends requests to /api/... which NGINX proxies to the Go api container.

export const BASE_URL = import.meta.env.VITE_API_BASE_URL ?? 'http://localhost:8080'

// In local dev: falls back to http://localhost:8080 (direct to Go dev server, no proxy needed).