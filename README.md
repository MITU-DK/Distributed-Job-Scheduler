# Chronos — Distributed Job Scheduler

> A production-realistic, distributed job scheduling system built in **Go + Redis**.
> Designed to handle priority queues, scheduled jobs, automatic retries with exponential backoff,
> crash recovery, and a live monitoring dashboard — all without a single message broker.

---

## Architecture

```
┌─────────────────┐        ┌─────────────────────────────────────────────────────┐
│   React         │        │                        Redis                         │
│   Dashboard     │        │                                                     │
│  (Vite + TS)   │        │  jobs:queue:p1  ──► List  (Priority 1 — Highest)   │
│                 │        │  jobs:queue:p2  ──► List  (Priority 2 — Medium)    │
│  ┌───────────┐  │        │  jobs:queue:p3  ──► List  (Priority 3 — Low)       │
│  │ Submit    │  │  HTTP  │  jobs:scheduled ──► Sorted Set (score = run_at)     │
│  │ Schedule  │◄─┼───────►│  jobs:retry     ──► Sorted Set (score = retry_at)  │
│  │ Inspect   │  │        │  jobs:inprogress:{worker_id} ──► List (safe deq)   │
│  │ Retry     │  │        │  jobs:dead      ──► List  (exhausted retries)       │
│  └───────────┘  │        │  jobs:meta:{id} ──► Hash  (full job metadata)      │
└─────────────────┘        │  workers:heartbeat:{id} ──► String (TTL=30s)       │
                            │  metrics:jobs:* ──► String (atomic counters)       │
┌─────────────────┐        └──────────────────────────┬──────────────────────────┘
│   API Server    │                                   │
│  (Go net/http)  │◄──── POST /jobs ─────────────────┘
│                 │◄──── POST /jobs/schedule           ▲
│  POST /jobs     │      GET  /jobs/{id}               │
│  GET  /metrics  │      GET  /jobs/{id}/history       │
│  GET  /health   │      POST /jobs/{id}/retry    ┌────┴──────────┐
└─────────────────┘      GET  /metrics            │  Worker Pool  │
                          GET  /dashboard/…        │  (N goroutines│
                                                   │   per worker) │
                                                   │               │
                                                   │  ┌─────────┐ │
                                                   │  │Scheduler│ │  Promotes
                                                   │  │  Loop   │─┼─► scheduled
                                                   │  └─────────┘ │   & retry jobs
                                                   │  ┌─────────┐ │
                                                   │  │Recovery │ │  Re-enqueues
                                                   │  │Scanner  │─┼─► orphaned jobs
                                                   │  └─────────┘ │
                                                   └───────────────┘
```

---

## Tech Stack

| Technology | Version | Why |
|---|---|---|
| **Go** | 1.24 | Goroutines make concurrent workers trivial; compiled binary = tiny Docker image |
| **Redis** | 7.x | Atomic list/sorted-set operations make at-least-once delivery possible without Kafka |
| **React + Vite** | React 19, Vite 8 | Fast, statically deployable dashboard |
| **Tailwind CSS** | v4 | Utility-first → no CSS files to maintain |
| **Docker Compose** | v2 | One command to spin up the entire system on any machine |

---

## Key Design Decisions

### 1. `RPOPLPUSH` over `BLPOP` — At-Least-Once Delivery
A plain `BLPOP` pops the job ID off the queue and hands it to the worker. If the worker crashes between receiving the job and completing it, the job is **silently lost forever**.

We use Redis's atomic `RPOPLPUSH (now LMOVE)` instead. The job ID is atomically *moved* from `jobs:queue:p1` into `jobs:inprogress:{worker_id}`. It stays there until the worker explicitly acknowledges success. If the worker crashes, the Recovery Scanner finds the orphaned job still sitting in the `inprogress` list and re-enqueues it. The job is never lost.

### 2. Exponential Backoff with Jitter — Thundering Herd Prevention
When a job fails and has retries remaining, the delay before the next attempt is calculated as:

```
delay = base_delay_seconds × (2 ^ retry_count) + random_jitter(0–5s)
```

Without jitter: 1,000 jobs that all fail at the same moment will all retry at the exact same moment, creating a thundering herd that can overwhelm Redis. Random jitter spreads the load over time.

### 3. Heartbeat-Based Crash Recovery
Every worker writes `SET workers:heartbeat:{worker_id} {timestamp} EX 30` every 10 seconds. If a worker process is killed (SIGKILL, OOM), the key expires in at most 30 seconds. The Recovery Scanner goroutine, which runs every 60 seconds, scans for missing heartbeat keys. For any dead worker, it moves all jobs from that worker's `inprogress` list back to the appropriate priority queue.

### 4. Strict Priority Queue + Documented Starvation Trade-off
We always check `p1 → p2 → p3` (highest first). This is the simplest correct implementation. The known trade-off is starvation: if P1 is always full, P3 jobs never run. In production, this would be solved with weighted random selection or job aging. The trade-off is explicitly documented rather than hidden.

### 5. Atomic Scheduler Promotion
The scheduler reads ready jobs from the sorted set, then uses a Redis Lua script to atomically claim each job with `ZREM` before pushing it into the priority queue. If multiple scheduler instances see the same job, only the instance whose `ZREM` succeeds promotes it, preventing duplicate queue entries.

---

## Performance

Load test methodology: 1,000 `"sleep"` jobs (100ms fixed duration) submitted to the queue all at once. Time measured from first job enqueued to last job's `total_processed` counter increment. Run on a single Ubuntu machine (Go + Redis on localhost).

| `WORKER_CONCURRENCY` | Total Time | Throughput | vs. Baseline |
|:---:|:---:|:---:|:---:|
| 1 | 103.74 s | **9.64 JPS** | 1× (baseline) |
| 50 | 2.31 s | **432.34 JPS** | **44.9× faster** |

> **Improvement ratio:** `(103.74 − 2.31) / 103.74 = 97.8%` reduction in queue drain time.

Throughput scales near-linearly because the `SleepExecutor` is pure IO-bound (no CPU contention). Goroutines are cheap — the bottleneck with 50 workers shifts to the Redis round-trip latency per `LMOVE` (~0.2ms on localhost), not CPU.

---

## How to Run Locally

### Prerequisites
- Go 1.24+
- Docker and Docker Compose
- Node.js 20+ (for the dashboard)

### Option A — Docker Compose (Recommended)

```bash
# Clone and start everything in one command
git clone https://github.com/MITU-DK/Distributed-Job-Scheduler
cd Distributed-Job-Scheduler
docker compose up --build
```

This spins up: Redis, the API server, and 3 worker replicas.

### Option B — Manual (Development)

```bash
# 1. Start Redis
docker run --name redis -p 6379:6379 -d redis redis-server --appendonly yes

# 2. In terminal 1 — Start API server
go run cmd/api/main.go

# 3. In terminal 2 — Start Worker(s)
WORKER_CONCURRENCY=10 go run cmd/worker/main.go

# 4. In terminal 3 — Start the Dashboard
cd frontend && npm install && npm run dev
# Open http://localhost:5173
```

---

## API Reference

All endpoints base URL: `http://localhost:8080`

### `POST /jobs`
Submit a job for immediate processing.

**Request:**
```json
{
  "name": "email",
  "priority": 1,
  "payload": { "to": "user@example.com" },
  "max_retries": 3
}
```

**Response `202 Accepted`:**
```json
{
  "job_id": "82bd9f9b-2adb-44b9-8d94-bee00f3c2799",
  "status": "QUEUED"
}
```

---

### `POST /jobs/schedule`
Submit a job to run at a specific future time.

**Request:**
```json
{
  "name": "cleanup",
  "priority": 2,
  "payload": { "folder": "/tmp" },
  "run_at": "2026-06-21T10:00:00Z"
}
```

**Response `202 Accepted`:**
```json
{
  "job_id": "...",
  "status": "SCHEDULED",
  "run_at": "2026-06-21T10:00:00Z"
}
```

---

### `GET /jobs/{id}`
Fetch full metadata for a specific job.

```json
{
  "id": "82bd9f9b-...",
  "name": "email",
  "status": "COMPLETED",
  "priority": 1,
  "retry_count": 0,
  "max_retries": 3,
  "enqueued_at": 1750400000
}
```

---

### `GET /jobs/{id}/history`
Returns a chronological list of status transitions for a job.

```json
[
  { "from": "QUEUED", "to": "IN_PROGRESS", "at": 1750400001, "worker_id": "abc-123" },
  { "from": "IN_PROGRESS", "to": "COMPLETED", "at": 1750400003, "worker_id": "abc-123" }
]
```

---

### `POST /jobs/{id}/retry`
Manually re-enqueue a `FAILED` or `DEAD` job. Resets retry count to 0.

---

### `GET /metrics`
Returns a live snapshot of system health.

```json
{
  "active_workers": 1,
  "queue_depth_p1": 0,
  "queue_depth_p2": 0,
  "queue_depth_p3": 0,
  "scheduled_count": 2,
  "retry_count": 0,
  "dead_count": 0,
  "total_processed": 1000,
  "total_failed": 0,
  "total_dead": 0
}
```

---

### `GET /health`
Simple liveness probe for load balancers and Kubernetes.
```json
{ "status": "ok", "redis": "ok" }
```

---

### `GET /dashboard/jobs/dead`
Returns paginated list of dead-letter jobs with full metadata (name, error, timestamps).

---

## Configuration (Environment Variables)

| Variable | Default | Description |
|---|---|---|
| `REDIS_ADDR` | `localhost:6379` | Redis connection address |
| `REDIS_PASSWORD` | `""` | Redis password (blank = no auth) |
| `API_PORT` | `8080` | HTTP server port |
| `WORKER_ID` | auto (UUID v4) | Unique name for this worker process |
| `WORKER_CONCURRENCY` | `5` | Number of goroutines in the worker pool |
| `MAX_RETRIES_DEFAULT` | `3` | Default retry limit when job doesn't specify |
| `RETRY_BASE_DELAY_SECONDS` | `10` | Base for exponential backoff: `base × 2^attempt` |
| `SCHEDULER_INTERVAL_MS` | `1000` | How often the scheduler loop fires (ms) |
| `WORKER_HEARTBEAT_INTERVAL_SECONDS` | `10` | How often a worker writes its heartbeat |
| `WORKER_HEARTBEAT_TTL_SECONDS` | `30` | How long a heartbeat key lives in Redis |
| `RECOVERY_INTERVAL_SECONDS` | `60` | How often the recovery scanner runs |
| `GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS` | `30` | How long to wait for in-flight jobs on SIGTERM |
| `DASHBOARD_CORS_ORIGIN` | `*` | Allowed origin for dashboard API calls |
| `LOG_LEVEL` | `INFO` | Log verbosity (DEBUG, INFO, WARN, ERROR) |
| `ENABLE_FAILING_EXECUTOR` | `false` | Register the fault-injection executor (testing only) |

---

## Registered Job Types

| Job Name | Executor | Simulated Duration |
|---|---|---|
| `email` | `EmailExecutor` | 2 seconds (SMTP call) |
| `backup` | `BackupExecutor` | 3 seconds (S3 upload) |
| `cleanup` | `CleanupExecutor` | 1 second (filesystem scan) |
| `sleep` | `SleepExecutor` | 100ms (used for load testing) |

---

## Known Limitations

1. **Strict priority can starve low-priority jobs.** If P3 is always full, P1 jobs never run. Fix: implement weighted random selection or timestamp-based aging.

2. **No exactly-once delivery.** The system guarantees at-least-once. If a worker crashes immediately after completing a job but before removing it from `inprogress`, the job will be run a second time. Fix: idempotency keys stored in the `jobs:meta:{id}` hash.

3. **Single Redis instance.** All state lives in one Redis. Fix: Redis Cluster or a Redis Sentinel setup for high availability.

---

## What I Would Do Next

- [ ] Prometheus metrics exporter (`/metrics` in Prometheus text format)
- [ ] Job dependency graph (Job B runs only after Job A completes)
- [ ] Redis Cluster support for horizontal Redis scaling
- [ ] Job timeout — kill a job that runs longer than N seconds
- [ ] Rate limiting on the `POST /jobs` endpoint
