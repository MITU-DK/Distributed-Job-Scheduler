// api/jobs.ts
// Job management API calls:
//   POST /jobs              — submit an immediate job
//   POST /jobs/schedule     — submit a future-scheduled job
//   GET  /jobs/{id}         — fetch full job metadata
//   GET  /jobs/{id}/history — fetch status-change event log
//   POST /jobs/{id}/retry   — manually re-enqueue a FAILED or DEAD job

import { BASE_URL } from './client'

// Types

export interface JobResponse {
  job_id: string
  status: string
  run_at?: string
}

export interface JobDetail {
  id: string
  name: string
  priority: number
  status: string
  retry_count: number
  max_retries: number
  last_error: string
  enqueued_at: number
  started_at?: number
  completed_at?: number
  payload: unknown
}

export interface HistoryEvent {
  from: string
  to: string
  at: number
  worker_id: string
  error?: string
}

// API calls 

// POST /jobs — enqueue an immediate job into the priority queue.
export async function submitJob(body: {
  name: string, priority: number, payload: unknown, max_retries?: number
}): Promise<JobResponse> {
  const res = await fetch(`${BASE_URL}/jobs`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body), })
  const data = await res.json()
  if (!res.ok) throw new Error(data?.message ?? `POST /jobs returned ${res.status}`)
  return data
}

// POST /jobs/schedule — place a job into the scheduled sorted set.
// run_at must be an RFC3339 string e.g. "2026-06-20T10:00:00Z"
export async function scheduleJob(body: { name: string, priority: number, payload: unknown, run_at: string, max_retries?: number }): Promise<JobResponse> {
  const res = await fetch(`${BASE_URL}/jobs/schedule`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body),
  })
  const data = await res.json()
  if (!res.ok) throw new Error(data?.message ?? `POST /jobs/schedule returned ${res.status}`)
  return data
}

// GET /jobs/{id} — fetch full job metadata from the Redis Hash.
export async function getJob(id: string): Promise<JobDetail> {
  const res = await fetch(`${BASE_URL}/jobs/${encodeURIComponent(id)}`) //encodeURIComponent - replace special characters
  const data = await res.json()
  if (!res.ok) throw new Error(data?.message ?? `GET /jobs/${id} returned ${res.status}`)
  return data
}

// GET /jobs/{id}/history — fetch the chronological list of status-change events.
export async function getJobHistory(id: string): Promise<HistoryEvent[]> {
  const res = await fetch(`${BASE_URL}/jobs/${encodeURIComponent(id)}/history`)
  const data = await res.json()
  if (!res.ok) throw new Error(data?.message ?? `GET /jobs/${id}/history returned ${res.status}`)
  return data
}

// POST /jobs/{id}/retry — manually re-enqueue a FAILED or DEAD job.
// The backend resets retry_count to 0 and places the job back in its priority queue.
export async function retryJob(id: string): Promise<JobResponse> {
  const res = await fetch(`${BASE_URL}/jobs/${encodeURIComponent(id)}/retry`, { method: 'POST', })
  const data = await res.json()
  if (!res.ok) throw new Error(data?.message ?? `POST /jobs/${id}/retry returned ${res.status}`)
  return data
}
