// api/metrics.ts
// Read-only observability fetchers:
//   GET /metrics              — counters + queue depths + active workers
//   GET /dashboard/jobs/dead  — dead-letter queue listing

import { BASE_URL } from './client'

export interface MetricsData {
  queue_depth_p1: number
  queue_depth_p2: number
  queue_depth_p3: number
  scheduled_count: number
  retry_count: number
  dead_count: number
  total_processed: number
  total_failed: number
  total_dead: number
  active_workers: number
}

export interface DeadJob {
  id: string
  name: string
  priority: string
  status: string
  retry_count: string
  max_retries: string
  last_error: string
  enqueued_at: string
}

export interface DeadJobsData {
  total: number
  jobs: DeadJob[]
}

export async function fetchMetrics(): Promise<MetricsData> {
  const res = await fetch(`${BASE_URL}/metrics`)
  if (!res.ok) throw new Error(`/metrics returned ${res.status}`)
  return res.json()
}

export async function fetchDeadJobs(limit = 50, offset = 0): Promise<DeadJobsData> {
  const res = await fetch(`${BASE_URL}/dashboard/jobs/dead?limit=${limit}&offset=${offset}`)
  if (!res.ok) throw new Error(`/dashboard/jobs/dead returned ${res.status}`)
  return res.json()
}
