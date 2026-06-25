// Handles two APIs in one component:
//   GET /jobs/{id}         — full job metadata
//   GET /jobs/{id}/history — chronological status events
//   POST /jobs/{id}/retry  — manually re-enqueue a FAILED or DEAD job

import { useState } from 'react'
import { getJob, getJobHistory, retryJob } from '../api/jobs'
import type { JobDetail, HistoryEvent } from '../api/jobs'

const STATUS_COLORS: Record<string, string> = {
  QUEUED: 'bg-blue-50 text-blue-600',
  IN_PROGRESS: 'bg-amber-50 text-amber-600',
  COMPLETED: 'bg-emerald-50 text-emerald-600',
  FAILED: 'bg-orange-50 text-orange-600',
  DEAD: 'bg-red-50 text-red-600',
  SCHEDULED: 'bg-violet-50 text-violet-600',
}

function StatusBadge({ status }: { status: string }) {
  const cls = STATUS_COLORS[status] ?? 'bg-slate-100 text-slate-500'
  return (
    <span className={`text-xs font-semibold px-2 py-0.5 rounded-full ${cls}`}>{status}</span>
  )
}

function JobDetailCard({ job, onRetry, retrying }: { job: JobDetail, onRetry: () => void, retrying: boolean }) {

  const canRetry = job.status === 'FAILED' || job.status === 'DEAD'
  return (
    <div className="mt-3 p-4 bg-slate-50 rounded-lg border border-gray-200 text-sm space-y-2">
      <div className="flex items-center justify-between">
        <span className="font-semibold text-slate-700">{job.name}</span>
        <StatusBadge status={job.status} />
      </div>
      <div className="grid grid-cols-2 gap-x-4 gap-y-1 text-slate-500 text-xs">
        <span>ID: <span className="font-mono text-slate-700">{job.id}</span></span>
        <span>Priority: P{job.priority}</span>
        <span>Retries: {job.retry_count} / {job.max_retries}</span>
        <span>Enqueued: {new Date(job.enqueued_at * 1000).toLocaleString()}</span>
        {job.started_at && <span>Started: {new Date(job.started_at * 1000).toLocaleString()}</span>}
        {job.completed_at && <span>Completed: {new Date(job.completed_at * 1000).toLocaleString()}</span>}
      </div>
      {job.last_error && (
        <p className="text-xs text-red-500 break-all">Last error: {job.last_error}</p>
      )}
      {canRetry && (
        <button
          onClick={onRetry}
          disabled={retrying}
          className="mt-2 w-full py-1.5 px-3 bg-amber-600 text-white text-xs font-medium rounded-lg hover:bg-amber-500 disabled:opacity-50 transition-colors"
        >
          {retrying ? 'Retrying…' : '↩ Retry this Job'}
        </button>
      )}
    </div>
  )
}

function HistoryTimeline({ events }: { events: HistoryEvent[] }) {
  if (events.length === 0) return <p className="mt-2 text-xs text-slate-400">No history events yet.</p>

  return (
    <ol className="mt-3 relative border-l border-gray-200 ml-2 space-y-3">
      {events.map((e, i) => (
        <li key={i} className="ml-4">
          <span className="absolute -left-1.5 w-3 h-3 rounded-full bg-slate-300 border-2 border-white" />
          <p className="text-xs text-slate-400">{new Date(e.at * 1000).toLocaleString()}</p>
          <p className="text-sm text-slate-700 font-medium">
            <StatusBadge status={e.from} /> → <StatusBadge status={e.to} />
          </p>
          {e.worker_id && <p className="text-xs text-slate-400 font-mono">{e.worker_id}</p>}
          {e.error && <p className="text-xs text-red-400 break-all">{e.error}</p>}
        </li>
      ))}
    </ol>
  )
}

export default function JobLookup() {
  const [jobId, setJobId] = useState('')
  const [job, setJob] = useState<JobDetail | null>(null)
  const [history, setHistory] = useState<HistoryEvent[] | null>(null)
  const [loading, setLoading] = useState(false)
  const [retrying, setRetrying] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [retryOk, setRetryOk] = useState<string | null>(null)

  async function handleLookup(e: React.FormEvent) {
    e.preventDefault()
    if (!jobId.trim()) return
    setLoading(true)
    setJob(null)
    setHistory(null)
    setError(null)
    setRetryOk(null)

    try {
      // Fetch metadata and history in parallel
      const [j, h] = await Promise.all([getJob(jobId.trim()), getJobHistory(jobId.trim())])
      setJob(j)
      setHistory(h)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Unknown error')
    } finally {
      setLoading(false)
    }
  }

  async function handleRetry() {
    if (!job) return
    setRetrying(true)
    setRetryOk(null)
    setError(null)
    try {
      const res = await retryJob(job.id)
      setRetryOk(`Job re-queued! New status: ${res.status}`)
      // Refresh the job state so the status updates
      const updated = await getJob(job.id)
      setJob(updated)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Unknown error')
    } finally {
      setRetrying(false)
    }
  }

  return (
    <div className="space-y-4">
      <form onSubmit={handleLookup} className="flex gap-2">
        <input
          id="lookup-id"
          type="text"
          required
          value={jobId}
          onChange={e => setJobId(e.target.value)}
          placeholder="Paste a Job ID…"
          className="flex-1 text-sm font-mono border border-gray-200 rounded-lg px-3 py-2 text-slate-700 focus:outline-none focus:ring-2 focus:ring-slate-300"
        />
        <button
          type="submit"
          disabled={loading}
          className="py-2 px-4 bg-slate-800 text-white text-sm font-medium rounded-lg hover:bg-slate-700 disabled:opacity-50 transition-colors"
        >
          {loading ? '…' : 'Look Up'}
        </button>
      </form>

      {error && (
        <div className="flex items-start justify-between gap-2 p-3 bg-red-50 border border-red-200 rounded-lg text-xs text-red-600">
          <span>✗ {error}</span>
          <button onClick={() => setError(null)} className="text-red-400 hover:text-red-700 font-bold text-sm leading-none shrink-0" aria-label="Dismiss">×</button>
        </div>
      )}
      {retryOk && (
        <div className="flex items-start justify-between gap-2 p-3 bg-emerald-50 border border-emerald-200 rounded-lg text-xs text-emerald-700">
          <span>✓ {retryOk}</span>
          <button onClick={() => setRetryOk(null)} className="text-emerald-400 hover:text-emerald-700 font-bold text-sm leading-none shrink-0" aria-label="Dismiss">×</button>
        </div>
      )}

      {job && <JobDetailCard job={job} onRetry={handleRetry} retrying={retrying} />}

      {history !== null && (
        <div>
          <p className="text-xs font-semibold text-slate-400 uppercase tracking-wider mt-4 mb-1">History</p>
          <HistoryTimeline events={history} />
        </div>
      )}
    </div>
  )
}
