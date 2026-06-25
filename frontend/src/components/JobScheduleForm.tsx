// Same fields as submit, plus a datetime-local picker for run_at.

import { useState } from 'react'
import { scheduleJob } from '../api/jobs'
import type { JobResponse } from '../api/jobs'

// Returns an ISO 8601 string for "now + 5 minutes" as a sensible default.
function defaultRunAt(): string {
  const d = new Date(Date.now() + 5 * 60 * 1000)
  // datetime-local input requires "YYYY-MM-DDTHH:MM" (no seconds/tz)
  return d.toISOString().slice(0, 16)
}

export default function JobScheduleForm() {
  const [name, setName] = useState('cleanup')
  const [priority, setPriority] = useState(2)
  const [payload, setPayload] = useState('{"folder": "/tmp"}')
  const [maxRetries, setMaxRetries] = useState('')
  const [runAt, setRunAt] = useState(defaultRunAt)
  const [loading, setLoading] = useState(false)
  const [result, setResult] = useState<JobResponse | null>(null)
  const [error, setError] = useState<string | null>(null)

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault()
    setLoading(true)
    setResult(null)
    setError(null)

    let parsedPayload: unknown
    try {
      parsedPayload = JSON.parse(payload)
    }
    catch {
      setError('Payload must be valid JSON')
      setLoading(false)
      return
    }

    // Convert local datetime string to RFC3339 (Go's time.Parse(time.RFC3339, ...) requires Z suffix)
    const runAtISO = new Date(runAt).toISOString()

    try {
      const res = await scheduleJob({
        name,
        priority,
        payload: parsedPayload,
        run_at: runAtISO,
        ...(maxRetries !== '' ? { max_retries: parseInt(maxRetries, 10) } : {}),
      })
      setResult(res)
    }
    catch (err) {
      setError(err instanceof Error ? err.message : 'Unknown error')
    }
    finally {
      setLoading(false)
    }
  }

  return (
    <form onSubmit={handleSubmit} className="space-y-4">
      <div className="grid grid-cols-2 gap-4">
        <div>
          <label htmlFor="sched-name" className="block text-xs font-medium text-slate-600 mb-1">
            Job Name <span className="text-red-400">*</span>
          </label>
          <select
            id="sched-name"
            required
            value={name}
            onChange={e => setName(e.target.value)}
            className="w-full text-sm border border-gray-200 rounded-lg px-3 py-2 text-slate-700 focus:outline-none focus:ring-2 focus:ring-slate-300"
          >
            <option value="email">email</option>
            <option value="backup">backup</option>
            <option value="cleanup">cleanup</option>
            <option value="sleep">sleep</option>
          </select>
        </div>
        <div>
          <label htmlFor="sched-priority" className="block text-xs font-medium text-slate-600 mb-1">
            Priority <span className="text-red-400">*</span>
          </label>
          <select
            id="sched-priority"
            value={priority}
            onChange={e => setPriority(Number(e.target.value))}
            className="w-full text-sm border border-gray-200 rounded-lg px-3 py-2 text-slate-700 focus:outline-none focus:ring-2 focus:ring-slate-300"
          >
            <option value={1}>P1 — High</option>
            <option value={2}>P2 — Medium</option>
            <option value={3}>P3 — Low</option>
          </select>
        </div>
      </div>

      <div>
        <label htmlFor="sched-runat" className="block text-xs font-medium text-slate-600 mb-1">
          Run At <span className="text-red-400">*</span>
        </label>
        <input
          id="sched-runat"
          type="datetime-local"
          required
          value={runAt}
          onChange={e => setRunAt(e.target.value)}
          className="w-full text-sm border border-gray-200 rounded-lg px-3 py-2 text-slate-700 focus:outline-none focus:ring-2 focus:ring-slate-300"
        />
        <p className="mt-1 text-xs text-slate-400">Will be sent to the server as RFC3339 UTC.</p>
      </div>

      <div>
        <label htmlFor="sched-payload" className="block text-xs font-medium text-slate-600 mb-1">
          Payload (JSON) <span className="text-red-400">*</span>
        </label>
        <textarea
          id="sched-payload"
          rows={3}
          required
          value={payload}
          onChange={e => setPayload(e.target.value)}
          className="w-full text-sm font-mono border border-gray-200 rounded-lg px-3 py-2 text-slate-700 focus:outline-none focus:ring-2 focus:ring-slate-300"
        />
      </div>

      <div>
        <label htmlFor="sched-maxretries" className="block text-xs font-medium text-slate-600 mb-1">
          Max Retries <span className="text-slate-400">(optional)</span>
        </label>
        <input
          id="sched-maxretries"
          type="number"
          min={0}
          value={maxRetries}
          onChange={e => setMaxRetries(e.target.value)}
          placeholder="e.g. 3"
          className="w-32 text-sm border border-gray-200 rounded-lg px-3 py-2 text-slate-700 focus:outline-none focus:ring-2 focus:ring-slate-300"
        />
      </div>

      <button
        type="submit"
        disabled={loading}
        className="w-full py-2 px-4 bg-slate-800 text-white text-sm font-medium rounded-lg hover:bg-violet-600 disabled:opacity-50 transition-colors"
      >
        {loading ? 'Scheduling…' : 'Schedule Job'}
      </button>

      {result && (
        <div className="flex items-start justify-between gap-2 p-3 bg-emerald-50 border border-emerald-200 rounded-lg text-xs font-mono text-emerald-700">
          <span>✓ Scheduled: <strong>{result.job_id}</strong> — runs at {result.run_at}</span>
          <button onClick={() => setResult(null)} className="text-emerald-400 hover:text-emerald-700 font-bold text-sm leading-none shrink-0" aria-label="Dismiss">×</button>
        </div>
      )}
      {error && (
        <div className="flex items-start justify-between gap-2 p-3 bg-red-50 border border-red-200 rounded-lg text-xs text-red-600">
          <span>✗ {error}</span>
          <button onClick={() => setError(null)} className="text-red-400 hover:text-red-700 font-bold text-sm leading-none shrink-0" aria-label="Dismiss">×</button>
        </div>
      )}
    </form>
  )
}
