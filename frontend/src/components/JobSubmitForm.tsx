import { useState } from 'react'
import { submitJob } from '../api/jobs'
import type { JobResponse } from '../api/jobs'

export default function JobSubmitForm() {
  const [name, setName] = useState('email')
  const [priority, setPriority] = useState(1)
  const [payload, setPayload] = useState('{"to": "user@example.com"}')
  const [maxRetries, setMaxRetries] = useState('')
  const [loading, setLoading] = useState(false)
  const [result, setResult] = useState<JobResponse | null>(null)
  const [error, setError] = useState<string | null>(null)

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault()
    setLoading(true)
    setResult(null)
    setError(null)

    // Validate JSON payload before sending
    let parsedPayload: unknown
    try {
      parsedPayload = JSON.parse(payload)
    } catch {
      setError('Payload must be valid JSON')
      setLoading(false)
      return
    }

    try {
      const res = await submitJob({ name, priority, payload: parsedPayload, ...(maxRetries !== '' ? { max_retries: parseInt(maxRetries, 10) } : {}) })
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
          <label htmlFor="submit-name" className="block text-xs font-medium text-slate-600 mb-1">
            Job Name <span className="text-red-400">*</span>
          </label>
          <select
            id="submit-name"
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
          <label htmlFor="submit-priority" className="block text-xs font-medium text-slate-600 mb-1">
            Priority <span className="text-red-400">*</span>
          </label>
          <select
            id="submit-priority"
            required
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
        <label htmlFor="submit-payload" className="block text-xs font-medium text-slate-600 mb-1">
          Payload (JSON) <span className="text-red-400">*</span>
        </label>
        <textarea
          id="submit-payload"
          rows={3}
          required
          value={payload}
          onChange={e => setPayload(e.target.value)}
          className="w-full text-sm font-mono border border-gray-200 rounded-lg px-3 py-2 text-slate-700 focus:outline-none focus:ring-2 focus:ring-slate-300"
        />
      </div>

      <div>
        <label htmlFor="submit-maxretries" className="block text-xs font-medium text-slate-600 mb-1">
          Max Retries <span className="text-slate-400">(optional )</span>
        </label>
        <input
          id="submit-maxretries"
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
        className="w-full py-2 px-4 bg-slate-800 text-white text-sm font-medium rounded-lg hover:bg-slate-700 disabled:opacity-50 transition-colors"
      >
        {loading ? 'Submitting…' : 'Submit Job'}
      </button>

      {/* Success */}
      {result && (
        <div className="flex items-start justify-between gap-2 p-3 bg-emerald-50 border border-emerald-200 rounded-lg text-xs font-mono text-emerald-700">
          <span>✓ Job created: <strong>{result.job_id}</strong> — status: {result.status}</span>
          <button onClick={() => setResult(null)} className="text-emerald-400 hover:text-emerald-700 font-bold text-sm leading-none shrink-0" aria-label="Dismiss">×</button>
        </div>
      )}
      {/* Error */}
      {error && (
        <div className="flex items-start justify-between gap-2 p-3 bg-red-50 border border-red-200 rounded-lg text-xs text-red-600">
          <span>✗ {error}</span>
          <button onClick={() => setError(null)} className="text-red-400 hover:text-red-700 font-bold text-sm leading-none shrink-0" aria-label="Dismiss">×</button>
        </div>
      )}
    </form>
  )
}
