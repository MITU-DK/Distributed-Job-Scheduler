// A paginated table that shows all jobs that exhausted their retries.
// Columns: Job Name | Priority | Retry Count | Last Error | Enqueued At | Status

import { useState } from 'react'
import type { DeadJob, DeadJobsData } from '../api/metrics'

interface DeadJobsTableProps {
  data: DeadJobsData | null
}

// Priority number → human-readable label + color
const PRIORITY_MAP: Record<string, { label: string; className: string }> = {
  '1': { label: 'P1 High', className: 'bg-red-50 text-red-600' },
  '2': { label: 'P2 Med', className: 'bg-amber-50 text-amber-600' },
  '3': { label: 'P3 Low', className: 'bg-blue-50 text-blue-600' },
}

function PriorityBadge({ priority }: { priority: string }) {
  const p = PRIORITY_MAP[priority] ?? { label: priority || '—', className: 'bg-slate-100 text-slate-500' }
  return (
    <span className={`text-xs font-semibold px-2 py-0.5 rounded-full ${p.className}`}>
      {p.label}
    </span>
  )
}

function StatusBadge({ status }: { status: string }) {
  const isExpired = status === 'METADATA_EXPIRED'
  return (
    <span className={`text-xs font-semibold px-2 py-0.5 rounded-full ${isExpired
        ? 'bg-slate-100 text-slate-400'
        : 'bg-red-50 text-red-600'
      }`}>
      {isExpired ? 'Expired' : status}
    </span>
  )
}

// Format a Unix timestamp (seconds as string) to a readable date.
function formatTime(raw: string): string {
  if (!raw) return '—'
  const n = Number(raw)
  if (isNaN(n) || n === 0) return raw // already a string date
  return new Date(n * 1000).toLocaleString()
}

function JobRow({ job }: { job: DeadJob }) {
  const [copied, setCopied] = useState(false)

  function handleCopy() {
    navigator.clipboard.writeText(job.id)
    setCopied(true)
    setTimeout(() => setCopied(false), 2000)
  }

  return (
    <tr className="border-b border-gray-100 hover:bg-slate-50 transition-colors">
      <td className="px-4 py-3 text-sm font-mono text-slate-500 whitespace-nowrap">
        <div className="flex items-center gap-2">
          <span title={job.id}>{job.id.split('-')[0]}…</span>
          <button
            onClick={handleCopy}
            className="p-1 rounded hover:bg-slate-200 text-slate-400 hover:text-slate-600 transition-colors"
            title="Copy full ID"
          >
            {copied ? '✓' : '📋'}
          </button>
        </div>
      </td>
      <td className="px-4 py-3 text-sm font-medium text-slate-700">{job.name || '—'}</td>
      <td className="px-4 py-3"><PriorityBadge priority={job.priority} /></td>
      <td className="px-4 py-3 text-sm text-slate-600 text-center">
        {job.retry_count || '0'}/{job.max_retries || '—'}
      </td>
      <td className="px-4 py-3 text-sm text-slate-500 max-w-xs truncate" title={job.last_error}>
        {job.last_error || '—'}
      </td>
      <td className="px-4 py-3 text-sm text-slate-500 whitespace-nowrap">
        {formatTime(job.enqueued_at)}
      </td>
      <td className="px-4 py-3"><StatusBadge status={job.status} /></td>
    </tr>
  )
}

export default function DeadJobsTable({ data }: DeadJobsTableProps) {
  const jobs = data?.jobs ?? []
  const total = data?.total ?? 0

  return (
    <section aria-label="Dead jobs">
      <div className="flex items-center justify-between mb-3">
        <h2 className="text-xs font-semibold text-slate-400 uppercase tracking-widest">
          Dead-Letter Queue
        </h2>
        {total > 0 && (
          <span className="text-xs font-semibold px-2 py-0.5 rounded-full bg-red-50 text-red-500">
            {total} job{total !== 1 ? 's' : ''}
          </span>
        )}
      </div>

      <div className="bg-white rounded-xl border border-gray-200 shadow-sm overflow-hidden">
        {jobs.length === 0 ? (
          /* Empty state */
          <div className="py-16 text-center">
            <p className="text-slate-400 text-sm">No dead jobs — everything is running cleanly 🎉</p>
          </div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-left">
              <thead className="bg-slate-50 border-b border-gray-200">
                <tr>
                  {['ID', 'Job Name', 'Priority', 'Retries', 'Last Error', 'Enqueued At', 'Status'].map(h => (
                    <th key={h} className="px-4 py-3 text-xs font-semibold text-slate-500 uppercase tracking-wider">
                      {h}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {jobs.map(job => <JobRow key={job.id} job={job} />)}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </section>
  )
}
