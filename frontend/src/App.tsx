// App.tsx
// Root component. Owns all polling logic and passes data down to observer components.
// The ControlPanel is completely independent — it has its own local state.
//
// Polling strategy (observers only):
//   - Every POLL_INTERVAL_MS (5s), fire both read-only API requests in parallel.
//   - On success: update state + record lastUpdated.
//   - On error: keep last good data visible, show an error banner.
//   - On unmount: clearInterval (no memory leaks).

import { useState, useEffect, useCallback } from 'react'
import { fetchMetrics, fetchDeadJobs } from './api/metrics'
import type { MetricsData, DeadJobsData } from './api/metrics'
import Layout         from './components/Layout'
import StatsOverview  from './components/StatsOverview'
import QueueDepths    from './components/QueueDepths'
import DeadJobsTable  from './components/DeadJobsTable'
import ControlPanel   from './components/ControlPanel'

const POLL_INTERVAL_MS = 5_000

export default function App() {
  const [metrics,     setMetrics]     = useState<MetricsData | null>(null)
  const [deadJobs,    setDeadJobs]    = useState<DeadJobsData | null>(null)
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null)
  const [error,       setError]       = useState<string | null>(null)

  const poll = useCallback(async () => {
    try {
      const [m, d] = await Promise.all([fetchMetrics(), fetchDeadJobs()])
      setMetrics(m)
      setDeadJobs(d)
      setLastUpdated(new Date())
      setError(null)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Unknown error')
    }
  }, [])

  useEffect(() => {
    poll()
    const id = setInterval(poll, POLL_INTERVAL_MS)
    return () => clearInterval(id)
  }, [poll])

  return (
    <Layout lastUpdated={lastUpdated} error={error}>
      {error && (
        <div className="mb-6 px-4 py-3 bg-red-50 border border-red-200 rounded-lg text-sm text-red-600">
          <strong>Cannot reach API:</strong> {error}. Make sure the Go API is running on{' '}
          <code className="font-mono">localhost:8080</code>.
        </div>
      )}

      <div className="space-y-8">
        {/* Row 1 — KPI overview cards */}
        <StatsOverview data={metrics} />

        {/* Row 2 — Queue depth bars + Control Panel side by side on large screens */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
          <QueueDepths data={metrics} />
          <ControlPanel />
        </div>

        {/* Row 3 — Dead jobs table full width */}
        <DeadJobsTable data={deadJobs} />
      </div>
    </Layout>
  )
}
