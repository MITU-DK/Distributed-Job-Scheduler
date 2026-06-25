// A row of 4 summary cards: Active Workers, Total Processed, Total Failed, Total Dead. white box with a label

import type { MetricsData } from '../api/metrics'

interface StatsOverviewProps {
  data: MetricsData | null
}

interface StatCardProps {
  label: string
  value: number | string
  accent: string   // Tailwind border-color class e.g. "border-emerald-400"
  note?: string   // small sub-label below the number
}

function StatCard({ label, value, accent, note }: StatCardProps) {
  return (
    <div className={`bg-white rounded-xl border border-gray-200 shadow-sm p-5 border-l-4 ${accent}`}>
      <p className="text-xs font-medium text-slate-400 uppercase tracking-wider">{label}</p>
      <p className="mt-2 text-3xl font-bold text-slate-800">
        {value ?? <span className="text-slate-300 text-2xl">—</span>}
      </p>
      {note && <p className="mt-1 text-xs text-slate-400">{note}</p>}
    </div>
  )
}

export default function StatsOverview({ data }: StatsOverviewProps) {
  return (
    <section aria-label="System overview">
      <h2 className="text-xs font-semibold text-slate-400 uppercase tracking-widest mb-3">
        System Overview
      </h2>
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        <StatCard
          label="Active Workers"
          value={data?.active_workers ?? '—'}
          accent="border-l-emerald-400"
          note="live heartbeats"
        />
        <StatCard
          label="Jobs Processed"
          value={data?.total_processed ?? '—'}
          accent="border-l-blue-400"
          note="all time"
        />
        <StatCard
          label="Jobs Failed"
          value={data?.total_failed ?? '—'}
          accent="border-l-amber-400"
          note="triggered retry"
        />
        <StatCard
          label="Jobs Dead"
          value={data?.total_dead ?? '—'}
          accent="border-l-red-400"
          note="exhausted retries"
        />
      </div>
    </section>
  )
}
