// Visualises how many jobs are waiting in each queue as horizontal progress bars.
import type { MetricsData } from '../api/metrics'

interface QueueDepthsProps {
  data: MetricsData | null
}

interface QueueBarProps {
  label: string
  count: number
  maxCount: number
  barColor: string   // Tailwind bg class for the filled portion
  badge?: string   // optional pill label e.g. "HIGH"
  badgeColor?: string  // Tailwind bg + text classes for the badge
}

function QueueBar({ label, count, maxCount, barColor, badge, badgeColor }: QueueBarProps) {
  // Percentage filled, minimum 2% so an empty bar is still visible as a sliver.
  const pct = maxCount > 0 ? Math.max((count / maxCount) * 100, count > 0 ? 2 : 0) : 0

  return (
    <div className="flex items-center gap-3">
      {/* Queue label + optional priority badge */}
      <div className="w-28 shrink-0 flex items-center gap-2">
        <span className="text-sm font-medium text-slate-700">{label}</span>
        {badge && (
          <span className={`text-[10px] font-semibold px-1.5 py-0.5 rounded-full ${badgeColor}`}>
            {badge}
          </span>
        )}
      </div>

      {/* Progress bar track */}
      <div className="flex-1 bg-slate-100 rounded-full h-2 overflow-hidden">
        <div
          className={`h-2 rounded-full transition-all duration-500 ${barColor}`}
          style={{ width: `${pct}%` }}
        />
      </div>

      {/* Numeric count */}
      <span className="w-8 text-right text-sm font-semibold text-slate-600">{count}</span>
    </div>
  )
}

export default function QueueDepths({ data }: QueueDepthsProps) {
  // Find the largest queue so we can scale all bars relative to it.
  const queues = data
    ? [
      data.queue_depth_p1,
      data.queue_depth_p2,
      data.queue_depth_p3,
      data.scheduled_count,
      data.retry_count,
    ]
    : []
  const maxCount = Math.max(...queues, 1)

  return (
    <section aria-label="Queue depths">
      <h2 className="text-xs font-semibold text-slate-400 uppercase tracking-widest mb-3">
        Queue Depths
      </h2>
      <div className="bg-white rounded-xl border border-gray-200 shadow-sm p-5 space-y-4">
        <QueueBar
          label="Priority 1"
          count={data?.queue_depth_p1 ?? 0}
          maxCount={maxCount}
          barColor="bg-red-400"
          badge="HIGH"
          badgeColor="bg-red-50 text-red-600"
        />
        <QueueBar
          label="Priority 2"
          count={data?.queue_depth_p2 ?? 0}
          maxCount={maxCount}
          barColor="bg-amber-400"
          badge="MED"
          badgeColor="bg-amber-50 text-amber-600"
        />
        <QueueBar
          label="Priority 3"
          count={data?.queue_depth_p3 ?? 0}
          maxCount={maxCount}
          barColor="bg-blue-400"
          badge="LOW"
          badgeColor="bg-blue-50 text-blue-600"
        />
        <hr className="border-gray-100" />
        <QueueBar
          label="Scheduled"
          count={data?.scheduled_count ?? 0}
          maxCount={maxCount}
          barColor="bg-violet-400"
        />
        <QueueBar
          label="Retry"
          count={data?.retry_count ?? 0}
          maxCount={maxCount}
          barColor="bg-slate-400"
        />
      </div>
    </section>
  )
}
