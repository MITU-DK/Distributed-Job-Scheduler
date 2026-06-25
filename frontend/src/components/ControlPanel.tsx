// Tabbed container that groups all job-management operations.
// Tabs: Submit Job | Schedule Job | Look Up / Retry

import { useState } from 'react'
import JobSubmitForm from './JobSubmitForm'
import JobScheduleForm from './JobScheduleForm'
import JobLookup from './JobLookup'

type Tab = 'submit' | 'schedule' | 'lookup'

const TABS: { id: Tab; label: string }[] = [
  { id: 'submit', label: 'Submit Job' },
  { id: 'schedule', label: 'Schedule Job' },
  { id: 'lookup', label: 'Metadata / History / Retry' },
]

export default function ControlPanel() {
  const [active, setActive] = useState<Tab>('submit')

  return (
    <section aria-label="Job control panel">
      <h2 className="text-xs font-semibold text-slate-400 uppercase tracking-widest mb-3">
        Job Control Panel
      </h2>
      <div className="bg-white rounded-xl border border-gray-200 shadow-sm overflow-hidden">
        {/* Tab bar */}
        <div className="flex border-b border-gray-200">
          {TABS.map(tab => (
            <button //buttons at top
              key={tab.id}
              id={`tab-${tab.id}`}
              onClick={() => setActive(tab.id)}
              className={`flex-1 py-3 px-2 text-xs font-semibold transition-colors ${active === tab.id
                ? 'border-b-2 border-slate-800 text-slate-800 bg-slate-50'
                : 'text-slate-400 hover:text-slate-600 hover:bg-slate-50'
                }`}
            >
              {tab.label}
            </button>
          ))}
        </div>

        {/* Active tab content */}
        <div className="p-5">
          {active === 'submit' && <JobSubmitForm />}
          {active === 'schedule' && <JobScheduleForm />}
          {active === 'lookup' && <JobLookup />}
        </div>
      </div>
    </section>
  )
}
