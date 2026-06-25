// Top navigation bar. Wraps the entire page.
// Receives `lastUpdated` so the nav can show the last auto-refresh time.

interface LayoutProps {
  children: React.ReactNode
  lastUpdated: Date | null
  error: string | null
}

export default function Layout({ children, lastUpdated, error }: LayoutProps) {
  return (
    <div className="min-h-screen bg-slate-50">
      {/* ── Navigation Bar ── */}
      <nav className="bg-white border-b border-gray-200 shadow-sm sticky top-0 z-10">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 h-14 flex items-center justify-between">
          {/* Logo + Title */}
          <div className="flex items-center gap-2.5">
            <div className="w-7 h-7 rounded-md bg-slate-800 flex items-center justify-center">
              <span className="text-white text-xs font-bold">MK</span>
            </div>
            <span className="text-slate-800 font-semibold text-sm tracking-tight">
              MKron
            </span>
            <span className="text-slate-400 text-xs font-medium hidden sm:block">/ Dashboard</span>
          </div>

          {/* Status indicator */}
          <div className="flex items-center gap-3">
            {error ? (
              <span className="text-xs text-red-500 font-medium">⚠ API unreachable</span>
            ) : (
              <div className="flex items-center gap-1.5">
                <span className="w-1.5 h-1.5 rounded-full bg-emerald-500 animate-pulse" />
                <span className="text-xs text-slate-400">
                  {lastUpdated
                    ? `Updated ${lastUpdated.toLocaleTimeString()}`
                    : 'Connecting…'}
                </span>
              </div>
            )}
          </div>
        </div>
      </nav>

      {/* ── Page Content ── */}
      <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        {children}
      </main>
    </div>
  )
}
