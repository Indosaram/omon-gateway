import { FormEvent, ReactNode, useCallback, useEffect, useMemo, useRef, useState } from 'react'
import ReactMarkdown from 'react-markdown'
import {
  AllowlistRecord,
  ApiList,
  CronJob,
  CronRun,
  JsonObject,
  LogEntry,
  MemoryRecord,
  PendingApproval,
  SessionMessage,
  SessionRecord,
  SkillRecord,
  StatusResponse,
  ToolRecord,
  api,
  formatBytes,
  formatDate,
  formatDuration,
  socketUrl,
} from './api'

type Page = 'overview' | 'chat' | 'cron' | 'sessions' | 'capabilities' | 'settings' | 'logs'

const NAV_ITEMS: Array<{ id: Page; label: string; hint: string; icon: string }> = [
  { id: 'overview', label: 'Overview', hint: 'Runtime health', icon: '◫' },
  { id: 'chat', label: 'Chat', hint: 'Agent sessions', icon: '⌁' },
  { id: 'cron', label: 'Cron', hint: 'Scheduled work', icon: '◴' },
  { id: 'sessions', label: 'Sessions', hint: 'Conversation data', icon: '◎' },
  { id: 'capabilities', label: 'Skills & Tools', hint: 'Agent surface', icon: '◇' },
  { id: 'settings', label: 'Settings', hint: 'Approvals & config', icon: '⚙' },
  { id: 'logs', label: 'Logs', hint: 'Live telemetry', icon: '≋' },
]

function pageFromHash(): Page {
  const candidate = window.location.hash.replace(/^#\/?/, '') as Page
  return NAV_ITEMS.some((item) => item.id === candidate) ? candidate : 'overview'
}

function usePage(): [Page, (page: Page) => void] {
  const [page, setPageState] = useState<Page>(pageFromHash)
  useEffect(() => {
    const onHash = () => setPageState(pageFromHash())
    window.addEventListener('hashchange', onHash)
    return () => window.removeEventListener('hashchange', onHash)
  }, [])
  const setPage = useCallback((next: Page) => {
    window.location.hash = `/${next}`
    setPageState(next)
  }, [])
  return [page, setPage]
}

function useAsync<T>(loader: () => Promise<T>, dependencies: unknown[], intervalMs?: number) {
  const [data, setData] = useState<T>()
  const [error, setError] = useState<string>()
  const [loading, setLoading] = useState(true)
  const refresh = useCallback(async () => {
    try {
      const next = await loader()
      setData(next)
      setError(undefined)
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : String(reason))
    } finally {
      setLoading(false)
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, dependencies)
  useEffect(() => {
    void refresh()
    if (!intervalMs) return
    const timer = window.setInterval(() => void refresh(), intervalMs)
    return () => window.clearInterval(timer)
  }, [intervalMs, refresh])
  return { data, error, loading, refresh }
}

export default function App() {
  const [page, setPage] = usePage()
  const [mobileNav, setMobileNav] = useState(false)
  const status = useAsync(() => api<StatusResponse>('/api/status'), [], 5000)
  const current = NAV_ITEMS.find((item) => item.id === page) ?? NAV_ITEMS[0]

  return (
    <div className="min-h-screen bg-slate-950 text-slate-100">
      <div className="pointer-events-none fixed inset-0 bg-[radial-gradient(circle_at_20%_-10%,rgba(56,189,248,0.12),transparent_34%),radial-gradient(circle_at_85%_10%,rgba(129,140,248,0.10),transparent_30%)]" />
      <aside className="fixed inset-y-0 left-0 z-40 hidden w-72 border-r border-white/5 bg-slate-950/85 backdrop-blur-xl lg:block">
        <Sidebar page={page} status={status.data} onNavigate={setPage} />
      </aside>

      {mobileNav && (
        <div className="fixed inset-0 z-50 lg:hidden">
          <button
            className="absolute inset-0 bg-black/60"
            aria-label="Close navigation"
            onClick={() => setMobileNav(false)}
          />
          <aside className="relative h-full w-[86%] max-w-72 border-r border-white/10 bg-slate-950 shadow-2xl">
            <Sidebar
              page={page}
              status={status.data}
              onNavigate={(next) => {
                setPage(next)
                setMobileNav(false)
              }}
            />
          </aside>
        </div>
      )}

      <main className="relative min-h-screen lg:pl-72">
        <header className="sticky top-0 z-30 flex h-16 items-center gap-4 border-b border-white/5 bg-slate-950/75 px-4 backdrop-blur-xl sm:px-7 lg:px-10">
          <button
            onClick={() => setMobileNav(true)}
            className="grid h-9 w-9 place-items-center rounded-xl border border-white/10 bg-white/[0.03] lg:hidden"
            aria-label="Open navigation"
          >
            ☰
          </button>
          <div className="min-w-0 flex-1">
            <div className="truncate text-sm font-semibold text-white">{current.label}</div>
            <div className="hidden text-xs text-slate-500 sm:block">{current.hint}</div>
          </div>
          <StatusPill status={status.data} error={status.error} />
        </header>

        <div className="mx-auto w-full max-w-[1540px] p-4 sm:p-7 lg:p-10">
          {status.error && (
            <Notice tone="danger" title="Dashboard API unavailable">
              {status.error}
            </Notice>
          )}
          {page === 'overview' && <OverviewPage status={status.data} refresh={status.refresh} />}
          {page === 'chat' && <ChatPage />}
          {page === 'cron' && <CronPage />}
          {page === 'sessions' && <SessionsPage />}
          {page === 'capabilities' && <CapabilitiesPage />}
          {page === 'settings' && <SettingsPage />}
          {page === 'logs' && <LogsPage />}
        </div>
      </main>
    </div>
  )
}

function Sidebar({ page, status, onNavigate }: { page: Page; status?: StatusResponse; onNavigate: (page: Page) => void }) {
  return (
    <div className="flex h-full flex-col p-5">
      <div className="mb-8 flex items-center gap-3 px-2 pt-1">
        <div className="grid h-10 w-10 place-items-center rounded-2xl border border-cyan-300/20 bg-cyan-300/10 font-mono text-sm font-bold text-cyan-200 shadow-[0_0_28px_rgba(34,211,238,0.10)]">
          OM
        </div>
        <div>
          <div className="font-semibold tracking-tight text-white">omon gateway</div>
          <div className="text-xs text-slate-500">control plane</div>
        </div>
      </div>
      <nav className="space-y-1.5">
        {NAV_ITEMS.map((item) => {
          const active = item.id === page
          return (
            <button
              key={item.id}
              onClick={() => onNavigate(item.id)}
              className={`group flex w-full items-center gap-3 rounded-2xl px-3.5 py-3 text-left transition ${
                active
                  ? 'bg-white/[0.075] text-white shadow-inner ring-1 ring-white/10'
                  : 'text-slate-400 hover:bg-white/[0.035] hover:text-slate-200'
              }`}
            >
              <span className={`grid h-8 w-8 place-items-center rounded-xl font-mono text-lg ${active ? 'bg-cyan-300/10 text-cyan-200' : 'bg-white/[0.025]'}`}>
                {item.icon}
              </span>
              <span className="min-w-0">
                <span className="block text-sm font-medium">{item.label}</span>
                <span className="block truncate text-[11px] text-slate-600 group-hover:text-slate-500">{item.hint}</span>
              </span>
            </button>
          )
        })}
      </nav>
      <div className="mt-auto rounded-2xl border border-white/5 bg-white/[0.025] p-4">
        <div className="flex items-center justify-between text-xs text-slate-500">
          <span>Runtime</span>
          <span className={status?.status === 'ok' ? 'text-emerald-300' : 'text-amber-300'}>{status?.status ?? 'connecting'}</span>
        </div>
        <div className="mt-3 grid grid-cols-2 gap-3">
          <MiniMetric label="Sessions" value={status?.active_sessions ?? '—'} />
          <MiniMetric label="Bots" value={status?.bot_connections ?? '—'} />
        </div>
      </div>
    </div>
  )
}

function StatusPill({ status, error }: { status?: StatusResponse; error?: string }) {
  const healthy = status?.status === 'ok' && !error
  return (
    <div className="flex items-center gap-2 rounded-full border border-white/10 bg-white/[0.035] px-3 py-1.5 text-xs text-slate-300">
      <span className={`h-2 w-2 rounded-full ${healthy ? 'bg-emerald-400 shadow-[0_0_12px_rgba(52,211,153,0.7)]' : 'bg-amber-400'}`} />
      {healthy ? 'Operational' : error ? 'Disconnected' : 'Connecting'}
    </div>
  )
}

function OverviewPage({ status, refresh }: { status?: StatusResponse; refresh: () => Promise<void> }) {
  const sessions = useAsync(() => api<ApiList<SessionRecord>>('/api/sessions?per_page=6'), [], 7000)
  const cron = useAsync(() => api<ApiList<CronJob>>('/api/cron/jobs'), [], 8000)
  const approvals = useAsync(() => api<{ items: PendingApproval[]; pending_count: number }>('/api/approvals/pending'), [], 5000)
  const diskUsed = status?.disk.workspace_total_bytes != null && status.disk.workspace_available_bytes != null
    ? status.disk.workspace_total_bytes - status.disk.workspace_available_bytes
    : null

  return (
    <PageStack>
      <Hero
        eyebrow="OPERATIONS"
        title="Gateway at a glance"
        description="Live runtime health, session activity, scheduled work, and approval pressure in one view."
        action={<Button onClick={() => void refresh()} variant="ghost">Refresh now</Button>}
      />
      <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
        <MetricCard label="Uptime" value={status ? formatDuration(status.uptime_seconds) : '—'} detail="Current process" accent="cyan" />
        <MetricCard label="Active sessions" value={status?.active_sessions ?? '—'} detail={`${status?.database.sessions ?? '—'} stored`} accent="indigo" />
        <MetricCard label="Bot connections" value={status?.bot_connections ?? '—'} detail={status?.chat_available ? 'Web chat ready' : 'Chat unavailable'} accent="emerald" />
        <MetricCard label="Pending approvals" value={approvals.data?.pending_count ?? status?.pending_approvals ?? '—'} detail="Requires attention" accent={approvals.data?.pending_count ? 'amber' : 'slate'} />
      </div>
      <div className="grid gap-5 xl:grid-cols-[1.45fr_1fr]">
        <Panel title="Recent sessions" subtitle="Latest stored activity across Discord, web, and cron">
          <div className="divide-y divide-white/5">
            {(sessions.data?.items ?? []).map((session) => (
              <div key={session.id} className="flex items-center gap-3 py-3.5">
                <PlatformBadge platform={session.platform} />
                <div className="min-w-0 flex-1">
                  <div className="truncate text-sm font-medium text-slate-200">{session.channel_id}</div>
                  <div className="truncate text-xs text-slate-500">{session.user_id}</div>
                </div>
                <div className="text-right text-xs text-slate-500">{formatDate(session.updated_at)}</div>
              </div>
            ))}
            {!sessions.loading && !(sessions.data?.items.length) && <EmptyState>No sessions yet.</EmptyState>}
          </div>
        </Panel>
        <Panel title="Resource footprint" subtitle="Process and workspace capacity">
          <div className="space-y-5 py-2">
            <ResourceRow label="Process memory" value={formatBytes(status?.memory.process_bytes)} />
            <ResourceRow label="Workspace used" value={formatBytes(diskUsed)} />
            <ResourceRow label="Workspace free" value={formatBytes(status?.disk.workspace_available_bytes)} />
            <ResourceRow label="DB connections" value={status ? `${status.database.pool_size - status.database.pool_idle} busy / ${status.database.pool_size} total` : '—'} />
          </div>
        </Panel>
      </div>
      <div className="grid gap-5 lg:grid-cols-2">
        <Panel title="Scheduled work" subtitle="Next enabled cron jobs">
          <div className="space-y-2">
            {(cron.data?.items ?? []).filter((job) => job.enabled).slice(0, 6).map((job) => (
              <div key={job.id} className="flex items-center gap-3 rounded-xl border border-white/5 bg-black/10 p-3">
                <span className="h-2 w-2 rounded-full bg-cyan-400" />
                <div className="min-w-0 flex-1">
                  <div className="truncate text-sm text-slate-200">{job.id}</div>
                  <div className="font-mono text-[11px] text-slate-500">{job.expression}</div>
                </div>
                <div className="text-xs text-slate-500">{formatDate(job.next_run_at)}</div>
              </div>
            ))}
            {!cron.loading && !(cron.data?.items.some((job) => job.enabled)) && <EmptyState>No enabled cron jobs.</EmptyState>}
          </div>
        </Panel>
        <Panel title="Database" subtitle="Durable gateway records">
          <div className="grid grid-cols-2 gap-3 py-2">
            <DataTile label="Sessions" value={status?.database.sessions} />
            <DataTile label="Messages" value={status?.database.messages} />
            <DataTile label="Cron jobs" value={status?.database.cron_jobs} />
            <DataTile label="Memories" value={status?.database.memories} />
          </div>
        </Panel>
      </div>
    </PageStack>
  )
}

type ChatItem = {
  id: string
  role: 'user' | 'assistant' | 'tool' | 'system'
  content: string
  final?: boolean
}

function ChatPage() {
  const [sessionId, setSessionId] = useState(() => localStorage.getItem('omon-web-session') || 'dashboard')
  const [activeSession, setActiveSession] = useState(sessionId)
  const [items, setItems] = useState<ChatItem[]>([])
  const [input, setInput] = useState('')
  const [connection, setConnection] = useState<'connecting' | 'open' | 'closed' | 'error'>('connecting')
  const [typing, setTyping] = useState(false)
  const [chatAvailable, setChatAvailable] = useState(true)
  const [error, setError] = useState<string>()
  const socketRef = useRef<WebSocket>()
  const streamIds = useRef(new Map<string, string>())
  const bottomRef = useRef<HTMLDivElement>(null)

  const loadHistory = useCallback(async (id: string) => {
    try {
      const history = await api<ApiList<SessionMessage>>(`/api/sessions/${encodeURIComponent(id)}/messages?per_page=200`)
      setItems(history.items.filter((message) => message.role !== 'tool').map((message) => ({
        id: message.id,
        role: message.role === 'user' ? 'user' : 'assistant',
        content: message.content,
        final: true,
      })))
    } catch {
      setItems([])
    }
  }, [])

  useEffect(() => {
    localStorage.setItem('omon-web-session', activeSession)
    void loadHistory(activeSession)
    streamIds.current.clear()
    setConnection('connecting')
    setError(undefined)
    const socket = new WebSocket(socketUrl(`/api/sessions/${encodeURIComponent(activeSession)}/ws`))
    socketRef.current = socket
    socket.onopen = () => setConnection('open')
    socket.onerror = () => {
      setConnection('error')
      setError('WebSocket connection failed.')
    }
    socket.onclose = () => setConnection('closed')
    socket.onmessage = (message) => {
      let payload: any
      try {
        payload = JSON.parse(String(message.data))
      } catch {
        return
      }
      if (payload.type === 'ready') {
        setChatAvailable(Boolean(payload.chat_available))
        return
      }
      if (payload.type === 'error') {
        setError(payload.message || 'Chat request failed.')
        return
      }
      if (payload.type !== 'event' || !payload.event) return
      const event = payload.event
      if (event.type === 'typing') {
        setTyping(Boolean(event.active))
        return
      }
      if (event.type === 'stream' && event.chunk) {
        const chunk = event.chunk
        const toolStatus = String(chunk.content).startsWith('⚙️ Running tool')
        if (toolStatus) {
          setItems((current) => [...current, { id: `tool-${chunk.stream_id}-${Date.now()}`, role: 'tool', content: chunk.content, final: true }])
          return
        }
        const known = streamIds.current.get(chunk.stream_id)
        if (known) {
          setItems((current) => current.map((item) => item.id === known ? { ...item, content: chunk.content, final: chunk.is_final } : item))
        } else {
          const id = `stream-${chunk.stream_id}`
          streamIds.current.set(chunk.stream_id, id)
          setItems((current) => [...current, { id, role: 'assistant', content: chunk.content, final: chunk.is_final }])
        }
        return
      }
      if (event.type === 'send_message' && event.content) {
        setItems((current) => [...current, { id: `message-${Date.now()}`, role: 'assistant', content: event.content, final: true }])
        return
      }
      if (event.type === 'approval_request') {
        setItems((current) => [...current, {
          id: `approval-${event.request_id}`,
          role: 'system',
          content: `Approval required: ${event.command}\n\n${event.reason || ''}`,
          final: true,
        }])
      }
    }
    return () => socket.close()
  }, [activeSession, loadHistory])

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth', block: 'end' })
  }, [items, typing])

  const connectSession = (event: FormEvent) => {
    event.preventDefault()
    const next = sessionId.trim()
    if (next) setActiveSession(next)
  }

  const send = (event: FormEvent) => {
    event.preventDefault()
    const content = input.trim()
    if (!content || socketRef.current?.readyState !== WebSocket.OPEN) return
    setItems((current) => [...current, { id: `local-${Date.now()}`, role: 'user', content, final: true }])
    socketRef.current.send(JSON.stringify({ type: 'message', content }))
    setInput('')
    setError(undefined)
  }

  return (
    <PageStack>
      <Hero
        eyebrow="AGENT CONSOLE"
        title="Streaming web chat"
        description="Use the same agent runner, tools, persistence, and approval policy through a dashboard-native session."
      />
      <div className="grid min-h-[690px] gap-5 xl:grid-cols-[290px_1fr]">
        <Panel title="Session" subtitle="Select or create a web session" className="h-fit">
          <form className="space-y-3" onSubmit={connectSession}>
            <Field label="Session ID">
              <Input value={sessionId} onChange={(event) => setSessionId(event.target.value)} placeholder="dashboard" />
            </Field>
            <Button type="submit" className="w-full">Open session</Button>
          </form>
          <div className="mt-5 border-t border-white/5 pt-5">
            <div className="mb-3 flex items-center justify-between text-xs">
              <span className="text-slate-500">Connection</span>
              <ConnectionBadge value={connection} />
            </div>
            <div className="rounded-xl bg-black/15 p-3 font-mono text-[11px] text-slate-500 break-all">{activeSession}</div>
          </div>
          {!chatAvailable && (
            <Notice tone="warning" title="Chat runtime disabled" compact>
              Set DEFAULT_MODEL and provider credentials, then restart the dashboard.
            </Notice>
          )}
        </Panel>

        <div className="flex min-h-[690px] flex-col overflow-hidden rounded-2xl border border-white/[0.07] bg-slate-900/35 shadow-panel">
          <div className="flex items-center justify-between border-b border-white/5 px-5 py-4">
            <div>
              <div className="text-sm font-medium text-white">{activeSession}</div>
              <div className="text-xs text-slate-500">Web agent session</div>
            </div>
            {typing && <span className="animate-pulse text-xs text-cyan-300">Agent is working…</span>}
          </div>
          <div className="flex-1 space-y-5 overflow-y-auto px-4 py-6 sm:px-6">
            {!items.length && (
              <div className="mx-auto mt-16 max-w-md text-center">
                <div className="mx-auto mb-5 grid h-14 w-14 place-items-center rounded-2xl border border-cyan-300/15 bg-cyan-300/[0.07] text-2xl text-cyan-200">⌁</div>
                <div className="font-medium text-slate-200">Start a dashboard session</div>
                <p className="mt-2 text-sm leading-6 text-slate-500">Messages stream progressively and tool execution appears inline as it happens.</p>
              </div>
            )}
            {items.map((item) => <ChatBubble key={item.id} item={item} />)}
            <div ref={bottomRef} />
          </div>
          {error && <div className="mx-4 mb-2 rounded-xl border border-rose-400/20 bg-rose-400/5 px-3 py-2 text-xs text-rose-200 sm:mx-6">{error}</div>}
          <form onSubmit={send} className="border-t border-white/5 p-4 sm:p-5">
            <div className="flex items-end gap-3 rounded-2xl border border-white/10 bg-black/15 p-2 focus-within:border-cyan-300/30">
              <textarea
                value={input}
                onChange={(event) => setInput(event.target.value)}
                onKeyDown={(event) => {
                  if (event.key === 'Enter' && !event.shiftKey) {
                    event.preventDefault()
                    event.currentTarget.form?.requestSubmit()
                  }
                }}
                rows={2}
                placeholder="Ask the agent…"
                className="min-h-12 flex-1 resize-none bg-transparent px-2 py-2 text-sm text-slate-100 outline-none placeholder:text-slate-600"
              />
              <Button type="submit" disabled={connection !== 'open' || !chatAvailable || !input.trim()}>Send</Button>
            </div>
            <div className="mt-2 px-1 text-[11px] text-slate-600">Enter to send · Shift+Enter for a new line</div>
          </form>
        </div>
      </div>
    </PageStack>
  )
}

function ChatBubble({ item }: { item: ChatItem }) {
  if (item.role === 'tool') {
    return (
      <div className="flex justify-center">
        <div className="rounded-full border border-indigo-300/15 bg-indigo-300/[0.06] px-3 py-1.5 font-mono text-[11px] text-indigo-200">{item.content}</div>
      </div>
    )
  }
  if (item.role === 'system') {
    return <div className="rounded-xl border border-amber-300/15 bg-amber-300/[0.05] p-3 text-xs text-amber-100 whitespace-pre-wrap">{item.content}</div>
  }
  const user = item.role === 'user'
  return (
    <div className={`flex ${user ? 'justify-end' : 'justify-start'}`}>
      <div className={`max-w-[88%] rounded-2xl px-4 py-3 text-sm leading-6 sm:max-w-[78%] ${
        user
          ? 'rounded-br-md bg-cyan-300 text-slate-950'
          : 'rounded-bl-md border border-white/[0.07] bg-white/[0.045] text-slate-200'
      }`}>
        {user ? <div className="whitespace-pre-wrap">{item.content}</div> : <Markdown>{item.content}</Markdown>}
        {!user && item.final === false && <span className="ml-1 inline-block h-3 w-1 animate-pulse rounded bg-cyan-300 align-middle" />}
      </div>
    </div>
  )
}

function CronPage() {
  const jobs = useAsync(() => api<ApiList<CronJob>>('/api/cron/jobs'), [], 5000)
  const runs = useAsync(() => api<ApiList<CronRun>>('/api/cron/runs?limit=30'), [], 5000)
  const [expression, setExpression] = useState('0 */15 * * * *')
  const [prompt, setPrompt] = useState('')
  const [error, setError] = useState<string>()
  const [saving, setSaving] = useState(false)

  const create = async (event: FormEvent) => {
    event.preventDefault()
    if (!expression.trim() || !prompt.trim()) return
    setSaving(true)
    try {
      await api('/api/cron/jobs', {
        method: 'POST',
        body: JSON.stringify({ expression: expression.trim(), payload: { prompt: prompt.trim() } }),
      })
      setPrompt('')
      setError(undefined)
      await jobs.refresh()
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : String(reason))
    } finally {
      setSaving(false)
    }
  }

  const act = async (job: CronJob, action: 'pause' | 'resume' | 'trigger' | 'delete') => {
    try {
      if (action === 'delete') {
        await api(`/api/cron/jobs/${encodeURIComponent(job.id)}`, { method: 'DELETE' })
      } else {
        await api(`/api/cron/jobs/${encodeURIComponent(job.id)}/${action}`, { method: 'POST' })
      }
      setError(undefined)
      await Promise.all([jobs.refresh(), runs.refresh()])
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : String(reason))
    }
  }

  return (
    <PageStack>
      <Hero eyebrow="SCHEDULER" title="Cron manager" description="Create, pause, resume, trigger, and inspect durable scheduled tasks without leaving the gateway." />
      {error && <Notice tone="danger" title="Cron operation failed">{error}</Notice>}
      <div className="grid gap-5 xl:grid-cols-[1.35fr_0.65fr]">
        <Panel title="Scheduled jobs" subtitle={`${jobs.data?.items.length ?? 0} registered jobs`}>
          <div className="space-y-3">
            {(jobs.data?.items ?? []).map((job) => (
              <div key={job.id} className="rounded-2xl border border-white/[0.065] bg-black/10 p-4">
                <div className="flex flex-wrap items-start gap-3">
                  <div className="min-w-0 flex-1">
                    <div className="flex items-center gap-2">
                      <span className={`h-2 w-2 rounded-full ${job.enabled ? 'bg-emerald-400' : 'bg-slate-600'}`} />
                      <div className="truncate font-mono text-sm text-slate-200">{job.id}</div>
                    </div>
                    <div className="mt-2 flex flex-wrap gap-2 text-[11px] text-slate-500">
                      <CodeChip>{job.expression}</CodeChip>
                      <span>Next {formatDate(job.next_run_at)}</span>
                    </div>
                    <pre className="mt-3 max-h-28 overflow-auto whitespace-pre-wrap rounded-xl bg-slate-950/55 p-3 text-[11px] leading-5 text-slate-500">{JSON.stringify(job.payload, null, 2)}</pre>
                  </div>
                  <div className="flex flex-wrap gap-2">
                    <SmallButton onClick={() => void act(job, 'trigger')}>Run now</SmallButton>
                    <SmallButton onClick={() => void act(job, job.enabled ? 'pause' : 'resume')}>{job.enabled ? 'Pause' : 'Resume'}</SmallButton>
                    <SmallButton tone="danger" onClick={() => void act(job, 'delete')}>Delete</SmallButton>
                  </div>
                </div>
              </div>
            ))}
            {!jobs.loading && !(jobs.data?.items.length) && <EmptyState>No cron jobs registered.</EmptyState>}
          </div>
        </Panel>
        <Panel title="New job" subtitle="Six-field cron expressions are supported" className="h-fit">
          <form onSubmit={create} className="space-y-4">
            <Field label="Schedule">
              <Input value={expression} onChange={(event) => setExpression(event.target.value)} placeholder="0 */15 * * * *" className="font-mono" />
            </Field>
            <Field label="Agent prompt">
              <textarea value={prompt} onChange={(event) => setPrompt(event.target.value)} rows={7} placeholder="Summarize the current project status…" className="input-base resize-y" />
            </Field>
            <Button disabled={saving || !expression.trim() || !prompt.trim()} type="submit" className="w-full">{saving ? 'Creating…' : 'Create job'}</Button>
          </form>
        </Panel>
      </div>
      <Panel title="Execution history" subtitle="Recent lease-backed cron runs and failures">
        <div className="overflow-x-auto">
          <table className="w-full min-w-[760px] text-left text-xs">
            <thead className="text-slate-600"><tr><Th>Run</Th><Th>Job</Th><Th>Status</Th><Th>Attempt</Th><Th>Started</Th><Th>Error</Th></tr></thead>
            <tbody className="divide-y divide-white/5">
              {(runs.data?.items ?? []).map((run) => (
                <tr key={run.run_id} className="text-slate-400">
                  <Td><span className="font-mono text-[11px]">{run.run_id.slice(0, 12)}</span></Td>
                  <Td><span className="font-mono text-[11px] text-slate-300">{run.job_id}</span></Td>
                  <Td><RunStatus value={run.status} /></Td>
                  <Td>{run.attempt}</Td><Td>{formatDate(run.started_at)}</Td>
                  <Td><span className="line-clamp-2 max-w-xs text-rose-300/80">{run.error || '—'}</span></Td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </Panel>
    </PageStack>
  )
}

function SessionsPage() {
  const [search, setSearch] = useState('')
  const [query, setQuery] = useState('')
  const sessions = useAsync(
    () => api<ApiList<SessionRecord>>(`/api/sessions?per_page=100${query ? `&search=${encodeURIComponent(query)}` : ''}`),
    [query],
  )
  const [selected, setSelected] = useState<string>()
  const detail = useAsync(
    async () => selected ? api<SessionRecord>(`/api/sessions/${encodeURIComponent(selected)}`) : undefined,
    [selected],
  )
  const messages = useAsync(
    async () => selected ? api<ApiList<SessionMessage>>(`/api/sessions/${encodeURIComponent(selected)}/messages?per_page=200`) : undefined,
    [selected],
  )
  const [error, setError] = useState<string>()

  const remove = async () => {
    if (!selected || !window.confirm('Delete this stored session and transcript?')) return
    try {
      await api(`/api/sessions/${encodeURIComponent(selected)}`, { method: 'DELETE' })
      setSelected(undefined)
      await sessions.refresh()
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : String(reason))
    }
  }

  return (
    <PageStack>
      <Hero eyebrow="SESSION STORE" title="Sessions explorer" description="Search durable sessions across Discord, web, and cron, then inspect their exact transcript and metadata." />
      {error && <Notice tone="danger" title="Session operation failed">{error}</Notice>}
      <div className="grid gap-5 xl:grid-cols-[420px_1fr]">
        <Panel title="Sessions" subtitle={`${sessions.data?.total ?? 0} matching records`}>
          <form onSubmit={(event) => { event.preventDefault(); setQuery(search.trim()) }} className="mb-4 flex gap-2">
            <Input value={search} onChange={(event) => setSearch(event.target.value)} placeholder="Search session, user, channel…" />
            <Button type="submit" variant="ghost">Search</Button>
          </form>
          <div className="max-h-[720px] space-y-2 overflow-y-auto pr-1">
            {(sessions.data?.items ?? []).map((session) => (
              <button key={session.id} onClick={() => setSelected(session.id)} className={`w-full rounded-xl border p-3 text-left transition ${selected === session.id ? 'border-cyan-300/25 bg-cyan-300/[0.06]' : 'border-white/5 bg-black/10 hover:bg-white/[0.035]'}`}>
                <div className="flex items-center gap-2"><PlatformBadge platform={session.platform} /><span className="min-w-0 truncate text-sm text-slate-200">{session.channel_id}</span></div>
                <div className="mt-2 flex items-center justify-between gap-2 text-[11px] text-slate-600"><span className="truncate">{session.user_id}</span><span className="shrink-0">{formatDate(session.updated_at)}</span></div>
              </button>
            ))}
            {!sessions.loading && !(sessions.data?.items.length) && <EmptyState>No matching sessions.</EmptyState>}
          </div>
        </Panel>
        <Panel title={detail.data?.channel_id || 'Session detail'} subtitle={selected ? detail.data?.id || selected : 'Select a session to inspect'}>
          {!selected ? <EmptyState>Select a session from the list.</EmptyState> : (
            <div className="space-y-5">
              {detail.data && (
                <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
                  <DataTile label="Platform" value={detail.data.platform} /><DataTile label="User" value={detail.data.user_id} />
                  <DataTile label="Messages" value={detail.data.message_count ?? messages.data?.total ?? '—'} /><DataTile label="Active" value={detail.data.active ? 'Yes' : 'No'} />
                </div>
              )}
              <div className="flex justify-end"><SmallButton tone="danger" onClick={() => void remove()}>Delete session</SmallButton></div>
              <div className="max-h-[600px] space-y-3 overflow-y-auto rounded-2xl border border-white/5 bg-black/10 p-4">
                {(messages.data?.items ?? []).map((message) => (
                  <div key={message.id} className="rounded-xl border border-white/5 bg-white/[0.025] p-3">
                    <div className="mb-2 flex items-center justify-between gap-3"><RoleBadge role={message.role} /><span className="text-[10px] text-slate-600">#{message.sequence} · {formatDate(message.created_at)}</span></div>
                    <div className="text-sm leading-6 text-slate-300"><Markdown>{message.content || '∅'}</Markdown></div>
                    {message.metadata && Object.keys(message.metadata).length > 0 && <details className="mt-3"><summary className="cursor-pointer text-[11px] text-slate-600">Metadata</summary><pre className="mt-2 overflow-auto text-[10px] text-slate-600">{JSON.stringify(message.metadata, null, 2)}</pre></details>}
                  </div>
                ))}
              </div>
            </div>
          )}
        </Panel>
      </div>
    </PageStack>
  )
}

function CapabilitiesPage() {
  const tools = useAsync(() => api<ApiList<ToolRecord>>('/api/tools'), [])
  const skills = useAsync(() => api<ApiList<SkillRecord>>('/api/skills'), [])
  const [tab, setTab] = useState<'tools' | 'skills'>('tools')
  return (
    <PageStack>
      <Hero eyebrow="CAPABILITIES" title="Skills & tools" description="Inspect the callable tool surface and discovered Hermes/Omon skill catalog available to the agent runtime." />
      <Segmented value={tab} options={[['tools', `Tools · ${tools.data?.items.length ?? 0}`], ['skills', `Skills · ${skills.data?.items.length ?? 0}`]]} onChange={(value) => setTab(value as 'tools' | 'skills')} />
      {tab === 'tools' ? (
        <div className="grid gap-4 lg:grid-cols-2 2xl:grid-cols-3">
          {(tools.data?.items ?? []).map((tool) => <CapabilityCard key={tool.name} title={tool.name} description={tool.description} meta="Registered tool"><pre className="max-h-64 overflow-auto whitespace-pre-wrap text-[10px] leading-5 text-slate-600">{JSON.stringify(tool.input_schema, null, 2)}</pre></CapabilityCard>)}
        </div>
      ) : (
        <div className="grid gap-4 lg:grid-cols-2 2xl:grid-cols-3">
          {(skills.data?.items ?? []).map((skill) => <CapabilityCard key={`${skill.source}:${skill.path}`} title={skill.name} description={skill.description || 'No description found in SKILL.md.'} meta={skill.source}><div className="break-all font-mono text-[10px] text-slate-600">{skill.path}</div></CapabilityCard>)}
          {!skills.loading && !(skills.data?.items.length) && <Panel title="No skills discovered"><EmptyState>Add SKILL.md files under the configured Hermes or Omon skill roots.</EmptyState></Panel>}
        </div>
      )}
    </PageStack>
  )
}

function SettingsPage() {
  const config = useAsync(() => api<JsonObject>('/api/config'), [])
  const approvals = useAsync(() => api<{ items: PendingApproval[]; pending_count: number }>('/api/approvals/pending'), [], 4000)
  const allowlist = useAsync(() => api<ApiList<AllowlistRecord>>('/api/approvals/allowlist'), [], 6000)
  const memory = useAsync(() => api<ApiList<MemoryRecord>>('/api/memory?per_page=30'), [], 10000)
  const [error, setError] = useState<string>()

  const resolve = async (id: string, decision: 'Once' | 'Session' | 'Always' | 'Deny') => {
    try {
      await api(`/api/approvals/${id}/resolve`, { method: 'POST', body: JSON.stringify({ decision }) })
      setError(undefined)
      await Promise.all([approvals.refresh(), allowlist.refresh()])
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : String(reason))
    }
  }

  return (
    <PageStack>
      <Hero eyebrow="CONTROL & SECURITY" title="Settings & approvals" description="Read the effective, redacted runtime configuration and resolve dangerous-command approvals from one place." />
      {error && <Notice tone="danger" title="Approval operation failed">{error}</Notice>}
      <div className="grid gap-5 xl:grid-cols-[1.1fr_0.9fr]">
        <Panel title="Pending approvals" subtitle={`${approvals.data?.pending_count ?? 0} requests awaiting a decision`}>
          <div className="space-y-3">
            {(approvals.data?.items ?? []).map((approval) => (
              <div key={approval.id} className="rounded-2xl border border-amber-300/15 bg-amber-300/[0.035] p-4">
                <div className="flex flex-wrap items-start gap-3"><div className="min-w-0 flex-1"><div className="font-mono text-xs text-amber-100 break-all">{approval.command}</div><div className="mt-2 text-xs leading-5 text-slate-500">{approval.reason || 'No reason supplied.'}</div><div className="mt-2 text-[10px] text-slate-600">{approval.session.platform} · {approval.session.channel_id} · {formatDate(approval.created_at)}</div></div></div>
                <div className="mt-4 flex flex-wrap gap-2"><SmallButton onClick={() => void resolve(approval.id, 'Once')}>Once</SmallButton><SmallButton onClick={() => void resolve(approval.id, 'Session')}>Session</SmallButton><SmallButton onClick={() => void resolve(approval.id, 'Always')}>Always</SmallButton><SmallButton tone="danger" onClick={() => void resolve(approval.id, 'Deny')}>Deny</SmallButton></div>
              </div>
            ))}
            {!approvals.loading && !(approvals.data?.items.length) && <EmptyState>No pending approvals.</EmptyState>}
          </div>
        </Panel>
        <Panel title="Persistent allowlist" subtitle="Commands approved with Always">
          <div className="space-y-2">
            {(allowlist.data?.items ?? []).map((rule) => <div key={rule.pattern_key} className="rounded-xl border border-white/5 bg-black/10 p-3"><div className="break-all font-mono text-xs text-slate-300">{rule.pattern_key}</div><div className="mt-1 text-[10px] text-slate-600">Added {formatDate(rule.created_at)}</div></div>)}
            {!allowlist.loading && !(allowlist.data?.items.length) && <EmptyState>No persistent allowlist rules.</EmptyState>}
          </div>
        </Panel>
      </div>
      <div className="grid gap-5 xl:grid-cols-2">
        <Panel title="Effective configuration" subtitle="Provider secrets are intentionally redacted">
          <JsonTree value={config.data ?? {}} />
        </Panel>
        <Panel title="Recent memory" subtitle="Latest persistent memory entries">
          <div className="max-h-[520px] space-y-2 overflow-y-auto">
            {(memory.data?.items ?? []).map((entry) => <div key={entry.id} className="rounded-xl border border-white/5 bg-black/10 p-3"><div className="line-clamp-4 text-xs leading-5 text-slate-400">{entry.content}</div><div className="mt-2 truncate font-mono text-[10px] text-slate-600">{entry.session_key}</div></div>)}
            {!memory.loading && !(memory.data?.items.length) && <EmptyState>No stored memories.</EmptyState>}
          </div>
        </Panel>
      </div>
    </PageStack>
  )
}

function LogsPage() {
  const [entries, setEntries] = useState<LogEntry[]>([])
  const [level, setLevel] = useState('ALL')
  const [search, setSearch] = useState('')
  const [connected, setConnected] = useState(false)
  const [paused, setPaused] = useState(false)
  const pausedRef = useRef(false)
  useEffect(() => { pausedRef.current = paused }, [paused])
  useEffect(() => {
    void api<ApiList<LogEntry>>('/api/logs?limit=300').then((result) => setEntries(result.items)).catch(() => undefined)
    const socket = new WebSocket(socketUrl('/api/logs/ws'))
    socket.onopen = () => setConnected(true)
    socket.onclose = () => setConnected(false)
    socket.onerror = () => setConnected(false)
    socket.onmessage = (message) => {
      if (pausedRef.current) return
      try {
        const payload = JSON.parse(String(message.data)) as { type?: string; entry?: LogEntry }
        if (payload.type === 'log' && payload.entry) {
          setEntries((current) => [...current.slice(-999), payload.entry!])
        }
      } catch {
        // Ignore malformed log frames.
      }
    }
    return () => socket.close()
  }, [])
  const filtered = useMemo(() => {
    const needle = search.trim().toLowerCase()
    return entries.filter((entry) => (level === 'ALL' || entry.level.toUpperCase() === level) && (!needle || `${entry.target} ${entry.message} ${JSON.stringify(entry.fields)}`.toLowerCase().includes(needle)))
  }, [entries, level, search])

  return (
    <PageStack>
      <Hero eyebrow="TELEMETRY" title="Live log viewer" description="Tail structured tracing events from the running process without shell access." action={<div className={`rounded-full px-3 py-1.5 text-xs ${connected ? 'bg-emerald-400/10 text-emerald-300' : 'bg-rose-400/10 text-rose-300'}`}>{connected ? 'Live' : 'Disconnected'}</div>} />
      <Panel title="Event stream" subtitle={`${filtered.length} visible · ${entries.length} buffered`}>
        <div className="mb-4 flex flex-wrap gap-2">
          <select value={level} onChange={(event) => setLevel(event.target.value)} className="input-base w-32"><option>ALL</option><option>ERROR</option><option>WARN</option><option>INFO</option><option>DEBUG</option><option>TRACE</option></select>
          <Input value={search} onChange={(event) => setSearch(event.target.value)} placeholder="Filter target, message, or fields…" className="min-w-52 flex-1" />
          <Button variant="ghost" onClick={() => setPaused((value) => !value)}>{paused ? 'Resume' : 'Pause'}</Button>
          <Button variant="ghost" onClick={() => setEntries([])}>Clear</Button>
        </div>
        <div className="h-[650px] overflow-auto rounded-xl border border-white/5 bg-[#070910] font-mono text-[11px]">
          {filtered.map((entry) => <LogLine key={`${entry.id}-${entry.timestamp}`} entry={entry} />)}
          {!filtered.length && <div className="p-8 text-center text-slate-700">No matching log entries.</div>}
        </div>
      </Panel>
    </PageStack>
  )
}

function LogLine({ entry }: { entry: LogEntry }) {
  const tone = entry.level === 'ERROR' ? 'text-rose-300' : entry.level === 'WARN' ? 'text-amber-300' : entry.level === 'INFO' ? 'text-cyan-300' : 'text-slate-500'
  return <div className="grid grid-cols-[85px_58px_minmax(120px,220px)_1fr] gap-3 border-b border-white/[0.035] px-3 py-2 hover:bg-white/[0.025]"><span className="text-slate-700">{new Date(entry.timestamp).toLocaleTimeString()}</span><span className={tone}>{entry.level}</span><span className="truncate text-indigo-300/65">{entry.target}</span><span className="break-words text-slate-400">{entry.message || JSON.stringify(entry.fields)}</span></div>
}

function CapabilityCard({ title, description, meta, children }: { title: string; description: string; meta: string; children: ReactNode }) {
  return <div className="rounded-2xl border border-white/[0.07] bg-slate-900/35 p-5 shadow-panel"><div className="mb-1 font-mono text-sm font-semibold text-cyan-200">{title}</div><div className="mb-4 truncate text-[10px] uppercase tracking-wider text-slate-600">{meta}</div><p className="mb-4 min-h-12 text-xs leading-5 text-slate-400">{description}</p><div className="rounded-xl border border-white/5 bg-black/15 p-3">{children}</div></div>
}

function JsonTree({ value }: { value: unknown }) {
  return <pre className="max-h-[560px] overflow-auto rounded-xl border border-white/5 bg-black/15 p-4 text-[11px] leading-5 text-slate-400">{JSON.stringify(value, null, 2)}</pre>
}

function Markdown({ children }: { children: string }) {
  return <div className="markdown"><ReactMarkdown>{children}</ReactMarkdown></div>
}

function PageStack({ children }: { children: ReactNode }) { return <div className="space-y-6">{children}</div> }

function Hero({ eyebrow, title, description, action }: { eyebrow: string; title: string; description: string; action?: ReactNode }) {
  return <div className="flex flex-col gap-4 pb-1 sm:flex-row sm:items-end sm:justify-between"><div><div className="mb-2 font-mono text-[10px] font-semibold tracking-[0.24em] text-cyan-300/70">{eyebrow}</div><h1 className="text-2xl font-semibold tracking-tight text-white sm:text-3xl">{title}</h1><p className="mt-2 max-w-2xl text-sm leading-6 text-slate-500">{description}</p></div>{action}</div>
}

function Panel({ title, subtitle, children, className = '' }: { title: string; subtitle?: string; children: ReactNode; className?: string }) {
  return <section className={`rounded-2xl border border-white/[0.07] bg-slate-900/35 shadow-panel ${className}`}><div className="border-b border-white/5 px-5 py-4"><h2 className="text-sm font-semibold text-slate-200">{title}</h2>{subtitle && <p className="mt-1 text-xs text-slate-600">{subtitle}</p>}</div><div className="p-5">{children}</div></section>
}

function MetricCard({ label, value, detail, accent }: { label: string; value: ReactNode; detail: string; accent: 'cyan' | 'indigo' | 'emerald' | 'amber' | 'slate' }) {
  const accentClass = { cyan: 'from-cyan-400/15 text-cyan-200', indigo: 'from-indigo-400/15 text-indigo-200', emerald: 'from-emerald-400/15 text-emerald-200', amber: 'from-amber-400/15 text-amber-200', slate: 'from-slate-400/10 text-slate-200' }[accent]
  return <div className={`rounded-2xl border border-white/[0.07] bg-gradient-to-br ${accentClass} to-transparent p-5 shadow-panel`}><div className="text-xs text-slate-500">{label}</div><div className="mt-3 text-2xl font-semibold tracking-tight text-current">{value}</div><div className="mt-1 text-[11px] text-slate-600">{detail}</div></div>
}

function DataTile({ label, value }: { label: string; value: ReactNode }) { return <div className="rounded-xl border border-white/5 bg-black/10 p-3"><div className="text-[10px] uppercase tracking-wider text-slate-600">{label}</div><div className="mt-2 truncate text-sm font-medium text-slate-300">{value ?? '—'}</div></div> }
function MiniMetric({ label, value }: { label: string; value: ReactNode }) { return <div><div className="text-lg font-semibold text-slate-200">{value}</div><div className="text-[10px] text-slate-600">{label}</div></div> }
function ResourceRow({ label, value }: { label: string; value: ReactNode }) { return <div className="flex items-center justify-between gap-4"><span className="text-xs text-slate-500">{label}</span><span className="font-mono text-xs text-slate-300">{value}</span></div> }

function Button({ children, variant = 'primary', className = '', ...props }: React.ButtonHTMLAttributes<HTMLButtonElement> & { variant?: 'primary' | 'ghost' }) {
  return <button {...props} className={`rounded-xl px-4 py-2 text-xs font-semibold transition disabled:cursor-not-allowed disabled:opacity-40 ${variant === 'primary' ? 'bg-cyan-300 text-slate-950 hover:bg-cyan-200' : 'border border-white/10 bg-white/[0.035] text-slate-300 hover:bg-white/[0.07]'} ${className}`}>{children}</button>
}

function SmallButton({ children, tone = 'normal', ...props }: React.ButtonHTMLAttributes<HTMLButtonElement> & { tone?: 'normal' | 'danger' }) {
  return <button {...props} className={`rounded-lg border px-2.5 py-1.5 text-[11px] transition ${tone === 'danger' ? 'border-rose-400/15 bg-rose-400/[0.04] text-rose-300 hover:bg-rose-400/10' : 'border-white/10 bg-white/[0.035] text-slate-300 hover:bg-white/[0.07]'}`}>{children}</button>
}

function Input({ className = '', ...props }: React.InputHTMLAttributes<HTMLInputElement>) { return <input {...props} className={`input-base ${className}`} /> }
function Field({ label, children }: { label: string; children: ReactNode }) { return <label className="block"><span className="mb-2 block text-[11px] font-medium text-slate-500">{label}</span>{children}</label> }
function CodeChip({ children }: { children: ReactNode }) { return <span className="rounded-md border border-white/5 bg-black/20 px-2 py-1 font-mono text-slate-400">{children}</span> }
function EmptyState({ children }: { children: ReactNode }) { return <div className="py-8 text-center text-xs text-slate-600">{children}</div> }
function Th({ children }: { children: ReactNode }) { return <th className="border-b border-white/5 px-3 py-3 font-medium">{children}</th> }
function Td({ children }: { children: ReactNode }) { return <td className="px-3 py-3 align-top">{children}</td> }

function PlatformBadge({ platform }: { platform: string }) { const web = platform === 'web'; return <span className={`shrink-0 rounded-md px-2 py-1 font-mono text-[9px] uppercase ${web ? 'bg-cyan-300/10 text-cyan-300' : platform === 'discord' ? 'bg-indigo-300/10 text-indigo-300' : 'bg-slate-300/10 text-slate-400'}`}>{platform}</span> }
function RoleBadge({ role }: { role: string }) { return <span className={`rounded-md px-2 py-1 font-mono text-[9px] uppercase ${role === 'assistant' ? 'bg-cyan-300/10 text-cyan-300' : role === 'user' ? 'bg-indigo-300/10 text-indigo-300' : role === 'tool' ? 'bg-amber-300/10 text-amber-300' : 'bg-slate-300/10 text-slate-400'}`}>{role}</span> }
function RunStatus({ value }: { value: string }) { const cls = value === 'completed' || value === 'success' ? 'text-emerald-300 bg-emerald-300/10' : value === 'failed' ? 'text-rose-300 bg-rose-300/10' : 'text-amber-300 bg-amber-300/10'; return <span className={`rounded-md px-2 py-1 text-[10px] ${cls}`}>{value}</span> }
function ConnectionBadge({ value }: { value: string }) { const cls = value === 'open' ? 'text-emerald-300' : value === 'error' ? 'text-rose-300' : 'text-amber-300'; return <span className={cls}>{value}</span> }

function Segmented({ value, options, onChange }: { value: string; options: Array<[string, string]>; onChange: (value: string) => void }) {
  return <div className="inline-flex rounded-xl border border-white/5 bg-black/15 p-1">{options.map(([id, label]) => <button key={id} onClick={() => onChange(id)} className={`rounded-lg px-4 py-2 text-xs transition ${value === id ? 'bg-white/[0.08] text-white' : 'text-slate-500 hover:text-slate-300'}`}>{label}</button>)}</div>
}

function Notice({ tone, title, children, compact = false }: { tone: 'danger' | 'warning'; title: string; children: ReactNode; compact?: boolean }) {
  const cls = tone === 'danger' ? 'border-rose-400/15 bg-rose-400/[0.045] text-rose-200' : 'border-amber-400/15 bg-amber-400/[0.045] text-amber-100'
  return <div className={`rounded-xl border ${cls} ${compact ? 'mt-4 p-3' : 'p-4'}`}><div className="text-xs font-semibold">{title}</div><div className="mt-1 text-xs leading-5 opacity-75">{children}</div></div>
}
