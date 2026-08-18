import React, { FormEvent, ReactNode, useCallback, useEffect, useMemo, useRef, useState } from 'react'
import ReactMarkdown from 'react-markdown'
import {
  Activity,
  AlertCircle,
  AlertTriangle,
  Bot,
  CheckCircle2,
  ChevronRight,
  Clock,
  Code2,
  Coins,
  Cpu,
  Database,
  ExternalLink,
  FileCode2,
  HardDrive,
  Layers,
  ListTodo,
  LogOut,
  MessageSquare,
  Pause,
  Play,
  Plus,
  RefreshCw,
  Search,
  Send,
  Server,
  Settings,
  Shield,
  ShieldAlert,
  Sparkles,
  Terminal,
  Trash2,
  User,
  Wrench,
  XCircle,
  Zap,
} from 'lucide-react'

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

import {
  Badge,
  Button,
  Card,
  CardContent,
  CardDescription,
  CardFooter,
  CardHeader,
  CardTitle,
  Dialog,
  Input,
  Separator,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
  Tabs,
  TabsContent,
  TabsList,
  TabsTrigger,
} from './components/ui'

import { BotsPage } from './pages/BotsPage'

type Page = 'overview' | 'bots' | 'chat' | 'cron' | 'sessions' | 'capabilities' | 'settings' | 'logs'

interface NavItem {
  id: Page
  label: string
  hint: string
  icon: React.ReactNode
}

const NAV_ITEMS: NavItem[] = [
  { id: 'overview', label: 'Overview', hint: 'System health & stats', icon: <Activity className="w-4 h-4" /> },
  { id: 'bots', label: 'Agent Bots', hint: 'Per-bot profiles & models', icon: <Bot className="w-4 h-4" /> },
  { id: 'chat', label: 'Playground', hint: 'Interactive Agent Chat', icon: <MessageSquare className="w-4 h-4" /> },
  { id: 'cron', label: 'Scheduled Jobs', hint: 'Cron tasks & history', icon: <Clock className="w-4 h-4" /> },
  { id: 'sessions', label: 'Sessions', hint: 'Active & past contexts', icon: <Layers className="w-4 h-4" /> },
  { id: 'capabilities', label: 'Skills & Tools', hint: 'Registered capabilities', icon: <Wrench className="w-4 h-4" /> },
  { id: 'settings', label: 'Security & Config', hint: 'Approvals & policies', icon: <Shield className="w-4 h-4" /> },
  { id: 'logs', label: 'Live Logs', hint: 'Real-time telemetry stream', icon: <Terminal className="w-4 h-4" /> },
]

function pageFromHash(): Page {
  const candidate = window.location.hash.replace(/^#\/?/, '') as Page
  return NAV_ITEMS.some((item) => item.id === candidate) ? candidate : 'overview'
}

export default function App() {
  const [page, setPage] = useState<Page>(pageFromHash)
  const [status, setStatus] = useState<StatusResponse | null>(null)
  const [statusError, setStatusError] = useState<string | null>(null)
  const [healthStatus, setHealthStatus] = useState<string>('ok')
  const [selectedSessionId, setSelectedSessionId] = useState<string | null>(null)

  const refreshStatus = useCallback(async () => {
    try {
      const data = await api.status()
      setStatus(data)
      setStatusError(null)
    } catch (err: any) {
      setStatusError(err.message || 'Failed to connect to gateway')
    }
    try {
      const h = await api.health()
      setHealthStatus(h.status || 'ok')
    } catch {
      setHealthStatus('unhealthy')
    }
  }, [])

  useEffect(() => {
    refreshStatus()
    const interval = setInterval(refreshStatus, 4000)
    return () => clearInterval(interval)
  }, [refreshStatus])

  useEffect(() => {
    const handleHash = () => setPage(pageFromHash())
    window.addEventListener('hashchange', handleHash)
    return () => window.removeEventListener('hashchange', handleHash)
  }, [])

  const selectPage = (next: Page, sessionId?: string) => {
    window.location.hash = `#/${next}`
    setPage(next)
    if (sessionId) {
      setSelectedSessionId(sessionId)
    }
  }

  return (
    <div className="flex h-screen w-screen overflow-hidden bg-background text-foreground font-sans">
      {/* Sidebar Navigation */}
      <aside className="flex flex-col w-64 border-r border-border bg-card/60 backdrop-blur-xl shrink-0 z-20">
        <div className="flex items-center gap-3 p-4 border-b border-border">
          <img
            src="/favicon-64.png"
            alt="omon logo"
            className="w-8 h-8 rounded-lg border border-primary/20 shadow-sm object-cover shrink-0"
          />
          <div className="flex flex-col min-w-0">
            <span className="font-semibold text-sm tracking-tight text-foreground flex items-center gap-1.5">
              omon gateway
              <Badge variant="outline" className="text-[10px] px-1.5 py-0 border-primary/30 text-primary bg-primary/5">
                v0.1
              </Badge>
            </span>
            <span className="text-[11px] text-muted-foreground font-medium truncate">
              Autonomous Agent Hub
            </span>
          </div>
        </div>

        <div className="flex-1 overflow-y-auto px-3 py-4 space-y-1">
          <div className="text-[11px] font-medium text-muted-foreground/70 px-2 pb-1 uppercase tracking-wider">
            Management
          </div>
          {NAV_ITEMS.map((item) => {
            const active = page === item.id
            return (
              <button
                key={item.id}
                onClick={() => selectPage(item.id)}
                className={`w-full flex items-center gap-3 px-3 py-2 rounded-md text-sm font-medium transition-all group ${
                  active
                    ? 'bg-primary text-primary-foreground shadow-sm'
                    : 'text-muted-foreground hover:text-foreground hover:bg-accent/60'
                }`}
              >
                <div className={`${active ? 'text-primary-foreground' : 'text-muted-foreground group-hover:text-foreground'}`}>
                  {item.icon}
                </div>
                <div className="flex flex-col text-left">
                  <span>{item.label}</span>
                </div>
              </button>
            )
          })}
        </div>

        {/* Footer Gateway Status Indicator */}
        <div className="p-3 m-3 rounded-lg border border-border/80 bg-background/50 flex flex-col gap-2">
          <div className="flex items-center justify-between">
            <span className="text-xs font-medium text-muted-foreground flex items-center gap-2">
              <span
                className={`w-2 h-2 rounded-full ${
                  statusError ? 'bg-destructive animate-pulse' : 'bg-emerald-500 shadow-[0_0_8px_rgba(16,185,129,0.5)]'
                }`}
              />
              {statusError ? 'Offline' : 'System Ready'}
            </span>
            <Badge variant={statusError ? 'destructive' : 'outline'} className="text-[10px] px-1.5">
              {status?.bot_connections ?? 0} bots
            </Badge>
          </div>
          <div className="text-[10px] text-muted-foreground font-mono flex items-center justify-between pt-1 border-t border-border/40">
            <span>Uptime: {status ? formatDuration(status.uptime_seconds) : '—'}</span>
            <span>{status?.database?.pool_idle ?? 0} conn</span>
          </div>
        </div>
      </aside>

      {/* Main Content Area */}
      <main className="flex-1 flex flex-col min-w-0 bg-background overflow-hidden">
        {/* Top Header Bar */}
        <header className="h-14 border-b border-border px-6 flex items-center justify-between bg-card/20 backdrop-blur-md shrink-0">
          <div className="flex items-center gap-2">
            <h1 className="text-base font-semibold tracking-tight text-foreground">
              {NAV_ITEMS.find((n) => n.id === page)?.label}
            </h1>
            <span className="text-muted-foreground/40">/</span>
            <span className="text-xs text-muted-foreground">
              {NAV_ITEMS.find((n) => n.id === page)?.hint}
            </span>
          </div>

          <div className="flex items-center gap-3">
            {status?.pending_approvals && status.pending_approvals > 0 ? (
              <Badge
                variant="destructive"
                className="cursor-pointer hover:opacity-90 animate-bounce"
                onClick={() => selectPage('settings')}
              >
                <ShieldAlert className="w-3 h-3 mr-1" />
                {status.pending_approvals} Pending Approvals
              </Badge>
            ) : null}
            <Button
              variant="outline"
              size="sm"
              className="h-8 gap-1.5 text-xs text-muted-foreground hover:text-foreground"
              onClick={refreshStatus}
            >
              <RefreshCw className="w-3.5 h-3.5" />
              Refresh
            </Button>
          </div>
        </header>

        {/* Dynamic Page Views */}
        <div className="flex-1 overflow-y-auto p-6">
          {page === 'overview' && <OverviewPage status={status} onNavigate={selectPage} />}
          {page === 'bots' && <BotsPage />}
          {page === 'chat' && (
            <ChatPlaygroundPage
              initialSessionId={selectedSessionId}
              onClearInitialSession={() => setSelectedSessionId(null)}
            />
          )}
          {page === 'cron' && <CronManagerPage />}
          {page === 'sessions' && (
            <SessionsExplorerPage
              onOpenChat={(id) => {
                selectPage('chat', id)
              }}
            />
          )}
          {page === 'capabilities' && <CapabilitiesPage />}
          {page === 'settings' && <SecuritySettingsPage onReloadStatus={refreshStatus} />}
          {page === 'logs' && <LiveLogsPage />}
        </div>
      </main>
    </div>
  )
}

/* =========================================================================
   1. OVERVIEW PAGE
   ========================================================================= */
function OverviewPage({ status, onNavigate }: { status: StatusResponse | null; onNavigate: (p: Page) => void }) {
  if (!status) {
    return (
      <div className="flex flex-col items-center justify-center h-64 space-y-4">
        <RefreshCw className="w-8 h-8 animate-spin text-muted-foreground" />
        <p className="text-sm text-muted-foreground font-medium">Connecting to omon gateway telemetry...</p>
      </div>
    )
  }

  const diskTotal = status.disk.workspace_total_bytes || 1
  const diskAvail = status.disk.workspace_available_bytes || 0
  const diskUsedPercent = Math.round(((diskTotal - diskAvail) / diskTotal) * 100)

  return (
    <div className="space-y-6 w-full">
      {/* Metric Cards Grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        <Card className="hover:border-primary/40 transition-colors">
          <CardHeader className="flex flex-row items-center justify-between pb-2">
            <CardTitle className="text-xs font-medium uppercase tracking-wider text-muted-foreground">Active Model</CardTitle>
            <Sparkles className="w-4 h-4 text-primary" />
          </CardHeader>
          <CardContent>
            <div className="text-xl font-bold font-mono text-foreground truncate">{status.model}</div>
            <p className="text-[11px] text-muted-foreground mt-1">
              Providers: OpenAI-compatible / Anthropic
            </p>
          </CardContent>
        </Card>

        <Card className="hover:border-primary/40 transition-colors">
          <CardHeader className="flex flex-row items-center justify-between pb-2">
            <CardTitle className="text-xs font-medium uppercase tracking-wider text-muted-foreground">Discord Shards</CardTitle>
            <Bot className="w-4 h-4 text-emerald-400" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold text-foreground">{status.bot_connections} Bots</div>
            <p className="text-[11px] text-emerald-400/90 mt-1 flex items-center gap-1">
              <CheckCircle2 className="w-3 h-3" /> Connected & Active
            </p>
          </CardContent>
        </Card>

        <Card className="hover:border-primary/40 transition-colors">
          <CardHeader className="flex flex-row items-center justify-between pb-2">
            <CardTitle className="text-xs font-medium uppercase tracking-wider text-muted-foreground">Active Sessions</CardTitle>
            <Layers className="w-4 h-4 text-sky-400" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold text-foreground">{status.database.sessions} Total</div>
            <p className="text-[11px] text-muted-foreground mt-1 font-mono">
              {status.database.messages} messages stored
            </p>
          </CardContent>
        </Card>

        <Card className="hover:border-primary/40 transition-colors">
          <CardHeader className="flex flex-row items-center justify-between pb-2">
            <CardTitle className="text-xs font-medium uppercase tracking-wider text-muted-foreground">Scheduled Work</CardTitle>
            <Clock className="w-4 h-4 text-amber-400" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold text-foreground">{status.database.cron_jobs} Jobs</div>
            <p className="text-[11px] text-muted-foreground mt-1">
              Hermes & Omon automated runners
            </p>
          </CardContent>
        </Card>
      </div>

      {/* System Resources & Fast Access */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        <Card className="lg:col-span-2">
          <CardHeader>
            <CardTitle className="text-sm font-semibold flex items-center gap-2">
              <Server className="w-4 h-4 text-primary" />
              Runtime Resources & Engine State
            </CardTitle>
            <CardDescription className="text-xs">
              Live hardware telemetry and database capacity
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <div>
              <div className="flex justify-between text-xs mb-1.5">
                <span className="text-muted-foreground flex items-center gap-1.5">
                  <HardDrive className="w-3.5 h-3.5" /> Workspace Disk Usage
                </span>
                <span className="font-mono font-medium">
                  {formatBytes(diskTotal - diskAvail)} / {formatBytes(diskTotal)} ({diskUsedPercent}%)
                </span>
              </div>
              <div className="w-full bg-secondary rounded-full h-2 overflow-hidden border border-border">
                <div
                  className={`h-full transition-all rounded-full ${
                    diskUsedPercent > 90 ? 'bg-destructive' : diskUsedPercent > 75 ? 'bg-amber-500' : 'bg-primary'
                  }`}
                  style={{ width: `${Math.min(100, diskUsedPercent)}%` }}
                />
              </div>
            </div>

            <div className="grid grid-cols-2 sm:grid-cols-3 gap-3 pt-2">
              <div className="p-3 rounded-lg border border-border bg-secondary/30">
                <div className="text-[11px] text-muted-foreground">Memory RSS</div>
                <div className="text-sm font-mono font-semibold mt-0.5">{formatBytes(status.memory.process_bytes)}</div>
              </div>
              <div className="p-3 rounded-lg border border-border bg-secondary/30">
                <div className="text-[11px] text-muted-foreground">Database Size</div>
                <div className="text-sm font-mono font-semibold mt-0.5">{status.database.pool_size} Pool Conn</div>
              </div>
              <div className="p-3 rounded-lg border border-border bg-secondary/30">
                <div className="text-[11px] text-muted-foreground">Long-Term Memory</div>
                <div className="text-sm font-mono font-semibold mt-0.5">{status.database.memories} Records</div>
              </div>
            </div>
          </CardContent>
        </Card>

        {/* Quick Launch Cards */}
        <Card className="flex flex-col justify-between">
          <CardHeader>
            <CardTitle className="text-sm font-semibold flex items-center gap-2">
              <Zap className="w-4 h-4 text-amber-400" />
              Quick Actions
            </CardTitle>
            <CardDescription className="text-xs">
              Fast navigation and operations
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-2">
            <Button
              variant="outline"
              className="w-full justify-between h-10 text-xs border-border bg-card/50 hover:bg-accent"
              onClick={() => onNavigate('chat')}
            >
              <span className="flex items-center gap-2">
                <MessageSquare className="w-3.5 h-3.5 text-primary" /> Open Web Chat Console
              </span>
              <ChevronRight className="w-3.5 h-3.5 text-muted-foreground" />
            </Button>
            <Button
              variant="outline"
              className="w-full justify-between h-10 text-xs border-border bg-card/50 hover:bg-accent"
              onClick={() => onNavigate('cron')}
            >
              <span className="flex items-center gap-2">
                <Clock className="w-3.5 h-3.5 text-amber-400" /> Manage Scheduled Cron Tasks
              </span>
              <ChevronRight className="w-3.5 h-3.5 text-muted-foreground" />
            </Button>
            <Button
              variant="outline"
              className="w-full justify-between h-10 text-xs border-border bg-card/50 hover:bg-accent"
              onClick={() => onNavigate('settings')}
            >
              <span className="flex items-center gap-2">
                <Shield className="w-3.5 h-3.5 text-emerald-400" /> Security & Approvals
              </span>
              <ChevronRight className="w-3.5 h-3.5 text-muted-foreground" />
            </Button>
          </CardContent>
          <CardFooter className="pt-0">
            <p className="text-[11px] text-muted-foreground">
              Zero-GC async AI gateway running in pure Rust
            </p>
          </CardFooter>
        </Card>
      </div>
    </div>
  )
}

/* =========================================================================
   2. CHAT PLAYGROUND PAGE
   ========================================================================= */
function ChatPlaygroundPage({
  initialSessionId,
  onClearInitialSession,
}: {
  initialSessionId?: string | null
  onClearInitialSession?: () => void
}) {
  const [sessions, setSessions] = useState<SessionRecord[]>([])
  const [currentSessionId, setCurrentSessionId] = useState<string>(initialSessionId || 'web-default')
  const [messages, setMessages] = useState<SessionMessage[]>([])
  const [input, setInput] = useState('')
  const [streamingContent, setStreamingContent] = useState('')
  const [isSending, setIsSending] = useState(false)
  const messagesEndRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    if (initialSessionId) {
      setCurrentSessionId(initialSessionId)
      onClearInitialSession?.()
    }
  }, [initialSessionId, onClearInitialSession])

  const loadSessions = useCallback(async () => {
    try {
      const data = await api.sessions()
      setSessions(data.items || [])
    } catch {}
  }, [])

  const loadMessages = useCallback(async (id: string) => {
    if (!id || id === 'web-default' || id.startsWith('web-')) {
      // Ephemeral web sessions start clean unless saved
      try {
        const data = await api.sessionMessages(id)
        setMessages(data.items || [])
      } catch {
        setMessages([])
      }
      return
    }
    try {
      const data = await api.sessionMessages(id)
      setMessages(data.items || [])
    } catch {
      setMessages([])
    }
  }, [])

  useEffect(() => {
    loadSessions()
  }, [loadSessions])

  useEffect(() => {
    if (currentSessionId) {
      loadMessages(currentSessionId)
    }
  }, [currentSessionId, loadMessages])

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [messages, streamingContent])

  const handleSendMessage = async (e: FormEvent) => {
    e.preventDefault()
    if (!input.trim() || isSending) return

    const userText = input.trim()
    setInput('')
    setIsSending(true)
    setStreamingContent('')

    // Append temporary optimistic message
    const tempUserMsg: SessionMessage = {
      id: `temp-${Date.now()}`,
      sequence: messages.length + 1,
      role: 'user',
      content: userText,
      created_at: new Date().toISOString(),
    }
    setMessages((prev) => [...prev, tempUserMsg])

    try {
      // Connect WebSocket for streaming response
      const ws = new WebSocket(socketUrl(`/api/sessions/${encodeURIComponent(currentSessionId)}/ws`))
      let accumulatedResponse = ''

      ws.onopen = () => {
        ws.send(JSON.stringify({ type: 'chat', content: userText }))
      }

      ws.onmessage = (event) => {
        try {
          const data = JSON.parse(event.data)
          if (data.type === 'stream_chunk') {
            accumulatedResponse = data.content
            setStreamingContent(accumulatedResponse)
          } else if (data.type === 'final') {
            accumulatedResponse = data.content
            setStreamingContent('')
            loadMessages(currentSessionId)
            setIsSending(false)
            ws.close()
          } else if (data.type === 'error') {
            setIsSending(false)
            setStreamingContent('')
            alert(`Error: ${data.message}`)
            ws.close()
          }
        } catch {
          accumulatedResponse += event.data
          setStreamingContent(accumulatedResponse)
        }
      }

      ws.onerror = async () => {
        // Fallback to HTTP POST
        try {
          await api.postChat(currentSessionId, userText)
          setTimeout(() => {
            loadMessages(currentSessionId)
            setIsSending(false)
          }, 1500)
        } catch (err: any) {
          setIsSending(false)
          alert(`Chat error: ${err.message}`)
        }
      }
    } catch {
      setIsSending(false)
    }
  }

  const handleStopTurn = async () => {
    try {
      const ws = new WebSocket(socketUrl(`/api/sessions/${encodeURIComponent(currentSessionId)}/ws`))
      ws.onopen = () => {
        ws.send(JSON.stringify({ type: 'stop' }))
        setTimeout(() => ws.close(), 500)
      }
      setIsSending(false)
      setStreamingContent('')
    } catch {}
  }

  return (
    <div className="flex h-full gap-4 w-full">
      {/* Session selector sidebar */}
      <Card className="w-72 flex flex-col shrink-0 border-border bg-card/40">
        <CardHeader className="p-3 border-b border-border">
          <div className="flex items-center justify-between">
            <CardTitle className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">
              Conversations
            </CardTitle>
            <Button
              variant="ghost"
              size="icon"
              className="h-7 w-7"
              onClick={() => {
                const newId = `web-${Date.now().toString(36)}`
                setCurrentSessionId(newId)
              }}
              title="New Session"
            >
              <Plus className="w-4 h-4" />
            </Button>
          </div>
        </CardHeader>
        <CardContent className="flex-1 overflow-y-auto p-2 space-y-1">
          <button
            onClick={() => setCurrentSessionId('web-default')}
            className={`w-full text-left px-3 py-2 rounded-md text-xs font-medium transition-colors ${
              currentSessionId === 'web-default'
                ? 'bg-secondary text-foreground font-semibold'
                : 'text-muted-foreground hover:bg-accent/40'
            }`}
          >
            <div className="flex items-center gap-2">
              <Sparkles className="w-3.5 h-3.5 text-primary" />
              <span>Default Playground</span>
            </div>
          </button>
          {sessions.map((s) => {
            const label = s.id.includes('|')
              ? s.id.split('|').slice(1).filter(p => !p.startsWith('-')).pop() || s.id
              : s.id
            return (
              <button
                key={s.id}
                onClick={() => setCurrentSessionId(s.id)}
                className={`w-full text-left px-3 py-2 rounded-md text-xs font-medium transition-colors truncate ${
                  currentSessionId === s.id
                    ? 'bg-secondary text-foreground font-semibold'
                    : 'text-muted-foreground hover:bg-accent/40'
                }`}
              >
                <div className="flex items-center justify-between">
                  <span className="truncate font-mono">{label}</span>
                  <Badge variant="outline" className="text-[9px] px-1 py-0 uppercase">
                    {s.platform}
                  </Badge>
                </div>
              </button>
            )
          })}
        </CardContent>
      </Card>

      {/* Chat Messages Panel */}
      <Card className="flex-1 flex flex-col border-border bg-card/60">
        <CardHeader className="p-3.5 border-b border-border flex flex-row items-center justify-between">
          <div className="flex items-center gap-2">
            <MessageSquare className="w-4 h-4 text-primary" />
            <span className="text-xs font-mono font-semibold text-foreground truncate max-w-md">
              {currentSessionId}
            </span>
          </div>
          <div className="flex items-center gap-2">
            <Button
              variant="outline"
              size="sm"
              className="h-7 text-xs text-muted-foreground"
              onClick={() => loadMessages(currentSessionId)}
            >
              <RefreshCw className="w-3 h-3 mr-1" /> Reload
            </Button>
            {isSending ? (
              <Button variant="destructive" size="sm" className="h-7 text-xs" onClick={handleStopTurn}>
                <Pause className="w-3 h-3 mr-1" /> Stop
              </Button>
            ) : null}
          </div>
        </CardHeader>

        {/* Message Feed */}
        <CardContent className="flex-1 overflow-y-auto p-4 space-y-4 font-sans text-sm">
          {messages.length === 0 && !streamingContent ? (
            <div className="flex flex-col items-center justify-center h-full text-center text-muted-foreground space-y-2">
              <Bot className="w-10 h-10 text-muted-foreground/40" />
              <p className="text-sm font-medium">Session initialized and ready</p>
              <p className="text-xs max-w-sm text-muted-foreground/70">
                Send a prompt below to interact directly with the agent multiplexer and tools.
              </p>
            </div>
          ) : null}

          {messages.map((m) => {
            const isUser = m.role === 'user'
            const isTool = m.role === 'tool'

            // Parse tool calls in assistant message metadata if content is empty
            let toolCallsText = ''
            if (!isUser && !isTool && !m.content?.trim()) {
              try {
                const meta = m.metadata
                if (meta && Array.isArray((meta as any).tool_calls)) {
                  const names = (meta as any).tool_calls.map((tc: any) => tc.name || tc.function?.name).filter(Boolean)
                  if (names.length > 0) {
                    toolCallsText = `⚙️ Executing tools: ${names.join(', ')}`
                  }
                }
              } catch {}
            }

            // Parse image links or attachments in content
            const contentStr = m.content || ''
            const imageRegex = /(https?:\/\/[^\s)]+\.(?:png|jpg|jpeg|gif|webp)(?:\?[^\s)]*)?)/gi
            const imageMatches = [...contentStr.matchAll(imageRegex)].map((match) => match[1])

            // If empty message and no tool calls, skip rendering empty block
            if (!isUser && !isTool && !contentStr.trim() && !toolCallsText) {
              return null
            }

            return (
              <div
                key={m.id}
                className={`flex gap-3 ${isUser ? 'justify-end' : 'justify-start'}`}
              >
                {!isUser ? (
                  <div className="w-7 h-7 rounded-full bg-primary/10 border border-primary/20 flex items-center justify-center shrink-0 text-primary mt-1">
                    {isTool ? <Terminal className="w-3.5 h-3.5" /> : <Bot className="w-3.5 h-3.5" />}
                  </div>
                ) : null}

                <div
                  className={`max-w-2xl rounded-lg px-4 py-3 text-sm shadow-sm leading-relaxed ${
                    isUser
                      ? 'bg-primary text-primary-foreground font-medium'
                      : isTool
                      ? 'bg-secondary/80 border border-border font-mono text-xs text-muted-foreground'
                      : 'bg-card border border-border text-foreground'
                  }`}
                >
                  {isTool ? (
                    (() => {
                      try {
                        const parsed = JSON.parse(m.content)
                        if (parsed.stdout || parsed.stderr || parsed.content) {
                          const text = parsed.stdout || parsed.content || parsed.stderr
                          return <div className="whitespace-pre-wrap break-words">{text}</div>
                        }
                        return <pre className="whitespace-pre-wrap break-words">{JSON.stringify(parsed, null, 2)}</pre>
                      } catch {
                        return <div className="whitespace-pre-wrap break-words">{m.content}</div>
                      }
                    })()
                  ) : toolCallsText ? (
                    <div className="text-xs font-mono text-muted-foreground flex items-center gap-1.5 italic">
                      {toolCallsText}
                    </div>
                  ) : (
                    <div className="prose prose-invert max-w-none text-sm leading-relaxed break-words space-y-2">
                      <ReactMarkdown
                        components={{
                          img: ({ node, ...props }) => (
                            <img
                              {...props}
                              className="max-w-full rounded-md border border-border/80 my-2 shadow-sm object-contain max-h-96"
                              loading="lazy"
                            />
                          ),
                          p: ({ children }) => <p className="mb-2 last:mb-0 whitespace-pre-wrap">{children}</p>,
                          pre: ({ children }) => (
                            <pre className="p-3 my-2 rounded-md bg-black/60 border border-border text-xs font-mono overflow-x-auto whitespace-pre-wrap break-words text-slate-200">
                              {children}
                            </pre>
                          ),
                          code: ({ node, inline, children, ...props }: any) => {
                            if (inline) {
                              return (
                                <code className="px-1.5 py-0.5 rounded bg-secondary/80 text-primary font-mono text-[12px]" {...props}>
                                  {children}
                                </code>
                              )
                            }
                            return <code {...props}>{children}</code>
                          },
                        }}
                      >
                        {m.content}
                      </ReactMarkdown>

                      {/* Display extracted direct image links inline */}
                      {imageMatches.length > 0 && !m.content.includes('![') ? (
                        <div className="grid grid-cols-1 gap-2 pt-2 border-t border-border/40">
                          {imageMatches.map((imgUrl, i) => (
                            <a key={i} href={imgUrl} target="_blank" rel="noreferrer" className="block">
                              <img
                                src={imgUrl}
                                alt="Message attachment"
                                className="max-w-full rounded-md border border-border/80 shadow-sm object-contain max-h-96 hover:opacity-95 transition-opacity"
                                loading="lazy"
                              />
                            </a>
                          ))}
                        </div>
                      ) : null}
                    </div>
                  )}
                </div>

                {isUser ? (
                  <div className="w-7 h-7 rounded-full bg-secondary flex items-center justify-center shrink-0 text-muted-foreground mt-1">
                    <User className="w-3.5 h-3.5" />
                  </div>
                ) : null}
              </div>
            )
          })}

          {/* Progressive Streaming Bubble */}
          {streamingContent ? (
            <div className="flex gap-3 justify-start">
              <div className="w-7 h-7 rounded-full bg-primary/10 border border-primary/20 flex items-center justify-center shrink-0 text-primary mt-1 animate-pulse">
                <Bot className="w-3.5 h-3.5" />
              </div>
              <div className="max-w-2xl rounded-lg px-4 py-3 text-sm bg-card border border-primary/30 text-foreground shadow-sm">
                <ReactMarkdown>{streamingContent}</ReactMarkdown>
                <span className="inline-block w-2 h-4 ml-1 bg-primary animate-pulse align-middle" />
              </div>
            </div>
          ) : null}

          <div ref={messagesEndRef} />
        </CardContent>

        {/* Input Bar */}
        <CardFooter className="p-3 border-t border-border bg-card/30">
          <form onSubmit={handleSendMessage} className="flex w-full gap-2">
            <Input
              value={input}
              onChange={(e) => setInput(e.target.value)}
              placeholder={isSending ? 'Agent is thinking & executing tools...' : 'Send message to agent...'}
              disabled={isSending}
              className="flex-1 bg-background"
            />
            <Button type="submit" size="default" disabled={isSending || !input.trim()} className="gap-1.5">
              {isSending ? <RefreshCw className="w-4 h-4 animate-spin" /> : <Send className="w-4 h-4" />}
              Send
            </Button>
          </form>
        </CardFooter>
      </Card>
    </div>
  )
}

/* =========================================================================
   3. CRON MANAGER PAGE
   ========================================================================= */
function CronManagerPage() {
  const [jobs, setJobs] = useState<CronJob[]>([])
  const [runs, setRuns] = useState<CronRun[]>([])
  const [loading, setLoading] = useState(false)
  const [dialogOpen, setDialogOpen] = useState(false)
  const [newJobId, setNewJobId] = useState('')
  const [newSchedule, setNewSchedule] = useState('')
  const [newPrompt, setNewPrompt] = useState('')

  const loadData = useCallback(async () => {
    setLoading(true)
    try {
      const [j, r] = await Promise.all([api.cronJobs(), api.cronRuns()])
      setJobs(j.items || [])
      setRuns(r.items || [])
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    loadData()
  }, [loadData])

  const handleTrigger = async (id: string) => {
    try {
      await api.triggerCronJob(id)
      setTimeout(loadData, 1000)
    } catch (err: any) {
      alert(`Trigger failed: ${err.message}`)
    }
  }

  const handleToggle = async (job: CronJob) => {
    try {
      if (job.enabled) {
        await api.pauseCronJob(job.id)
      } else {
        await api.resumeCronJob(job.id)
      }
      loadData()
    } catch (err: any) {
      alert(`Failed to update status: ${err.message}`)
    }
  }

  const handleDelete = async (id: string) => {
    if (!confirm(`Delete cron job "${id}"?`)) return
    try {
      await api.deleteCronJob(id)
      loadData()
    } catch (err: any) {
      alert(`Delete failed: ${err.message}`)
    }
  }

  const handleCreate = async (e: FormEvent) => {
    e.preventDefault()
    if (!newJobId || !newSchedule) return
    try {
      await api.createCronJob({
        id: newJobId,
        expression: newSchedule,
        payload: {
          prompt: newPrompt,
          name: newJobId,
        },
      })
      setDialogOpen(false)
      setNewJobId('')
      setNewSchedule('')
      setNewPrompt('')
      loadData()
    } catch (err: any) {
      alert(`Create failed: ${err.message}`)
    }
  }

  return (
    <div className="space-y-6 w-full">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-lg font-semibold tracking-tight">Scheduled Cron Automations</h2>
          <p className="text-xs text-muted-foreground">
            Manage autonomous Hermes & Omon background pipelines
          </p>
        </div>
        <Button size="sm" onClick={() => setDialogOpen(true)} className="gap-1.5">
          <Plus className="w-4 h-4" /> New Scheduled Job
        </Button>
      </div>

      <Tabs value="jobs" onValueChange={() => {}}>
        <Card>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Job Identifier</TableHead>
                <TableHead>Schedule Expression</TableHead>
                <TableHead>Next Execution</TableHead>
                <TableHead>State</TableHead>
                <TableHead className="text-right">Actions</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {jobs.map((job) => (
                <TableRow key={job.id}>
                  <TableCell className="font-mono text-xs font-semibold text-foreground">
                    {job.id}
                  </TableCell>
                  <TableCell className="font-mono text-xs text-muted-foreground">
                    <Badge variant="outline" className="font-mono">
                      {job.expression}
                    </Badge>
                  </TableCell>
                  <TableCell className="text-xs text-muted-foreground">
                    {job.next_run_at ? formatDate(job.next_run_at) : '—'}
                  </TableCell>
                  <TableCell>
                    <Badge variant={job.enabled ? 'success' : 'secondary'}>
                      {job.enabled ? 'Enabled' : 'Paused'}
                    </Badge>
                  </TableCell>
                  <TableCell className="text-right space-x-1.5">
                    <Button
                      variant="outline"
                      size="sm"
                      className="h-7 text-xs"
                      onClick={() => handleTrigger(job.id)}
                      title="Run immediately"
                    >
                      <Play className="w-3 h-3 mr-1 text-emerald-400" /> Run Now
                    </Button>
                    <Button
                      variant="ghost"
                      size="sm"
                      className="h-7 text-xs"
                      onClick={() => handleToggle(job)}
                    >
                      {job.enabled ? <Pause className="w-3 h-3 text-amber-400" /> : <Play className="w-3 h-3" />}
                    </Button>
                    <Button
                      variant="ghost"
                      size="icon"
                      className="h-7 w-7 text-destructive hover:bg-destructive/10"
                      onClick={() => handleDelete(job.id)}
                    >
                      <Trash2 className="w-3.5 h-3.5" />
                    </Button>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </Card>
      </Tabs>

      {/* Execution Logs Table */}
      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="text-sm font-semibold flex items-center gap-2">
            <Clock className="w-4 h-4 text-primary" />
            Recent Execution History
          </CardTitle>
        </CardHeader>
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Started At</TableHead>
              <TableHead>Job</TableHead>
              <TableHead>Status</TableHead>
              <TableHead>Outcome Details</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {runs.slice(0, 10).map((r) => (
              <TableRow key={r.run_id}>
                <TableCell className="text-xs text-muted-foreground whitespace-nowrap">
                  {formatDate(r.started_at)}
                </TableCell>
                <TableCell className="font-mono text-xs font-medium">{r.job_id}</TableCell>
                <TableCell>
                  <Badge variant={r.status === 'succeeded' ? 'success' : 'destructive'} className="capitalize text-[11px]">
                    {r.status}
                  </Badge>
                </TableCell>
                <TableCell className="text-xs text-muted-foreground truncate max-w-md">
                  {r.error ? (
                    <span className="text-destructive font-mono">{r.error}</span>
                  ) : (
                    <span className="text-muted-foreground/70">Completed cleanly</span>
                  )}
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </Card>

      {/* Create Dialog */}
      <Dialog open={dialogOpen} onOpenChange={setDialogOpen}>
        <form onSubmit={handleCreate} className="space-y-4">
          <h3 className="text-lg font-semibold tracking-tight">Create Scheduled Cron Task</h3>
          <div className="space-y-2">
            <label className="text-xs font-medium text-muted-foreground">Job ID</label>
            <Input
              value={newJobId}
              onChange={(e) => setNewJobId(e.target.value)}
              placeholder="e.g. daily-summary"
              required
            />
          </div>
          <div className="space-y-2">
            <label className="text-xs font-medium text-muted-foreground">Cron Expression / Schedule</label>
            <Input
              value={newSchedule}
              onChange={(e) => setNewSchedule(e.target.value)}
              placeholder="e.g. 0 9 * * * or every 30m"
              required
            />
          </div>
          <div className="space-y-2">
            <label className="text-xs font-medium text-muted-foreground">Agent Prompt Task</label>
            <Input
              value={newPrompt}
              onChange={(e) => setNewPrompt(e.target.value)}
              placeholder="Summarize recent market news..."
            />
          </div>
          <div className="flex justify-end gap-2 pt-2">
            <Button type="button" variant="ghost" onClick={() => setDialogOpen(false)}>
              Cancel
            </Button>
            <Button type="submit">Create Task</Button>
          </div>
        </form>
      </Dialog>
    </div>
  )
}

/* =========================================================================
   4. SESSIONS EXPLORER PAGE
   ========================================================================= */
function SessionsExplorerPage({ onOpenChat }: { onOpenChat: (id: string) => void }) {
  const [sessions, setSessions] = useState<SessionRecord[]>([])
  const [search, setSearch] = useState('')

  const load = useCallback(async () => {
    try {
      const res = await api.sessions(search)
      setSessions(res.items || [])
    } catch {}
  }, [search])

  useEffect(() => {
    load()
  }, [load])

  return (
    <div className="space-y-6 w-full">
      <div className="flex items-center justify-between gap-4">
        <div>
          <h2 className="text-lg font-semibold tracking-tight">Session Explorer</h2>
          <p className="text-xs text-muted-foreground">
            Browse and inspect agent context windows across Discord and Web
          </p>
        </div>
        <div className="w-72">
          <Input
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            placeholder="Search sessions..."
            className="h-8 text-xs"
          />
        </div>
      </div>

      <Card>
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Session ID</TableHead>
              <TableHead>Platform</TableHead>
              <TableHead>Channel ID</TableHead>
              <TableHead>Last Updated</TableHead>
              <TableHead className="text-right">Actions</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {sessions.map((s) => (
              <TableRow key={s.id}>
                <TableCell className="font-mono text-xs font-semibold text-foreground">
                  {s.id}
                </TableCell>
                <TableCell>
                  <Badge variant="outline" className="capitalize">
                    {s.platform}
                  </Badge>
                </TableCell>
                <TableCell className="font-mono text-xs text-muted-foreground">
                  {s.channel_id || '—'}
                </TableCell>
                <TableCell className="text-xs text-muted-foreground">
                  {formatDate(s.updated_at)}
                </TableCell>
                <TableCell className="text-right">
                  <Button
                    variant="outline"
                    size="sm"
                    className="h-7 text-xs"
                    onClick={() => onOpenChat(s.id)}
                  >
                    Open in Chat
                  </Button>
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </Card>
    </div>
  )
}

/* =========================================================================
   5. CAPABILITIES PAGE (Skills & Tools)
   ========================================================================= */
function CapabilitiesPage() {
  const [tools, setTools] = useState<ToolRecord[]>([])
  const [skills, setSkills] = useState<SkillRecord[]>([])

  useEffect(() => {
    api.tools().then((t) => setTools(t.items || []))
    api.skills().then((s) => setSkills(s.items || []))
  }, [])

  return (
    <div className="space-y-6 w-full">
      <div>
        <h2 className="text-lg font-semibold tracking-tight">Agent Capabilities</h2>
        <p className="text-xs text-muted-foreground">
          Tools, skills, and execution surfaces available to OMON Agent
        </p>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        {/* Tools Section */}
        <Card>
          <CardHeader>
            <CardTitle className="text-sm font-semibold flex items-center gap-2">
              <Wrench className="w-4 h-4 text-primary" /> Registered Tool Surfaces ({tools.length})
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-3">
            {tools.map((tool) => (
              <div key={tool.name} className="p-3 rounded-lg border border-border bg-secondary/20 space-y-1">
                <div className="flex items-center justify-between">
                  <span className="font-mono text-xs font-semibold text-foreground">{tool.name}</span>
                  <Badge variant="outline" className="text-[10px]">Tool</Badge>
                </div>
                <p className="text-xs text-muted-foreground">{tool.description}</p>
              </div>
            ))}
          </CardContent>
        </Card>

        {/* Skills Section */}
        <Card>
          <CardHeader>
            <CardTitle className="text-sm font-semibold flex items-center gap-2">
              <Sparkles className="w-4 h-4 text-amber-400" /> Discovered Skills ({skills.length})
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-3 max-h-[520px] overflow-y-auto">
            {skills.map((skill) => (
              <div key={skill.name} className="p-3 rounded-lg border border-border bg-secondary/20 space-y-1">
                <div className="flex items-center justify-between">
                  <span className="font-mono text-xs font-semibold text-foreground">{skill.name}</span>
                  <Badge variant="secondary" className="text-[10px] capitalize">{skill.source}</Badge>
                </div>
                <p className="text-xs text-muted-foreground">{skill.description || 'Skill definition file loaded'}</p>
                <div className="text-[10px] text-muted-foreground/60 font-mono truncate">{skill.path}</div>
              </div>
            ))}
          </CardContent>
        </Card>
      </div>
    </div>
  )
}

/* =========================================================================
   6. SECURITY & SETTINGS PAGE
   ========================================================================= */
function SecuritySettingsPage({ onReloadStatus }: { onReloadStatus: () => void }) {
  const [approvals, setApprovals] = useState<PendingApproval[]>([])
  const [allowlist, setAllowlist] = useState<AllowlistRecord[]>([])
  const [config, setConfig] = useState<JsonObject | null>(null)

  const load = useCallback(async () => {
    try {
      const [app, all, cfg] = await Promise.all([
        api.pendingApprovals(),
        api.approvalAllowlist(),
        api.config(),
      ])
      setApprovals(app.items || [])
      setAllowlist(all.items || [])
      setConfig(cfg)
    } catch {}
  }, [])

  useEffect(() => {
    load()
  }, [load])

  const handleResolve = async (id: string, decision: 'Once' | 'Session' | 'Always' | 'Deny') => {
    try {
      await api.resolveApproval(id, decision)
      load()
      onReloadStatus()
    } catch (err: any) {
      alert(`Approval error: ${err.message}`)
    }
  }

  return (
    <div className="space-y-6 w-full">
      <div>
        <h2 className="text-lg font-semibold tracking-tight">Security & Terminal Approvals</h2>
        <p className="text-xs text-muted-foreground">
          Review pending dangerous-command executions and configure safety policies
        </p>
      </div>

      {/* Pending Approvals */}
      <Card className="border-amber-500/30">
        <CardHeader className="pb-3">
          <CardTitle className="text-sm font-semibold flex items-center gap-2 text-amber-400">
            <ShieldAlert className="w-4 h-4" />
            Pending Dangerous-Command Approvals ({approvals.length})
          </CardTitle>
          <CardDescription className="text-xs">
            Commands requiring operator clearance before executing on the host
          </CardDescription>
        </CardHeader>
        <CardContent>
          {approvals.length === 0 ? (
            <p className="text-xs text-muted-foreground py-4 text-center">
              No pending execution approvals. All systems operating safely.
            </p>
          ) : (
            <div className="space-y-3">
              {approvals.map((a) => (
                <div key={a.id} className="p-4 rounded-lg border border-border bg-secondary/30 space-y-3">
                  <div className="flex items-center justify-between">
                    <span className="text-xs font-mono font-semibold text-foreground">{a.session_id}</span>
                    <Badge variant="destructive">Approval Required</Badge>
                  </div>
                  <pre className="p-3 rounded-md bg-background/80 border border-border text-xs font-mono overflow-x-auto text-amber-300">
                    {a.command}
                  </pre>
                  <div className="flex gap-2 justify-end">
                    <Button variant="destructive" size="sm" onClick={() => handleResolve(a.id, 'Deny')}>
                      Deny
                    </Button>
                    <Button variant="outline" size="sm" onClick={() => handleResolve(a.id, 'Once')}>
                      Allow Once
                    </Button>
                    <Button variant="secondary" size="sm" onClick={() => handleResolve(a.id, 'Session')}>
                      Allow Session
                    </Button>
                    <Button variant="default" size="sm" onClick={() => handleResolve(a.id, 'Always')}>
                      Always Allow
                    </Button>
                  </div>
                </div>
              ))}
            </div>
          )}
        </CardContent>
      </Card>

      {/* Allowlist & Configuration View */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        <Card>
          <CardHeader>
            <CardTitle className="text-sm font-semibold flex items-center gap-2">
              <Shield className="w-4 h-4 text-emerald-400" />
              Persistent Allowlist Patterns ({allowlist.length})
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-2">
            {allowlist.length === 0 ? (
              <p className="text-xs text-muted-foreground">No permanent allowlist rules stored.</p>
            ) : (
              allowlist.map((al) => (
                <div key={al.pattern_key} className="p-2 rounded border border-border bg-secondary/20 font-mono text-xs">
                  {al.pattern_key}
                </div>
              ))
            )}
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle className="text-sm font-semibold flex items-center gap-2">
              <Settings className="w-4 h-4 text-primary" /> Active Configuration Dump
            </CardTitle>
          </CardHeader>
          <CardContent>
            <pre className="p-3 rounded-md bg-background border border-border text-[11px] font-mono text-muted-foreground overflow-x-auto max-h-64">
              {config ? JSON.stringify(config, null, 2) : 'Loading...'}
            </pre>
          </CardContent>
        </Card>
      </div>
    </div>
  )
}

/* =========================================================================
   7. LIVE LOGS PAGE
   ========================================================================= */
function LiveLogsPage() {
  const [logs, setLogs] = useState<LogEntry[]>([])
  const [connected, setConnected] = useState(false)
  const logsEndRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    api.logs().then((l) => setLogs(l.items || []))

    const ws = new WebSocket(socketUrl('/api/logs/ws'))
    ws.onopen = () => setConnected(true)
    ws.onclose = () => setConnected(false)
    ws.onmessage = (event) => {
      try {
        const entry = JSON.parse(event.data)
        setLogs((prev) => [...prev.slice(-400), entry])
      } catch {}
    }

    return () => ws.close()
  }, [])

  useEffect(() => {
    logsEndRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [logs])

  return (
    <div className="space-y-4 w-full h-full flex flex-col">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-lg font-semibold tracking-tight">Live Telemetry & Logs</h2>
          <p className="text-xs text-muted-foreground">
            Streaming runtime events, tool dispatches, and diagnostics
          </p>
        </div>
        <Badge variant={connected ? 'success' : 'secondary'} className="gap-1 text-xs">
          <span className={`w-1.5 h-1.5 rounded-full ${connected ? 'bg-emerald-400' : 'bg-muted'}`} />
          {connected ? 'WebSocket Live' : 'Disconnected'}
        </Badge>
      </div>

      <Card className="flex-1 overflow-hidden flex flex-col bg-black/80 border-border">
        <div className="flex-1 overflow-y-auto p-4 font-mono text-[11px] leading-relaxed space-y-1">
          {logs.map((log, idx) => {
            const timeStr = log.timestamp && typeof log.timestamp === 'string' && log.timestamp.length >= 19
              ? log.timestamp.slice(11, 19)
              : (log.timestamp || '—')
            return (
              <div key={log.id ?? idx} className="flex gap-2 items-start hover:bg-white/5 py-0.5 px-1 rounded">
                <span className="text-muted-foreground/50 shrink-0">{timeStr}</span>
                <span
                  className={`uppercase font-bold text-[9px] px-1 rounded shrink-0 ${
                    log.level === 'ERROR'
                      ? 'bg-destructive text-destructive-foreground'
                      : log.level === 'WARN'
                      ? 'bg-amber-500/20 text-amber-300'
                      : 'text-primary/70'
                  }`}
                >
                  {log.level || 'INFO'}
                </span>
                <span className="text-muted-foreground/80 shrink-0">[{log.target || 'system'}]</span>
                <span className="text-slate-200 flex-1 break-all">{log.message}</span>
              </div>
            )
          })}
          <div ref={logsEndRef} />
        </div>
      </Card>
    </div>
  )
}
