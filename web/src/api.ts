export type JsonObject = Record<string, unknown>

export interface ApiList<T> {
  items: T[]
  total?: number
  page?: number
  per_page?: number
}

export interface StatusResponse {
  status: string
  model?: string
  uptime_seconds: number
  bot_connections: number
  active_sessions: number
  chat_available: boolean
  pending_approvals: number
  time: string
  database: {
    sessions: number
    messages: number
    cron_jobs: number
    memories: number
    pool_size: number
    pool_idle: number
  }
  memory: { process_bytes: number | null }
  disk: {
    workspace_total_bytes: number | null
    workspace_available_bytes: number | null
  }
}

export interface SessionRecord {
  id: string
  platform: string
  guild_id?: string | null
  channel_id: string
  thread_id?: string | null
  user_id: string
  state?: JsonObject
  created_at: string
  updated_at: string
  message_count?: number
  active?: boolean
}

export interface SessionMessage {
  sequence: number
  id: string
  role: string
  content: string
  metadata?: JsonObject
  created_at: string
}

export interface CronJob {
  id: string
  session_key?: string | null
  expression: string
  payload: unknown
  enabled: boolean
  next_run_at?: string | null
  created_at: string
  updated_at: string
}

export interface CronRun {
  run_id: string
  job_id: string
  started_at: string
  completed_at?: string | null
  status: string
  attempt: number
  error?: string | null
  owner_pid?: number | null
}

export interface ToolRecord {
  name: string
  description: string
  input_schema: unknown
}

export interface SkillRecord {
  name: string
  source: string
  path: string
  description?: string | null
}

export interface PendingApproval {
  id: string
  session_id?: string
  session: {
    platform: string
    channel_id: string
    user_id: string
  }
  command: string
  reason: string
  created_at: string
}

export interface AllowlistRecord {
  pattern_key: string
  created_at: string
}

export interface LogEntry {
  id: number
  timestamp: string
  level: string
  target: string
  message: string
  fields: JsonObject
}

export interface MemoryRecord {
  id: string
  session_key: string
  content: string
  metadata?: JsonObject
  created_at: string
  updated_at: string
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const headers = new Headers(init?.headers)
  if (init?.body && !headers.has('content-type')) {
    headers.set('content-type', 'application/json')
  }
  const response = await fetch(path, { ...init, headers })
  if (!response.ok) {
    let detail = `${response.status} ${response.statusText}`
    try {
      const payload = (await response.json()) as { error?: string }
      if (payload.error) detail = payload.error
    } catch {
      // Ignore non-JSON error bodies.
    }
    throw new Error(detail)
  }
  if (response.status === 204) return undefined as T
  return (await response.json()) as T
}

export const api = {
  request,
  status: () => request<StatusResponse>('/api/status'),
  health: () => request<{ status: string }>('/api/health'),
  config: () => request<JsonObject>('/api/config'),
  sessions: (search?: string) =>
    request<ApiList<SessionRecord>>(search ? `/api/sessions?q=${encodeURIComponent(search)}` : '/api/sessions'),
  session: (id: string) => request<SessionRecord>(`/api/sessions/${encodeURIComponent(id)}`),
  sessionMessages: (id: string) =>
    request<ApiList<SessionMessage>>(`/api/sessions/${encodeURIComponent(id)}/messages`),
  deleteSession: (id: string) =>
    request<void>(`/api/sessions/${encodeURIComponent(id)}`, { method: 'DELETE' }),
  postChat: (id: string, content: string) =>
    request<{ response: string }>(`/api/sessions/${encodeURIComponent(id)}/chat`, {
      method: 'POST',
      body: JSON.stringify({ content }),
    }),
  cronJobs: () => request<ApiList<CronJob>>('/api/cron/jobs'),
  cronJob: (id: string) => request<CronJob>(`/api/cron/jobs/${encodeURIComponent(id)}`),
  createCronJob: (data: { id: string; expression: string; payload: unknown }) =>
    request<CronJob>('/api/cron/jobs', {
      method: 'POST',
      body: JSON.stringify(data),
    }),
  updateCronJob: (id: string, data: Partial<CronJob>) =>
    request<CronJob>(`/api/cron/jobs/${encodeURIComponent(id)}`, {
      method: 'PUT',
      body: JSON.stringify(data),
    }),
  deleteCronJob: (id: string) =>
    request<void>(`/api/cron/jobs/${encodeURIComponent(id)}`, { method: 'DELETE' }),
  triggerCronJob: (id: string) =>
    request<void>(`/api/cron/jobs/${encodeURIComponent(id)}/trigger`, { method: 'POST' }),
  pauseCronJob: (id: string) =>
    request<void>(`/api/cron/jobs/${encodeURIComponent(id)}/pause`, { method: 'POST' }),
  resumeCronJob: (id: string) =>
    request<void>(`/api/cron/jobs/${encodeURIComponent(id)}/resume`, { method: 'POST' }),
  cronRuns: () => request<ApiList<CronRun>>('/api/cron/runs'),
  tools: () => request<ApiList<ToolRecord>>('/api/tools'),
  skills: () => request<ApiList<SkillRecord>>('/api/skills'),
  pendingApprovals: () => request<ApiList<PendingApproval>>('/api/approvals/pending'),
  resolveApproval: (id: string, decision: 'Once' | 'Session' | 'Always' | 'Deny', reason?: string) =>
    request<void>(`/api/approvals/${encodeURIComponent(id)}/resolve`, {
      method: 'POST',
      body: JSON.stringify({ decision, reason }),
    }),
  approvalAllowlist: () => request<ApiList<AllowlistRecord>>('/api/approvals/allowlist'),
  logs: () => request<ApiList<LogEntry>>('/api/logs'),
  memories: () => request<ApiList<MemoryRecord>>('/api/memory'),
}

export function socketUrl(path: string): string {
  const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:'
  return `${protocol}//${window.location.host}${path}`
}

export function formatBytes(value: number | null | undefined): string {
  if (value == null || Number.isNaN(value)) return '—'
  if (value < 1024) return `${value} B`
  const units = ['KB', 'MB', 'GB', 'TB']
  let next = value / 1024
  let unit = units[0]
  for (let index = 1; index < units.length && next >= 1024; index += 1) {
    next /= 1024
    unit = units[index]
  }
  return `${next >= 10 ? next.toFixed(1) : next.toFixed(2)} ${unit}`
}

export function formatDuration(seconds: number): string {
  const days = Math.floor(seconds / 86400)
  const hours = Math.floor((seconds % 86400) / 3600)
  const minutes = Math.floor((seconds % 3600) / 60)
  if (days) return `${days}d ${hours}h`
  if (hours) return `${hours}h ${minutes}m`
  if (minutes) return `${minutes}m ${seconds % 60}s`
  return `${seconds}s`
}

export function formatDate(value?: string | null): string {
  if (!value) return '—'
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return value
  return new Intl.DateTimeFormat(undefined, {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  }).format(date)
}
