import React, { FormEvent, useEffect, useState } from 'react'
import { Bot, Check, Cpu, Edit3, MessageSquare, Plus, Save, Sparkles, Terminal, Trash2, Wrench } from 'lucide-react'
import { Badge, Button, Card, CardContent, CardDescription, CardFooter, CardHeader, CardTitle, Dialog, Input, Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '../components/ui'
import { api, formatDate } from '../api'

export interface BotRecord {
  bot_id: string
  name: string
  model: string | null
  system_prompt: string | null
  enabled_toolsets: string[] | null
  custom_settings: Record<string, any>
  created_at: string
  updated_at: string
}

export function BotsPage() {
  const [bots, setBots] = useState<BotRecord[]>([])
  const [editingBot, setEditingBot] = useState<BotRecord | null>(null)
  const [dialogOpen, setDialogOpen] = useState(false)
  const [createDialogOpen, setCreateDialogOpen] = useState(false)
  const [loading, setLoading] = useState(false)
  const [saving, setSaving] = useState(false)

  const [formName, setFormName] = useState('')
  const [formModel, setFormModel] = useState('')
  const [formPrompt, setFormPrompt] = useState('')
  const [formToolsets, setFormToolsets] = useState('')

  const [newBotId, setNewBotId] = useState('')
  const [newName, setNewName] = useState('')
  const [newModel, setNewModel] = useState('')
  const [newPrompt, setNewPrompt] = useState('')
  const [newToolsets, setNewToolsets] = useState('terminal, file, mcp, cron, skills')

  const loadBots = async () => {
    setLoading(true)
    try {
      const res = await api.request<{ items: BotRecord[] }>('/api/bots')
      setBots(res.items || [])
    } catch (err: any) {
      alert(`Failed to load bots: ${err.message}`)
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    loadBots()
  }, [])

  const handleEdit = (bot: BotRecord) => {
    setEditingBot(bot)
    setFormName(bot.name || '')
    setFormModel(bot.model || '')
    setFormPrompt(bot.system_prompt || '')
    setFormToolsets(bot.enabled_toolsets?.join(', ') || '')
    setDialogOpen(true)
  }

  const handleDelete = async (bot: BotRecord) => {
    if (!confirm(`Are you sure you want to delete bot profile "${bot.name}" (${bot.bot_id})?`)) return
    try {
      await api.request(`/api/bots/${encodeURIComponent(bot.bot_id)}`, {
        method: 'DELETE',
      })
      loadBots()
    } catch (err: any) {
      alert(`Delete failed: ${err.message}`)
    }
  }

  const handleCreate = async (e: FormEvent) => {
    e.preventDefault()
    if (!newBotId.trim() || !newName.trim()) return
    setSaving(true)
    try {
      const toolsetsArray = newToolsets
        .split(',')
        .map((s) => s.trim())
        .filter(Boolean)

      await api.request('/api/bots', {
        method: 'POST',
        body: JSON.stringify({
          bot_id: newBotId.trim(),
          name: newName.trim(),
          model: newModel.trim() || null,
          system_prompt: newPrompt.trim() || null,
          enabled_toolsets: toolsetsArray.length > 0 ? toolsetsArray : null,
        }),
      })

      setCreateDialogOpen(false)
      setNewBotId('')
      setNewName('')
      setNewModel('')
      setNewPrompt('')
      loadBots()
    } catch (err: any) {
      alert(`Create failed: ${err.message}`)
    } finally {
      setSaving(false)
    }
  }

  const handleSave = async (e: FormEvent) => {
    e.preventDefault()
    if (!editingBot) return
    setSaving(true)
    try {
      const toolsetsArray = formToolsets
        .split(',')
        .map((s) => s.trim())
        .filter(Boolean)

      await api.request(`/api/bots/${encodeURIComponent(editingBot.bot_id)}`, {
        method: 'PUT',
        body: JSON.stringify({
          name: formName,
          model: formModel.trim() || null,
          system_prompt: formPrompt.trim() || null,
          enabled_toolsets: toolsetsArray.length > 0 ? toolsetsArray : null,
        }),
      })

      setDialogOpen(false)
      loadBots()
    } catch (err: any) {
      alert(`Save failed: ${err.message}`)
    } finally {
      setSaving(false)
    }
  }

  return (
    <div className="space-y-6 w-full">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-lg font-semibold tracking-tight flex items-center gap-2">
            <Bot className="w-5 h-5 text-primary" />
            Agent & Bot Fleet Profiles
          </h2>
          <p className="text-xs text-muted-foreground">
            Configure individual model overrides, persona prompts, and tool access per Discord Agent Bot
          </p>
        </div>
        <Button size="sm" className="gap-1.5 text-xs" onClick={() => setCreateDialogOpen(true)}>
          <Plus className="w-4 h-4" /> Add Agent Profile
        </Button>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
        {bots.map((bot) => {
          const avatarUrl = bot.custom_settings?.avatar_url
          return (
            <Card key={bot.bot_id} className="flex flex-col justify-between border-border bg-card/60 hover:border-primary/40 transition-colors">
              <CardHeader className="pb-3">
                <div className="flex items-center justify-between">
                  {avatarUrl ? (
                    <img
                      src={avatarUrl}
                      alt={bot.name}
                      className="w-10 h-10 rounded-full border border-primary/30 object-cover shadow-sm bg-muted"
                      onError={(e) => {
                        (e.target as HTMLElement).style.display = 'none'
                      }}
                    />
                  ) : (
                    <div className="w-10 h-10 rounded-full bg-primary/10 border border-primary/20 flex items-center justify-center text-primary font-bold">
                      <Bot className="w-5 h-5" />
                    </div>
                  )}
                  <Badge variant={bot.model ? 'success' : 'outline'} className="text-[10px]">
                    {bot.model ? 'Custom Model' : 'Default Model'}
                  </Badge>
                </div>
                <CardTitle className="text-base font-bold tracking-tight text-foreground pt-2">
                  {bot.name}
                </CardTitle>
                <CardDescription className="text-xs font-mono truncate">
                  ID: {bot.bot_id}
                </CardDescription>
              </CardHeader>

              <CardContent className="space-y-3 text-xs">
                <div className="p-2.5 rounded-md bg-secondary/30 border border-border/60 space-y-1.5">
                  <div className="text-[11px] font-medium text-muted-foreground flex items-center gap-1.5">
                    <Cpu className="w-3.5 h-3.5 text-primary" /> Active Model
                  </div>
                  <div className="font-mono text-foreground font-semibold truncate">
                    {bot.model || <span className="text-muted-foreground/60">Global default</span>}
                  </div>
                </div>

                <div className="p-2.5 rounded-md bg-secondary/30 border border-border/60 space-y-1.5">
                  <div className="text-[11px] font-medium text-muted-foreground flex items-center gap-1.5">
                    <Sparkles className="w-3.5 h-3.5 text-amber-400" /> Persona / System Prompt
                  </div>
                  <div className="text-muted-foreground line-clamp-2 italic">
                    {bot.system_prompt || 'Default general assistant persona'}
                  </div>
                </div>

                <div className="p-2.5 rounded-md bg-secondary/30 border border-border/60 space-y-1.5">
                  <div className="text-[11px] font-medium text-muted-foreground flex items-center gap-1.5">
                    <Wrench className="w-3.5 h-3.5 text-emerald-400" /> Enabled Toolsets
                  </div>
                  <div className="flex flex-wrap gap-1">
                    {bot.enabled_toolsets && bot.enabled_toolsets.length > 0 ? (
                      bot.enabled_toolsets.map((t) => (
                        <Badge key={t} variant="secondary" className="text-[10px] px-1.5 py-0 font-mono">
                          {t}
                        </Badge>
                      ))
                    ) : (
                      <span className="text-muted-foreground/60 text-[11px]">All default toolsets</span>
                    )}
                  </div>
                </div>
              </CardContent>

              <CardFooter className="pt-2 border-t border-border/60 flex justify-between items-center">
                <span className="text-[10px] text-muted-foreground">
                  Updated: {formatDate(bot.updated_at)}
                </span>
                <div className="flex items-center gap-1.5">
                  <Button
                    size="icon"
                    variant="ghost"
                    className="h-8 w-8 text-muted-foreground hover:text-destructive"
                    onClick={() => handleDelete(bot)}
                    title="Delete Bot Profile"
                  >
                    <Trash2 className="w-3.5 h-3.5" />
                  </Button>
                  <Button size="sm" variant="outline" className="h-8 gap-1.5 text-xs" onClick={() => handleEdit(bot)}>
                    <Edit3 className="w-3.5 h-3.5" /> Configure
                  </Button>
                </div>
              </CardFooter>
            </Card>
          )
        })}
      </div>

      <Dialog open={createDialogOpen} onOpenChange={setCreateDialogOpen}>
        <form onSubmit={handleCreate} className="space-y-4">
          <div>
            <h3 className="text-lg font-semibold tracking-tight flex items-center gap-2">
              <Plus className="w-5 h-5 text-primary" />
              Add New Agent Bot Profile
            </h3>
            <p className="text-xs text-muted-foreground">
              Register a new Discord Bot User ID with a custom persona and toolset policies
            </p>
          </div>

          <div className="grid grid-cols-2 gap-4">
            <div className="space-y-2">
              <label className="text-xs font-medium text-muted-foreground">Discord Bot ID (Snowflake)</label>
              <Input
                value={newBotId}
                onChange={(e) => setNewBotId(e.target.value)}
                placeholder="e.g. 1465631383862120451"
                className="font-mono text-xs"
                required
              />
            </div>
            <div className="space-y-2">
              <label className="text-xs font-medium text-muted-foreground">Display Name</label>
              <Input
                value={newName}
                onChange={(e) => setNewName(e.target.value)}
                placeholder="e.g. 아테나 (Athena)"
                required
              />
            </div>
          </div>

          <div className="space-y-2">
            <label className="text-xs font-medium text-muted-foreground flex items-center justify-between">
              <span>Model Override</span>
              <span className="text-[10px] text-muted-foreground">e.g. gpt-5.6-sol, claude-3-7-sonnet</span>
            </label>
            <Input
              value={newModel}
              onChange={(e) => setNewModel(e.target.value)}
              placeholder="Leave empty for gateway default model"
              className="font-mono text-xs"
            />
          </div>

          <div className="space-y-2">
            <label className="text-xs font-medium text-muted-foreground">System Persona / Instructions</label>
            <textarea
              value={newPrompt}
              onChange={(e) => setNewPrompt(e.target.value)}
              placeholder="너는 고도의 분석 능력을 갖춘 비즈니스 전략가다..."
              rows={4}
              className="w-full rounded-md border border-input bg-transparent px-3 py-2 text-xs shadow-sm focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring text-foreground font-sans"
            />
          </div>

          <div className="space-y-2">
            <label className="text-xs font-medium text-muted-foreground flex items-center justify-between">
              <span>Enabled Toolsets (comma-separated)</span>
              <span className="text-[10px] text-muted-foreground">terminal, file, mcp, cron, skills</span>
            </label>
            <Input
              value={newToolsets}
              onChange={(e) => setNewToolsets(e.target.value)}
              placeholder="terminal, file, mcp, cron"
              className="font-mono text-xs"
            />
          </div>

          <div className="flex justify-end gap-2 pt-2 border-t border-border">
            <Button type="button" variant="ghost" onClick={() => setCreateDialogOpen(false)}>
              Cancel
            </Button>
            <Button type="submit" disabled={saving} className="gap-1.5">
              <Save className="w-4 h-4" /> Create Profile
            </Button>
          </div>
        </form>
      </Dialog>

      <Dialog open={dialogOpen} onOpenChange={setDialogOpen}>
        <form onSubmit={handleSave} className="space-y-4">
          <div>
            <h3 className="text-lg font-semibold tracking-tight flex items-center gap-2">
              <Bot className="w-5 h-5 text-primary" />
              Configure {editingBot?.name}
            </h3>
            <p className="text-xs text-muted-foreground">
              Customize model, system prompt persona, and authorized tools for this bot ID ({editingBot?.bot_id})
            </p>
          </div>

          <div className="space-y-2">
            <label className="text-xs font-medium text-muted-foreground">Display Name</label>
            <Input value={formName} onChange={(e) => setFormName(e.target.value)} required />
          </div>

          <div className="space-y-2">
            <label className="text-xs font-medium text-muted-foreground flex items-center justify-between">
              <span>Model Override</span>
              <span className="text-[10px] text-muted-foreground">e.g. gpt-5.6-sol, claude-3-7-sonnet, deepseek-r1</span>
            </label>
            <Input
              value={formModel}
              onChange={(e) => setFormModel(e.target.value)}
              placeholder="Leave empty for gateway default model"
              className="font-mono text-xs"
            />
          </div>

          <div className="space-y-2">
            <label className="text-xs font-medium text-muted-foreground">System Persona / Instructions</label>
            <textarea
              value={formPrompt}
              onChange={(e) => setFormPrompt(e.target.value)}
              placeholder="You are a helpful senior software architect..."
              rows={4}
              className="w-full rounded-md border border-input bg-transparent px-3 py-2 text-xs shadow-sm focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring text-foreground font-sans"
            />
          </div>

          <div className="space-y-2">
            <label className="text-xs font-medium text-muted-foreground flex items-center justify-between">
              <span>Enabled Toolsets (comma-separated)</span>
              <span className="text-[10px] text-muted-foreground">terminal, file, mcp, cron, skills</span>
            </label>
            <Input
              value={formToolsets}
              onChange={(e) => setFormToolsets(e.target.value)}
              placeholder="terminal, file, mcp, cron"
              className="font-mono text-xs"
            />
          </div>

          <div className="flex justify-end gap-2 pt-2 border-t border-border">
            <Button type="button" variant="ghost" onClick={() => setDialogOpen(false)}>
              Cancel
            </Button>
            <Button type="submit" disabled={saving} className="gap-1.5">
              <Save className="w-4 h-4" /> Save Profile
            </Button>
          </div>
        </form>
      </Dialog>
    </div>
  )
}
