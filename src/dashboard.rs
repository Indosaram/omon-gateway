use std::collections::{HashMap, HashSet, VecDeque};
use std::env;
use std::net::IpAddr;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use axum::body::Body;
use axum::extract::ws::{Message as WsMessage, WebSocket, WebSocketUpgrade};
use axum::extract::{Path as AxumPath, Query, State};
use axum::http::{header, HeaderValue, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post, put};
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use omon_gateway::{
    CronJob, CronJobSpec, CronScheduler, InboundEvent, OmonError, OutboundAction,
    OutboundDispatcher, SessionKey, SessionMultiplexer, SmartApprovalGuard, ToolRegistry,
};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use sqlx::{FromRow, SqlitePool};
use tokio::net::TcpListener;
use tokio::sync::{broadcast, RwLock};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tower_http::trace::TraceLayer;
use tracing::field::{Field, Visit};
use tracing::{Event, Subscriber};
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::{EnvFilter, Layer};
use uuid::Uuid;

const DEFAULT_DASHBOARD_HOST: &str = "127.0.0.1";
const DEFAULT_DASHBOARD_PORT: u16 = 9119;
const MAX_PAGE_SIZE: u32 = 200;
const LOG_CAPACITY: usize = 2_000;

#[derive(Clone, Debug, clap::Args)]
pub struct DashboardArgs {
    /// Address to bind the dashboard HTTP server to.
    #[arg(long, default_value = DEFAULT_DASHBOARD_HOST)]
    pub host: String,
    /// Port to bind the dashboard HTTP server to.
    #[arg(long, default_value_t = DEFAULT_DASHBOARD_PORT)]
    pub port: u16,
    /// Allow binding to a non-loopback interface without transport authentication.
    #[arg(long)]
    pub insecure: bool,
}

#[derive(Clone, Debug)]
pub struct DashboardSettings {
    pub enabled: bool,
    pub host: String,
    pub port: u16,
    pub insecure: bool,
    pub web_root: PathBuf,
}

impl DashboardSettings {
    pub fn from_args(args: DashboardArgs) -> Self {
        Self {
            enabled: true,
            host: args.host,
            port: args.port,
            insecure: args.insecure,
            web_root: dashboard_web_root(),
        }
    }

    pub fn from_env() -> Self {
        let port_raw = env::var("DASHBOARD_PORT").ok();
        let enabled = env_bool("DASHBOARD_ENABLED", false) || port_raw.is_some();
        let port = port_raw
            .as_deref()
            .and_then(|raw| raw.trim().parse::<u16>().ok())
            .unwrap_or(DEFAULT_DASHBOARD_PORT);
        Self {
            enabled,
            host: env::var("DASHBOARD_HOST")
                .ok()
                .filter(|value| !value.trim().is_empty())
                .unwrap_or_else(|| DEFAULT_DASHBOARD_HOST.to_owned()),
            port,
            insecure: env_bool("DASHBOARD_INSECURE", false),
            web_root: dashboard_web_root(),
        }
    }

    pub fn validate(&self) -> Result<(), OmonError> {
        if self.port == 0 {
            return Err(OmonError::Config(
                "dashboard port must be greater than zero".into(),
            ));
        }
        if !self.insecure && !is_loopback_host(&self.host) {
            return Err(OmonError::Config(format!(
                "refusing to expose the unauthenticated dashboard on non-loopback host {}; pass --insecure or set DASHBOARD_INSECURE=true to acknowledge the risk",
                self.host
            )));
        }
        Ok(())
    }

    pub fn display_address(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

fn dashboard_web_root() -> PathBuf {
    env::var_os("DASHBOARD_WEB_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("web/dist"))
}

fn env_bool(name: &str, default: bool) -> bool {
    env::var(name)
        .ok()
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(default)
}

fn is_loopback_host(host: &str) -> bool {
    if host.eq_ignore_ascii_case("localhost") {
        return true;
    }
    host.parse::<IpAddr>().is_ok_and(|ip| ip.is_loopback())
}

#[derive(Clone, Debug, Serialize)]
pub struct DashboardLogEntry {
    pub id: u64,
    pub timestamp: DateTime<Utc>,
    pub level: String,
    pub target: String,
    pub message: String,
    pub fields: Value,
}

#[derive(Clone)]
pub struct DashboardLogStore {
    entries: Arc<Mutex<VecDeque<DashboardLogEntry>>>,
    sender: broadcast::Sender<DashboardLogEntry>,
    sequence: Arc<AtomicU64>,
}

impl Default for DashboardLogStore {
    fn default() -> Self {
        let (sender, _) = broadcast::channel(512);
        Self {
            entries: Arc::new(Mutex::new(VecDeque::with_capacity(LOG_CAPACITY))),
            sender,
            sequence: Arc::new(AtomicU64::new(1)),
        }
    }
}

impl DashboardLogStore {
    fn push(&self, level: &str, target: &str, message: String, fields: Value) {
        let entry = DashboardLogEntry {
            id: self.sequence.fetch_add(1, Ordering::Relaxed),
            timestamp: Utc::now(),
            level: level.to_owned(),
            target: target.to_owned(),
            message,
            fields,
        };
        {
            let mut entries = self.entries.lock();
            if entries.len() >= LOG_CAPACITY {
                entries.pop_front();
            }
            entries.push_back(entry.clone());
        }
        let _ = self.sender.send(entry);
    }

    pub fn snapshot(&self) -> Vec<DashboardLogEntry> {
        self.entries.lock().iter().cloned().collect()
    }

    pub fn subscribe(&self) -> broadcast::Receiver<DashboardLogEntry> {
        self.sender.subscribe()
    }
}

static DASHBOARD_LOGS: OnceLock<DashboardLogStore> = OnceLock::new();

pub fn global_logs() -> DashboardLogStore {
    DASHBOARD_LOGS.get_or_init(Default::default).clone()
}

struct EventFieldVisitor {
    message: Option<String>,
    fields: Map<String, Value>,
}

impl EventFieldVisitor {
    fn new() -> Self {
        Self {
            message: None,
            fields: Map::new(),
        }
    }
}

impl Visit for EventFieldVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            self.message = Some(value.to_owned());
        } else {
            self.fields
                .insert(field.name().to_owned(), Value::String(value.to_owned()));
        }
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.fields.insert(field.name().to_owned(), Value::Bool(value));
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.fields
            .insert(field.name().to_owned(), Value::Number(value.into()));
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.fields
            .insert(field.name().to_owned(), Value::Number(value.into()));
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        let rendered = format!("{value:?}");
        if field.name() == "message" {
            self.message = Some(rendered);
        } else {
            self.fields
                .insert(field.name().to_owned(), Value::String(rendered));
        }
    }
}

#[derive(Clone)]
struct DashboardLogLayer {
    logs: DashboardLogStore,
}

impl<S> Layer<S> for DashboardLogLayer
where
    S: Subscriber,
{
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let metadata = event.metadata();
        let mut visitor = EventFieldVisitor::new();
        event.record(&mut visitor);
        let message = visitor.message.unwrap_or_else(|| {
            visitor
                .fields
                .get("error")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned()
        });
        self.logs.push(
            metadata.level().as_str(),
            metadata.target(),
            message,
            Value::Object(visitor.fields),
        );
    }
}

pub fn init_tracing() {
    let logs = global_logs();
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer())
        .with(DashboardLogLayer { logs })
        .init();
}

#[derive(Clone, Debug, Serialize)]
pub struct PendingApprovalView {
    pub id: Uuid,
    pub session: SessionKey,
    pub command: String,
    pub reason: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Clone)]
pub struct WebDashboardDispatcher {
    sender: broadcast::Sender<OutboundAction>,
    pending: Arc<RwLock<HashMap<Uuid, PendingApprovalView>>>,
}

impl Default for WebDashboardDispatcher {
    fn default() -> Self {
        let (sender, _) = broadcast::channel(1024);
        Self {
            sender,
            pending: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl WebDashboardDispatcher {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn subscribe(&self) -> broadcast::Receiver<OutboundAction> {
        self.sender.subscribe()
    }

    pub async fn pending_approvals(&self) -> Vec<PendingApprovalView> {
        let mut pending = self
            .pending
            .read()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>();
        pending.sort_by_key(|entry| entry.created_at);
        pending
    }

    pub async fn remove_pending(&self, request_id: Uuid) {
        self.pending.write().await.remove(&request_id);
    }
}

#[async_trait]
impl OutboundDispatcher for WebDashboardDispatcher {
    async fn dispatch(&self, action: OutboundAction) -> omon_gateway::Result<()> {
        match &action {
            OutboundAction::ApprovalRequest {
                session,
                request_id,
                command,
                reason,
            } => {
                self.pending.write().await.insert(
                    *request_id,
                    PendingApprovalView {
                        id: *request_id,
                        session: session.clone(),
                        command: command.clone(),
                        reason: reason.clone(),
                        created_at: Utc::now(),
                    },
                );
            }
            OutboundAction::ExpireApproval { request_id } => {
                self.pending.write().await.remove(request_id);
            }
            _ => {}
        }
        let _ = self.sender.send(action);
        Ok(())
    }
}

#[derive(Clone)]
#[allow(dead_code)]
pub struct CompositeDispatcher {
    primary: Arc<dyn OutboundDispatcher>,
    dashboard: WebDashboardDispatcher,
}

#[allow(dead_code)]
impl CompositeDispatcher {
    pub fn new(
        primary: Arc<dyn OutboundDispatcher>,
        dashboard: WebDashboardDispatcher,
    ) -> Self {
        Self { primary, dashboard }
    }
}

#[async_trait]
impl OutboundDispatcher for CompositeDispatcher {
    async fn dispatch(&self, action: OutboundAction) -> omon_gateway::Result<()> {
        let web_only = action_session(&action).is_some_and(|session| session.platform == "web");
        self.dashboard.dispatch(action.clone()).await?;
        if web_only {
            Ok(())
        } else {
            self.primary.dispatch(action).await
        }
    }
}

fn action_session(action: &OutboundAction) -> Option<&SessionKey> {
    match action {
        OutboundAction::SendMessage { session, .. }
        | OutboundAction::EditMessage { session, .. }
        | OutboundAction::DeleteMessage { session, .. }
        | OutboundAction::UploadFile { session, .. }
        | OutboundAction::Stream { session, .. }
        | OutboundAction::Typing { session, .. }
        | OutboundAction::React { session, .. }
        | OutboundAction::ApprovalRequest { session, .. } => Some(session),
        OutboundAction::ExpireApproval { .. } => None,
    }
}

#[derive(Clone)]
pub struct DashboardState {
    pub pool: SqlitePool,
    pub multiplexer: Option<SessionMultiplexer>,
    pub scheduler: CronScheduler,
    pub tools: ToolRegistry,
    pub approvals: SmartApprovalGuard,
    pub events: WebDashboardDispatcher,
    pub config: Value,
    pub workspace_root: PathBuf,
    pub skill_roots: Vec<PathBuf>,
    pub bot_connections: usize,
    pub started_at: Instant,
    pub web_root: PathBuf,
    pub logs: DashboardLogStore,
}

#[allow(clippy::too_many_arguments)]
impl DashboardState {
    pub fn new(
        pool: SqlitePool,
        multiplexer: Option<SessionMultiplexer>,
        scheduler: CronScheduler,
        tools: ToolRegistry,
        approvals: SmartApprovalGuard,
        events: WebDashboardDispatcher,
        config: Value,
        workspace_root: PathBuf,
        skill_roots: Vec<PathBuf>,
        bot_connections: usize,
        web_root: PathBuf,
    ) -> Self {
        Self {
            pool,
            multiplexer,
            scheduler,
            tools,
            approvals,
            events,
            config,
            workspace_root,
            skill_roots,
            bot_connections,
            started_at: Instant::now(),
            web_root,
            logs: global_logs(),
        }
    }
}

#[allow(dead_code)]
pub(crate) fn config_view_from_gateway(
    config: &super::Config,
    settings: &DashboardSettings,
) -> Value {
    json!({
        "model": config.default_model,
        "providers": {
            "openai_base_url": config.openai_api_base,
            "openai_api_key_configured": !config.openai_api_key.trim().is_empty(),
            "anthropic_base_url": config.anthropic_base_url,
            "anthropic_api_key_configured": config.anthropic_api_key.as_deref().is_some_and(|value| !value.trim().is_empty()),
        },
        "approval": {
            "policy": format!("{:?}", config.approval_policy),
            "timeout_secs": config.approval_timeout_secs,
            "mention_requesters": config.approval_mentions,
            "deny_patterns": config.approvals_deny,
        },
        "workspace_root": config.workspace_root,
        "tool_roots": config.extra_tool_roots,
        "discord": {
            "bot_count": config.discord_bot_tokens.len(),
            "allowed_users": config.allowed_users,
            "allowed_roles": config.allowed_roles,
            "allowed_channels": config.allowed_channels,
            "ignored_channels": config.ignored_channels,
            "allow_all_users": config.allow_all_users,
            "allow_bots": format!("{:?}", config.allow_bots),
            "auto_thread": config.auto_thread,
            "thread_sessions_per_user": config.thread_sessions_per_user,
            "thread_require_mention": config.thread_require_mention,
        },
        "runtime": {
            "processing_reactions": config.processing_reactions,
            "runtime_footer": config.runtime_footer,
            "cron_script_timeout_secs": config.cron_script_timeout_secs,
        },
        "dashboard": {
            "host": settings.host,
            "port": settings.port,
            "insecure": settings.insecure,
        }
    })
}

pub fn config_view_from_environment(settings: &DashboardSettings) -> Value {
    json!({
        "model": env::var("DEFAULT_MODEL").ok(),
        "providers": {
            "openai_base_url": env::var("OPENAI_API_BASE").ok(),
            "openai_api_key_configured": env::var("OPENAI_API_KEY").is_ok_and(|value| !value.trim().is_empty()),
            "anthropic_base_url": env::var("ANTHROPIC_BASE_URL").ok(),
            "anthropic_api_key_configured": env::var("ANTHROPIC_API_KEY").is_ok_and(|value| !value.trim().is_empty()),
        },
        "approval": {
            "policy": env::var("APPROVAL_POLICY").unwrap_or_else(|_| "ask".into()),
            "timeout_secs": env::var("APPROVAL_TIMEOUT_SECS").ok().and_then(|v| v.parse::<u64>().ok()).unwrap_or(900),
        },
        "workspace_root": env::var("WORKSPACE_ROOT").unwrap_or_else(|_| "workspace".into()),
        "tool_roots": env::var("EXTRA_TOOL_ROOTS").ok(),
        "discord": {
            "bot_count": env::var("DISCORD_BOT_TOKENS").ok().map(|v| v.split(',').filter(|v| !v.trim().is_empty()).count()).unwrap_or(0),
            "configured": env::var("DISCORD_BOT_TOKEN").is_ok() || env::var("DISCORD_BOT_TOKENS").is_ok(),
        },
        "dashboard": {
            "host": settings.host,
            "port": settings.port,
            "insecure": settings.insecure,
        }
    })
}

pub async fn spawn_server(
    settings: DashboardSettings,
    mut state: DashboardState,
    shutdown: CancellationToken,
) -> Result<JoinHandle<()>, OmonError> {
    settings.validate()?;
    state.web_root = settings.web_root.clone();
    let listener = TcpListener::bind((settings.host.as_str(), settings.port))
        .await
        .map_err(|error| {
            OmonError::Config(format!(
                "failed to bind dashboard on {}: {error}",
                settings.display_address()
            ))
        })?;
    let local_addr = listener
        .local_addr()
        .map_err(|error| OmonError::Config(format!("failed to read dashboard address: {error}")))?;
    tracing::info!(address = %local_addr, "omon dashboard listening");
    let app = router(state);
    Ok(tokio::spawn(async move {
        let result = axum::serve(listener, app)
            .with_graceful_shutdown(shutdown.cancelled_owned())
            .await;
        if let Err(error) = result {
            tracing::error!(%error, "dashboard HTTP server stopped with an error");
        }
    }))
}

pub fn router(state: DashboardState) -> Router {
    Router::new()
        .route("/api/status", get(api_status))
        .route("/api/health", get(api_health))
        .route("/api/readiness", get(api_readiness))
        .route("/api/sessions", get(list_sessions))
        .route(
            "/api/sessions/{id}",
            get(get_session).delete(delete_session),
        )
        .route("/api/sessions/{id}/messages", get(list_messages))
        .route("/api/sessions/{id}/chat", post(post_chat))
        .route("/api/sessions/{id}/ws", get(session_ws))
        .route("/api/cron/jobs", get(list_cron_jobs).post(create_cron_job))
        .route(
            "/api/cron/jobs/{id}",
            get(get_cron_job).put(update_cron_job).delete(delete_cron_job),
        )
        .route("/api/cron/jobs/{id}/trigger", post(trigger_cron_job))
        .route("/api/cron/jobs/{id}/pause", post(pause_cron_job))
        .route("/api/cron/jobs/{id}/resume", post(resume_cron_job))
        .route("/api/cron/runs", get(list_cron_runs))
        .route("/api/config", get(get_config))
        .route("/api/tools", get(list_tools))
        .route("/api/skills", get(list_skills))
        .route("/api/memory", get(list_memory))
        .route("/api/approvals/pending", get(list_pending_approvals))
        .route("/api/approvals/{id}/resolve", post(resolve_approval))
        .route("/api/approvals/allowlist", get(list_approval_allowlist))
        .route("/api/bots", get(list_bots).post(create_bot))
        .route("/api/bots/{id}", get(get_bot).put(update_bot).delete(delete_bot))
        .route("/api/logs", get(list_logs))
        .route("/api/logs/ws", get(logs_ws))
        .fallback(get(serve_static))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    fn new(status: StatusCode, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
        }
    }

    fn not_found(resource: &str) -> Self {
        Self::new(StatusCode::NOT_FOUND, format!("{resource} not found"))
    }
}

impl From<OmonError> for ApiError {
    fn from(error: OmonError) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
    }
}

impl From<sqlx::Error> for ApiError {
    fn from(error: sqlx::Error) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(json!({
                "error": self.message,
                "status": self.status.as_u16(),
            })),
        )
            .into_response()
    }
}

async fn api_health(State(state): State<DashboardState>) -> Json<Value> {
    Json(json!({
        "status": "ok",
        "uptime_seconds": state.started_at.elapsed().as_secs(),
        "time": Utc::now(),
    }))
}

async fn api_readiness(State(state): State<DashboardState>) -> Response {
    let db_ok = sqlx::query_scalar::<_, i64>("SELECT 1")
        .fetch_one(&state.pool)
        .await
        .is_ok();
    let workspace_ok = tokio::fs::metadata(&state.workspace_root).await.is_ok();
    let chat_ok = state.multiplexer.is_some();
    let ready = db_ok && workspace_ok && chat_ok;
    let status = if ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (
        status,
        Json(json!({
            "status": if ready { "ready" } else { "degraded" },
            "checks": {
                "database": db_ok,
                "workspace": workspace_ok,
                "chat_runtime": chat_ok,
            }
        })),
    )
        .into_response()
}

async fn api_status(State(state): State<DashboardState>) -> Result<Json<Value>, ApiError> {
    let stored_sessions = scalar_count(&state.pool, "SELECT COUNT(*) FROM sessions").await?;
    let messages = scalar_count(&state.pool, "SELECT COUNT(*) FROM messages").await?;
    let cron_jobs = scalar_count(&state.pool, "SELECT COUNT(*) FROM cron_jobs").await?;
    let memories = scalar_count(&state.pool, "SELECT COUNT(*) FROM memories").await?;
    let active_sessions = state
        .multiplexer
        .as_ref()
        .map_or(0, SessionMultiplexer::active_sessions);
    let disk_total = fs2::total_space(&state.workspace_root).ok();
    let disk_available = fs2::available_space(&state.workspace_root).ok();
    let memory_bytes = process_memory_bytes();
    Ok(Json(json!({
        "status": "ok",
        "uptime_seconds": state.started_at.elapsed().as_secs(),
        "bot_connections": state.bot_connections,
        "active_sessions": active_sessions,
        "chat_available": state.multiplexer.is_some(),
        "database": {
            "sessions": stored_sessions,
            "messages": messages,
            "cron_jobs": cron_jobs,
            "memories": memories,
            "pool_size": state.pool.size(),
            "pool_idle": state.pool.num_idle(),
        },
        "memory": {
            "process_bytes": memory_bytes,
        },
        "disk": {
            "workspace_total_bytes": disk_total,
            "workspace_available_bytes": disk_available,
        },
        "pending_approvals": state.approvals.pending_count().await,
        "time": Utc::now(),
    })))
}

async fn scalar_count(pool: &SqlitePool, sql: &str) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar::<_, i64>(sql).fetch_one(pool).await
}

fn process_memory_bytes() -> Option<u64> {
    #[cfg(unix)]
    {
        let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();
        // SAFETY: getrusage writes a fully initialized rusage structure when it returns 0.
        let result = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
        if result != 0 {
            return None;
        }
        // SAFETY: a successful getrusage call initialized the value above.
        let usage = unsafe { usage.assume_init() };
        #[cfg(target_os = "macos")]
        {
            Some(usage.ru_maxrss as u64)
        }
        #[cfg(not(target_os = "macos"))]
        {
            Some((usage.ru_maxrss as u64).saturating_mul(1024))
        }
    }
    #[cfg(not(unix))]
    {
        None
    }
}

#[derive(Clone, Debug, Serialize, FromRow)]
struct SessionRow {
    session_key: String,
    platform: String,
    guild_id: Option<String>,
    channel_id: String,
    thread_id: Option<String>,
    user_id: String,
    state_json: String,
    created_at: String,
    updated_at: String,
}

impl SessionRow {
    fn view(self) -> Value {
        json!({
            "id": self.session_key,
            "platform": self.platform,
            "guild_id": self.guild_id,
            "channel_id": self.channel_id,
            "thread_id": self.thread_id,
            "user_id": self.user_id,
            "state": serde_json::from_str::<Value>(&self.state_json).unwrap_or_else(|_| json!({})),
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        })
    }
}

#[derive(Debug, Deserialize)]
struct PageQuery {
    page: Option<u32>,
    per_page: Option<u32>,
    search: Option<String>,
}

impl PageQuery {
    fn values(&self) -> (u32, u32, i64) {
        let page = self.page.unwrap_or(1).max(1);
        let per_page = self.per_page.unwrap_or(50).clamp(1, MAX_PAGE_SIZE);
        let offset = i64::from((page - 1).saturating_mul(per_page));
        (page, per_page, offset)
    }
}

async fn list_sessions(
    State(state): State<DashboardState>,
    Query(query): Query<PageQuery>,
) -> Result<Json<Value>, ApiError> {
    let (page, per_page, offset) = query.values();
    let search = query
        .search
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| format!("%{value}%"));

    let (rows, total) = if let Some(pattern) = search {
        let rows = sqlx::query_as::<_, SessionRow>(
            "SELECT session_key, platform, guild_id, channel_id, thread_id, user_id, state_json, created_at, updated_at
             FROM sessions
             WHERE session_key LIKE ? OR platform LIKE ? OR channel_id LIKE ? OR user_id LIKE ?
             ORDER BY updated_at DESC
             LIMIT ? OFFSET ?",
        )
        .bind(&pattern)
        .bind(&pattern)
        .bind(&pattern)
        .bind(&pattern)
        .bind(i64::from(per_page))
        .bind(offset)
        .fetch_all(&state.pool)
        .await?;
        let total = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM sessions
             WHERE session_key LIKE ? OR platform LIKE ? OR channel_id LIKE ? OR user_id LIKE ?",
        )
        .bind(&pattern)
        .bind(&pattern)
        .bind(&pattern)
        .bind(&pattern)
        .fetch_one(&state.pool)
        .await?;
        (rows, total)
    } else {
        let rows = sqlx::query_as::<_, SessionRow>(
            "SELECT session_key, platform, guild_id, channel_id, thread_id, user_id, state_json, created_at, updated_at
             FROM sessions ORDER BY updated_at DESC LIMIT ? OFFSET ?",
        )
        .bind(i64::from(per_page))
        .bind(offset)
        .fetch_all(&state.pool)
        .await?;
        let total = scalar_count(&state.pool, "SELECT COUNT(*) FROM sessions").await?;
        (rows, total)
    };

    Ok(Json(json!({
        "items": rows.into_iter().map(SessionRow::view).collect::<Vec<_>>(),
        "page": page,
        "per_page": per_page,
        "total": total,
    })))
}

async fn get_session(
    State(state): State<DashboardState>,
    AxumPath(id): AxumPath<String>,
) -> Result<Json<Value>, ApiError> {
    let storage_id = resolve_storage_id(&state.pool, &id).await?;
    let row = fetch_session_row(&state.pool, &storage_id)
        .await?
        .ok_or_else(|| ApiError::not_found("session"))?;
    let message_count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM messages WHERE session_key = ?",
    )
    .bind(&storage_id)
    .fetch_one(&state.pool)
    .await?;
    let mut view = row.view();
    if let Value::Object(ref mut object) = view {
        object.insert("message_count".into(), Value::Number(message_count.into()));
        object.insert(
            "active".into(),
            Value::Bool(
                state
                    .multiplexer
                    .as_ref()
                    .is_some_and(|mux| parse_storage_key(&storage_id).is_some_and(|key| mux.contains_session(&key))),
            ),
        );
    }
    Ok(Json(view))
}

async fn fetch_session_row(
    pool: &SqlitePool,
    storage_id: &str,
) -> Result<Option<SessionRow>, sqlx::Error> {
    sqlx::query_as::<_, SessionRow>(
        "SELECT session_key, platform, guild_id, channel_id, thread_id, user_id, state_json, created_at, updated_at
         FROM sessions WHERE session_key = ?",
    )
    .bind(storage_id)
    .fetch_optional(pool)
    .await
}

#[derive(Debug, Serialize, FromRow)]
struct MessageRow {
    sequence: i64,
    id: String,
    role: String,
    content: String,
    metadata_json: String,
    created_at: String,
}

async fn list_messages(
    State(state): State<DashboardState>,
    AxumPath(id): AxumPath<String>,
    Query(query): Query<PageQuery>,
) -> Result<Json<Value>, ApiError> {
    let storage_id = resolve_storage_id(&state.pool, &id).await?;
    if fetch_session_row(&state.pool, &storage_id).await?.is_none() {
        return Err(ApiError::not_found("session"));
    }
    let (page, per_page, offset) = query.values();
    let rows = sqlx::query_as::<_, MessageRow>(
        "SELECT sequence, id, role, content, metadata_json, created_at
         FROM messages WHERE session_key = ? ORDER BY sequence ASC LIMIT ? OFFSET ?",
    )
    .bind(&storage_id)
    .bind(i64::from(per_page))
    .bind(offset)
    .fetch_all(&state.pool)
    .await?;
    let total = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM messages WHERE session_key = ?",
    )
    .bind(&storage_id)
    .fetch_one(&state.pool)
    .await?;
    let items = rows
        .into_iter()
        .map(|row| {
            json!({
                "sequence": row.sequence,
                "id": row.id,
                "role": row.role,
                "content": row.content,
                "metadata": serde_json::from_str::<Value>(&row.metadata_json).unwrap_or_else(|_| json!({})),
                "created_at": row.created_at,
            })
        })
        .collect::<Vec<_>>();
    Ok(Json(json!({
        "items": items,
        "page": page,
        "per_page": per_page,
        "total": total,
    })))
}

async fn delete_session(
    State(state): State<DashboardState>,
    AxumPath(id): AxumPath<String>,
) -> Result<Response, ApiError> {
    let storage_id = resolve_storage_id(&state.pool, &id).await?;
    if let (Some(mux), Some(key)) = (&state.multiplexer, parse_storage_key(&storage_id)) {
        let _ = mux.stop(&key).await;
    }
    let result = sqlx::query("DELETE FROM sessions WHERE session_key = ?")
        .bind(&storage_id)
        .execute(&state.pool)
        .await?;
    if result.rows_affected() == 0 {
        return Err(ApiError::not_found("session"));
    }
    Ok(StatusCode::NO_CONTENT.into_response())
}

#[derive(Debug, Deserialize)]
struct ChatRequest {
    message: String,
}

async fn post_chat(
    State(state): State<DashboardState>,
    AxumPath(id): AxumPath<String>,
    Json(request): Json<ChatRequest>,
) -> Result<Response, ApiError> {
    let message = request.message.trim();
    if message.is_empty() {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "message must not be empty",
        ));
    }
    let mux = state.multiplexer.as_ref().ok_or_else(|| {
        ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "chat runtime is not configured; set DEFAULT_MODEL and provider credentials",
        )
    })?;
    let key = resolve_session_key(&state.pool, &id).await?;
    let event = InboundEvent::message(key.clone(), Uuid::new_v4().to_string(), message);
    mux.route(event).await.map_err(ApiError::from)?;
    Ok((
        StatusCode::ACCEPTED,
        Json(json!({
            "queued": true,
            "session_id": key.storage_key(),
        })),
    )
        .into_response())
}

async fn session_ws(
    ws: WebSocketUpgrade,
    State(state): State<DashboardState>,
    AxumPath(id): AxumPath<String>,
) -> Result<Response, ApiError> {
    let key = resolve_session_key(&state.pool, &id).await?;
    let storage_id = key.storage_key();
    Ok(ws
        .on_upgrade(move |socket| handle_session_socket(socket, state, key, storage_id))
        .into_response())
}

async fn handle_session_socket(
    mut socket: WebSocket,
    state: DashboardState,
    key: SessionKey,
    storage_id: String,
) {
    let mut events = state.events.subscribe();
    let ready = json!({
        "type": "ready",
        "session_id": storage_id,
        "chat_available": state.multiplexer.is_some(),
    });
    if socket
        .send(WsMessage::Text(ready.to_string().into()))
        .await
        .is_err()
    {
        return;
    }

    loop {
        tokio::select! {
            incoming = socket.recv() => {
                let Some(incoming) = incoming else { break; };
                let Ok(message) = incoming else { break; };
                match message {
                    WsMessage::Text(text) => {
                        let text = text.as_str();
                        let parsed = serde_json::from_str::<Value>(text).unwrap_or_else(|_| json!({"type":"message","content":text}));
                        match parsed.get("type").and_then(Value::as_str).unwrap_or("message") {
                            "message" => {
                                let content = parsed.get("content").and_then(Value::as_str).unwrap_or_default().trim();
                                if content.is_empty() {
                                    let _ = send_ws_error(&mut socket, "message must not be empty").await;
                                    continue;
                                }
                                let Some(mux) = state.multiplexer.as_ref() else {
                                    let _ = send_ws_error(&mut socket, "chat runtime is not configured").await;
                                    continue;
                                };
                                let event = InboundEvent::message(key.clone(), Uuid::new_v4().to_string(), content);
                                if let Err(error) = mux.route(event).await {
                                    let _ = send_ws_error(&mut socket, &error.to_string()).await;
                                } else {
                                    let ack = json!({"type":"accepted","session_id":key.storage_key()});
                                    if socket.send(WsMessage::Text(ack.to_string().into())).await.is_err() {
                                        break;
                                    }
                                }
                            }
                            "stop" => {
                                if let Some(mux) = state.multiplexer.as_ref() {
                                    match mux.stop(&key).await {
                                        Ok(stopped) => {
                                            let ack = json!({"type":"stopped","stopped":stopped});
                                            if socket.send(WsMessage::Text(ack.to_string().into())).await.is_err() { break; }
                                        }
                                        Err(error) => { let _ = send_ws_error(&mut socket, &error.to_string()).await; }
                                    }
                                }
                            }
                            "ping" => {
                                if socket.send(WsMessage::Text(json!({"type":"pong"}).to_string().into())).await.is_err() { break; }
                            }
                            other => {
                                let _ = send_ws_error(&mut socket, &format!("unsupported websocket message type {other}")).await;
                            }
                        }
                    }
                    WsMessage::Ping(payload) => {
                        if socket.send(WsMessage::Pong(payload)).await.is_err() { break; }
                    }
                    WsMessage::Close(_) => break,
                    _ => {}
                }
            }
            outbound = events.recv() => {
                match outbound {
                    Ok(action) => {
                        if action_session(&action).is_some_and(|session| session.storage_key() == storage_id) {
                            let payload = json!({"type":"event","event":action});
                            if socket.send(WsMessage::Text(payload.to_string().into())).await.is_err() {
                                break;
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        let warning = json!({"type":"warning","message":format!("skipped {skipped} dashboard events")});
                        if socket.send(WsMessage::Text(warning.to_string().into())).await.is_err() { break; }
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        }
    }
}

async fn send_ws_error(socket: &mut WebSocket, message: &str) -> Result<(), axum::Error> {
    socket
        .send(WsMessage::Text(
            json!({"type":"error","message":message}).to_string().into(),
        ))
        .await
}

fn web_session_key(id: &str) -> SessionKey {
    SessionKey::new(
        "web",
        None::<String>,
        id,
        None::<String>,
        "dashboard",
    )
}

async fn resolve_storage_id(pool: &SqlitePool, id: &str) -> Result<String, sqlx::Error> {
    let exact = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM sessions WHERE session_key = ?",
    )
    .bind(id)
    .fetch_one(pool)
    .await?;
    if exact != 0 {
        return Ok(id.to_owned());
    }
    Ok(web_session_key(id).storage_key())
}

async fn resolve_session_key(pool: &SqlitePool, id: &str) -> Result<SessionKey, ApiError> {
    let storage_id = resolve_storage_id(pool, id).await?;
    if let Some(key) = parse_storage_key(&storage_id) {
        return Ok(key);
    }
    if let Some(row) = fetch_session_row(pool, &storage_id).await? {
        return Ok(SessionKey::new(
            row.platform,
            row.guild_id,
            row.channel_id,
            row.thread_id,
            row.user_id,
        ));
    }
    Ok(web_session_key(id))
}

fn parse_storage_key(input: &str) -> Option<SessionKey> {
    fn read_component(input: &str, index: &mut usize) -> Option<Option<String>> {
        let bytes = input.as_bytes();
        if *index >= bytes.len() {
            return None;
        }
        if bytes[*index] == b'-' {
            *index += 1;
            return Some(None);
        }
        let length_start = *index;
        while *index < bytes.len() && bytes[*index].is_ascii_digit() {
            *index += 1;
        }
        if *index == length_start || bytes.get(*index) != Some(&b':') {
            return None;
        }
        let length = input.get(length_start..*index)?.parse::<usize>().ok()?;
        *index += 1;
        let end = index.checked_add(length)?;
        let value = input.get(*index..end)?.to_owned();
        *index = end;
        Some(Some(value))
    }

    let mut index = 0usize;
    let mut values = Vec::new();
    while index < input.len() {
        values.push(read_component(input, &mut index)?);
        if index == input.len() {
            break;
        }
        if input.as_bytes().get(index) != Some(&b'|') {
            return None;
        }
        index += 1;
    }
    if values.len() != 5 && values.len() != 6 {
        return None;
    }
    Some(SessionKey {
        platform: values.first()?.clone()?,
        guild_id: values.get(1)?.clone(),
        channel_id: values.get(2)?.clone()?,
        thread_id: values.get(3)?.clone(),
        user_id: values.get(4)?.clone()?,
        bot_id: values.get(5).cloned().flatten(),
    })
}

#[derive(Debug, Deserialize)]
struct CronJobInput {
    id: Option<String>,
    expression: String,
    #[serde(default)]
    payload: Value,
    session_key: Option<String>,
    enabled: Option<bool>,
}

async fn list_cron_jobs(
    State(state): State<DashboardState>,
) -> Result<Json<Value>, ApiError> {
    let jobs = sqlx::query_as::<_, CronJob>(
        "SELECT * FROM cron_jobs ORDER BY enabled DESC, next_run_at IS NULL, next_run_at, id",
    )
    .fetch_all(&state.pool)
    .await?;
    Ok(Json(json!({
        "items": jobs.into_iter().map(cron_job_view).collect::<Result<Vec<_>, _>>()?,
    })))
}

async fn create_cron_job(
    State(state): State<DashboardState>,
    Json(input): Json<CronJobInput>,
) -> Result<Response, ApiError> {
    if input.expression.trim().is_empty() {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "expression must not be empty",
        ));
    }
    let spec = CronJobSpec {
        expression: input.expression,
        payload: input.payload,
        session_key: input.session_key,
    };
    let job = if let Some(id) = input.id {
        state.scheduler.register_with_id(id, spec).await?
    } else {
        state.scheduler.register(spec).await?
    };
    if input.enabled == Some(false) {
        state.scheduler.pause(&job.id).await?;
    }
    let job = state
        .scheduler
        .get(&job.id)
        .await?
        .ok_or_else(|| ApiError::not_found("cron job"))?;
    Ok((StatusCode::CREATED, Json(cron_job_view(job)?)).into_response())
}

async fn get_cron_job(
    State(state): State<DashboardState>,
    AxumPath(id): AxumPath<String>,
) -> Result<Json<Value>, ApiError> {
    let job = state
        .scheduler
        .get(&id)
        .await?
        .ok_or_else(|| ApiError::not_found("cron job"))?;
    Ok(Json(cron_job_view(job)?))
}

async fn update_cron_job(
    State(state): State<DashboardState>,
    AxumPath(id): AxumPath<String>,
    Json(input): Json<CronJobInput>,
) -> Result<Json<Value>, ApiError> {
    if state.scheduler.get(&id).await?.is_none() {
        return Err(ApiError::not_found("cron job"));
    }
    let spec = CronJobSpec {
        expression: input.expression,
        payload: input.payload,
        session_key: input.session_key.clone(),
    };
    state.scheduler.register_with_id(&id, spec).await?;
    sqlx::query("UPDATE cron_jobs SET session_key = ?, updated_at = ? WHERE id = ?")
        .bind(input.session_key)
        .bind(Utc::now())
        .bind(&id)
        .execute(&state.pool)
        .await?;
    match input.enabled {
        Some(true) => {
            state.scheduler.resume(&id).await?;
        }
        Some(false) => {
            state.scheduler.pause(&id).await?;
        }
        None => {}
    }
    let job = state
        .scheduler
        .get(&id)
        .await?
        .ok_or_else(|| ApiError::not_found("cron job"))?;
    Ok(Json(cron_job_view(job)?))
}

async fn delete_cron_job(
    State(state): State<DashboardState>,
    AxumPath(id): AxumPath<String>,
) -> Result<Response, ApiError> {
    if !state.scheduler.delete(&id).await? {
        return Err(ApiError::not_found("cron job"));
    }
    Ok(StatusCode::NO_CONTENT.into_response())
}

async fn trigger_cron_job(
    State(state): State<DashboardState>,
    AxumPath(id): AxumPath<String>,
) -> Result<Json<Value>, ApiError> {
    if !state.scheduler.trigger(&id).await? {
        return Err(ApiError::new(
            StatusCode::CONFLICT,
            "cron job does not exist or is already running",
        ));
    }
    Ok(Json(json!({"triggered": true, "id": id})))
}

async fn pause_cron_job(
    State(state): State<DashboardState>,
    AxumPath(id): AxumPath<String>,
) -> Result<Json<Value>, ApiError> {
    if !state.scheduler.pause(&id).await? {
        return Err(ApiError::not_found("cron job"));
    }
    Ok(Json(json!({"paused": true, "id": id})))
}

async fn resume_cron_job(
    State(state): State<DashboardState>,
    AxumPath(id): AxumPath<String>,
) -> Result<Json<Value>, ApiError> {
    if !state.scheduler.resume(&id).await? {
        return Err(ApiError::not_found("cron job"));
    }
    Ok(Json(json!({"resumed": true, "id": id})))
}

fn cron_job_view(job: CronJob) -> Result<Value, ApiError> {
    let payload = job
        .payload()
        .map_err(|error| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(json!({
        "id": job.id,
        "session_key": job.session_key,
        "expression": job.expression,
        "payload": payload,
        "enabled": job.enabled,
        "next_run_at": job.next_run_at,
        "created_at": job.created_at,
        "updated_at": job.updated_at,
    }))
}

#[derive(Debug, Serialize, FromRow)]
struct CronRunRow {
    run_id: String,
    job_id: String,
    claim_token: String,
    lease_expires_at: String,
    started_at: String,
    completed_at: Option<String>,
    status: String,
    attempt: i64,
    error: Option<String>,
    owner_pid: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct CronRunsQuery {
    job_id: Option<String>,
    status: Option<String>,
    limit: Option<u32>,
}

async fn list_cron_runs(
    State(state): State<DashboardState>,
    Query(query): Query<CronRunsQuery>,
) -> Result<Json<Value>, ApiError> {
    let limit = query.limit.unwrap_or(100).clamp(1, 500);
    let rows = match (query.job_id.as_deref(), query.status.as_deref()) {
        (Some(job_id), Some(status)) => {
            sqlx::query_as::<_, CronRunRow>(
                "SELECT run_id, job_id, claim_token, lease_expires_at, started_at, completed_at, status, attempt, error, owner_pid
                 FROM cron_runs WHERE job_id = ? AND status = ? ORDER BY started_at DESC LIMIT ?",
            )
            .bind(job_id)
            .bind(status)
            .bind(i64::from(limit))
            .fetch_all(&state.pool)
            .await?
        }
        (Some(job_id), None) => {
            sqlx::query_as::<_, CronRunRow>(
                "SELECT run_id, job_id, claim_token, lease_expires_at, started_at, completed_at, status, attempt, error, owner_pid
                 FROM cron_runs WHERE job_id = ? ORDER BY started_at DESC LIMIT ?",
            )
            .bind(job_id)
            .bind(i64::from(limit))
            .fetch_all(&state.pool)
            .await?
        }
        (None, Some(status)) => {
            sqlx::query_as::<_, CronRunRow>(
                "SELECT run_id, job_id, claim_token, lease_expires_at, started_at, completed_at, status, attempt, error, owner_pid
                 FROM cron_runs WHERE status = ? ORDER BY started_at DESC LIMIT ?",
            )
            .bind(status)
            .bind(i64::from(limit))
            .fetch_all(&state.pool)
            .await?
        }
        (None, None) => {
            sqlx::query_as::<_, CronRunRow>(
                "SELECT run_id, job_id, claim_token, lease_expires_at, started_at, completed_at, status, attempt, error, owner_pid
                 FROM cron_runs ORDER BY started_at DESC LIMIT ?",
            )
            .bind(i64::from(limit))
            .fetch_all(&state.pool)
            .await?
        }
    };
    Ok(Json(json!({"items": rows})))
}

async fn get_config(State(state): State<DashboardState>) -> Json<Value> {
    Json(state.config)
}

async fn list_tools(State(state): State<DashboardState>) -> Json<Value> {
    let mut names = state.tools.names();
    names.sort();
    let items = names
        .into_iter()
        .filter_map(|name| {
            state.tools.get(&name).map(|tool| {
                json!({
                    "name": tool.name(),
                    "description": tool.description(),
                    "input_schema": tool.input_schema(),
                })
            })
        })
        .collect::<Vec<_>>();
    Json(json!({"items": items}))
}

#[derive(Clone, Debug, Serialize)]
struct SkillView {
    name: String,
    source: String,
    path: String,
    description: Option<String>,
}

async fn list_skills(State(state): State<DashboardState>) -> Json<Value> {
    let roots = state.skill_roots.clone();
    let items = tokio::task::spawn_blocking(move || discover_skills(&roots))
        .await
        .unwrap_or_default();
    Json(json!({"items": items}))
}

fn discover_skills(roots: &[PathBuf]) -> Vec<SkillView> {
    let mut seen = HashSet::new();
    let mut skills = Vec::new();
    for root in roots {
        collect_skills(root, root, 0, &mut seen, &mut skills);
    }
    skills.sort_by(|a, b| a.name.cmp(&b.name).then_with(|| a.path.cmp(&b.path)));
    skills
}

fn collect_skills(
    source_root: &Path,
    current: &Path,
    depth: usize,
    seen: &mut HashSet<PathBuf>,
    skills: &mut Vec<SkillView>,
) {
    if depth > 4 || !current.is_dir() {
        return;
    }
    let Ok(entries) = std::fs::read_dir(current) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_skills(source_root, &path, depth + 1, seen, skills);
            continue;
        }
        let is_skill = path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.eq_ignore_ascii_case("skill.md"));
        if !is_skill {
            continue;
        }
        let canonical = path.canonicalize().unwrap_or_else(|_| path.clone());
        if !seen.insert(canonical) {
            continue;
        }
        let content = std::fs::read_to_string(&path).unwrap_or_default();
        let description = content
            .lines()
            .map(str::trim)
            .find(|line| !line.is_empty() && !line.starts_with('#') && !line.starts_with("---"))
            .map(|line| line.chars().take(240).collect::<String>());
        let name = path
            .parent()
            .and_then(Path::file_name)
            .and_then(|name| name.to_str())
            .unwrap_or("skill")
            .to_owned();
        skills.push(SkillView {
            name,
            source: source_root.display().to_string(),
            path: path.display().to_string(),
            description,
        });
    }
}

#[derive(Debug, Serialize, Deserialize, FromRow)]
pub struct BotProfileRow {
    pub bot_id: String,
    pub name: String,
    pub model: Option<String>,
    pub system_prompt: Option<String>,
    pub enabled_toolsets: Option<String>,
    pub custom_settings_json: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Deserialize)]
struct UpdateBotPayload {
    name: Option<String>,
    model: Option<String>,
    system_prompt: Option<String>,
    enabled_toolsets: Option<Vec<String>>,
    custom_settings: Option<Value>,
}

async fn list_bots(State(state): State<DashboardState>) -> Result<Json<Value>, ApiError> {
    let mut profiles = sqlx::query_as::<_, BotProfileRow>(
        "SELECT bot_id, name, model, system_prompt, enabled_toolsets, custom_settings_json, created_at, updated_at FROM bot_profiles ORDER BY name ASC"
    )
    .fetch_all(&state.pool)
    .await
    .unwrap_or_default();

    let mut configured_ids = HashSet::new();
    for p in &profiles {
        configured_ids.insert(p.bot_id.clone());
    }

    let known_bots = vec![
        ("1465631383862120451", "wawabot"),
        ("1529539440589013182", "실피"),
        ("1529738312833830984", "에리스"),
    ];

    for (bid, bname) in known_bots {
        if !configured_ids.contains(bid) {
            let row = BotProfileRow {
                bot_id: bid.to_string(),
                name: bname.to_string(),
                model: None,
                system_prompt: None,
                enabled_toolsets: None,
                custom_settings_json: "{}".to_string(),
                created_at: Utc::now().to_rfc3339(),
                updated_at: Utc::now().to_rfc3339(),
            };
            profiles.push(row);
        }
    }

    let items = profiles
        .into_iter()
        .map(|b| {
            json!({
                "bot_id": b.bot_id,
                "name": b.name,
                "model": b.model,
                "system_prompt": b.system_prompt,
                "enabled_toolsets": b.enabled_toolsets.map(|t| t.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect::<Vec<_>>()),
                "custom_settings": serde_json::from_str::<Value>(&b.custom_settings_json).unwrap_or_else(|_| json!({})),
                "created_at": b.created_at,
                "updated_at": b.updated_at,
            })
        })
        .collect::<Vec<_>>();

    Ok(Json(json!({
        "items": items,
        "total": items.len(),
    })))
}

async fn get_bot(
    State(state): State<DashboardState>,
    AxumPath(bot_id): AxumPath<String>,
) -> Result<Json<Value>, ApiError> {
    let profile = sqlx::query_as::<_, BotProfileRow>(
        "SELECT bot_id, name, model, system_prompt, enabled_toolsets, custom_settings_json, created_at, updated_at FROM bot_profiles WHERE bot_id = ?"
    )
    .bind(&bot_id)
    .fetch_optional(&state.pool)
    .await?;

    let item = match profile {
        Some(b) => json!({
            "bot_id": b.bot_id,
            "name": b.name,
            "model": b.model,
            "system_prompt": b.system_prompt,
            "enabled_toolsets": b.enabled_toolsets.map(|t| t.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect::<Vec<_>>()),
            "custom_settings": serde_json::from_str::<Value>(&b.custom_settings_json).unwrap_or_else(|_| json!({})),
            "created_at": b.created_at,
            "updated_at": b.updated_at,
        }),
        None => {
            let default_name = match bot_id.as_str() {
                "1465631383862120451" => "wawabot",
                "1529539440589013182" => "실피",
                "1529738312833830984" => "에리스",
                _ => "Discord Bot",
            };
            json!({
                "bot_id": bot_id,
                "name": default_name,
                "model": null,
                "system_prompt": null,
                "enabled_toolsets": null,
                "custom_settings": {},
                "created_at": Utc::now().to_rfc3339(),
                "updated_at": Utc::now().to_rfc3339(),
            })
        }
    };

    Ok(Json(item))
}

#[derive(Debug, Deserialize)]
struct CreateBotPayload {
    bot_id: String,
    name: String,
    model: Option<String>,
    system_prompt: Option<String>,
    enabled_toolsets: Option<Vec<String>>,
    custom_settings: Option<Value>,
}

async fn create_bot(
    State(state): State<DashboardState>,
    Json(payload): Json<CreateBotPayload>,
) -> Result<Json<Value>, ApiError> {
    let bot_id = payload.bot_id.trim();
    if bot_id.is_empty() {
        return Err(ApiError::new(StatusCode::BAD_REQUEST, "bot_id cannot be empty"));
    }
    let name = payload.name.trim();
    if name.is_empty() {
        return Err(ApiError::new(StatusCode::BAD_REQUEST, "name cannot be empty"));
    }
    let toolsets_str = payload.enabled_toolsets.map(|ts| ts.join(","));
    let settings_json = payload.custom_settings.map(|cs| cs.to_string()).unwrap_or_else(|| "{}".to_string());
    let now = Utc::now().to_rfc3339();

    sqlx::query(
        "INSERT INTO bot_profiles (bot_id, name, model, system_prompt, enabled_toolsets, custom_settings_json, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(bot_id) DO UPDATE SET
            name = excluded.name,
            model = excluded.model,
            system_prompt = excluded.system_prompt,
            enabled_toolsets = excluded.enabled_toolsets,
            custom_settings_json = excluded.custom_settings_json,
            updated_at = excluded.updated_at"
    )
    .bind(bot_id)
    .bind(name)
    .bind(&payload.model)
    .bind(&payload.system_prompt)
    .bind(&toolsets_str)
    .bind(&settings_json)
    .bind(&now)
    .bind(&now)
    .execute(&state.pool)
    .await?;

    Ok(Json(json!({
        "status": "created",
        "bot_id": bot_id,
        "name": name,
    })))
}

async fn delete_bot(
    State(state): State<DashboardState>,
    AxumPath(bot_id): AxumPath<String>,
) -> Result<Json<Value>, ApiError> {
    let result = sqlx::query("DELETE FROM bot_profiles WHERE bot_id = ?")
        .bind(&bot_id)
        .execute(&state.pool)
        .await?;

    if result.rows_affected() == 0 {
        return Err(ApiError::not_found("bot profile"));
    }

    Ok(Json(json!({
        "status": "deleted",
        "bot_id": bot_id,
    })))
}

async fn update_bot(
    State(state): State<DashboardState>,
    AxumPath(bot_id): AxumPath<String>,
    Json(payload): Json<UpdateBotPayload>,
) -> Result<Json<Value>, ApiError> {
    let name = payload.name.unwrap_or_else(|| match bot_id.as_str() {
        "1465631383862120451" => "wawabot".to_string(),
        "1529539440589013182" => "실피".to_string(),
        "1529738312833830984" => "에리스".to_string(),
        _ => "Discord Bot".to_string(),
    });
    let toolsets_str = payload.enabled_toolsets.map(|ts| ts.join(","));
    let settings_json = payload.custom_settings.map(|cs| cs.to_string()).unwrap_or_else(|| "{}".to_string());
    let now = Utc::now().to_rfc3339();

    sqlx::query(
        "INSERT INTO bot_profiles (bot_id, name, model, system_prompt, enabled_toolsets, custom_settings_json, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(bot_id) DO UPDATE SET
            name = excluded.name,
            model = excluded.model,
            system_prompt = excluded.system_prompt,
            enabled_toolsets = excluded.enabled_toolsets,
            custom_settings_json = excluded.custom_settings_json,
            updated_at = excluded.updated_at"
    )
    .bind(&bot_id)
    .bind(&name)
    .bind(&payload.model)
    .bind(&payload.system_prompt)
    .bind(&toolsets_str)
    .bind(&settings_json)
    .bind(&now)
    .bind(&now)
    .execute(&state.pool)
    .await?;

    get_bot(State(state), AxumPath(bot_id)).await
}

#[derive(Debug, Serialize, FromRow)]
struct MemoryRow {
    id: String,
    session_key: String,
    content: String,
    metadata_json: String,
    created_at: String,
    updated_at: String,
}

async fn list_memory(
    State(state): State<DashboardState>,
    Query(query): Query<PageQuery>,
) -> Result<Json<Value>, ApiError> {
    let (page, per_page, offset) = query.values();
    let search = query
        .search
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| format!("%{value}%"));
    let (rows, total) = if let Some(pattern) = search {
        let rows = sqlx::query_as::<_, MemoryRow>(
            "SELECT id, session_key, content, metadata_json, created_at, updated_at FROM memories
             WHERE content LIKE ? OR session_key LIKE ? ORDER BY updated_at DESC LIMIT ? OFFSET ?",
        )
        .bind(&pattern)
        .bind(&pattern)
        .bind(i64::from(per_page))
        .bind(offset)
        .fetch_all(&state.pool)
        .await?;
        let total = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM memories WHERE content LIKE ? OR session_key LIKE ?",
        )
        .bind(&pattern)
        .bind(&pattern)
        .fetch_one(&state.pool)
        .await?;
        (rows, total)
    } else {
        let rows = sqlx::query_as::<_, MemoryRow>(
            "SELECT id, session_key, content, metadata_json, created_at, updated_at FROM memories
             ORDER BY updated_at DESC LIMIT ? OFFSET ?",
        )
        .bind(i64::from(per_page))
        .bind(offset)
        .fetch_all(&state.pool)
        .await?;
        let total = scalar_count(&state.pool, "SELECT COUNT(*) FROM memories").await?;
        (rows, total)
    };
    let items = rows
        .into_iter()
        .map(|row| {
            json!({
                "id": row.id,
                "session_key": row.session_key,
                "content": row.content,
                "metadata": serde_json::from_str::<Value>(&row.metadata_json).unwrap_or_else(|_| json!({})),
                "created_at": row.created_at,
                "updated_at": row.updated_at,
            })
        })
        .collect::<Vec<_>>();
    Ok(Json(json!({
        "items": items,
        "page": page,
        "per_page": per_page,
        "total": total,
    })))
}

async fn list_pending_approvals(State(state): State<DashboardState>) -> Json<Value> {
    Json(json!({
        "items": state.events.pending_approvals().await,
        "pending_count": state.approvals.pending_count().await,
    }))
}

#[derive(Debug, Deserialize)]
struct ResolveApprovalRequest {
    decision: String,
}

async fn resolve_approval(
    State(state): State<DashboardState>,
    AxumPath(id): AxumPath<Uuid>,
    Json(request): Json<ResolveApprovalRequest>,
) -> Result<Json<Value>, ApiError> {
    let suffix = match request.decision.trim().to_ascii_lowercase().as_str() {
        "once" | "allow_once" => "once",
        "session" | "allow_session" => "session",
        "always" | "allow_always" => "always",
        "deny" | "reject" => "deny",
        _ => {
            return Err(ApiError::new(
                StatusCode::BAD_REQUEST,
                "decision must be one of Once, Session, Always, or Deny",
            ));
        }
    };
    let custom_id = format!("omon:approval:{id}:{suffix}");
    if !state.approvals.resolve_custom_id(&custom_id).await {
        return Err(ApiError::not_found("approval request"));
    }
    state.events.remove_pending(id).await;
    Ok(Json(json!({"resolved": true, "id": id, "decision": suffix})))
}

#[derive(Debug, Serialize, FromRow)]
struct ApprovalAllowlistRow {
    pattern_key: String,
    created_at: String,
}

async fn list_approval_allowlist(
    State(state): State<DashboardState>,
) -> Result<Json<Value>, ApiError> {
    let rows = sqlx::query_as::<_, ApprovalAllowlistRow>(
        "SELECT pattern_key, created_at FROM approval_allowlist ORDER BY created_at DESC, pattern_key",
    )
    .fetch_all(&state.pool)
    .await?;
    Ok(Json(json!({"items": rows})))
}

#[derive(Debug, Deserialize)]
struct LogsQuery {
    limit: Option<u32>,
    level: Option<String>,
    search: Option<String>,
}

async fn list_logs(
    State(state): State<DashboardState>,
    Query(query): Query<LogsQuery>,
) -> Json<Value> {
    let limit = query.limit.unwrap_or(250).clamp(1, 1_000) as usize;
    let level = query.level.map(|value| value.to_ascii_uppercase());
    let search = query.search.map(|value| value.to_ascii_lowercase());
    let mut entries = state
        .logs
        .snapshot()
        .into_iter()
        .filter(|entry| {
            level
                .as_deref()
                .is_none_or(|level| entry.level.eq_ignore_ascii_case(level))
                && search.as_deref().is_none_or(|search| {
                    entry.message.to_ascii_lowercase().contains(search)
                        || entry.target.to_ascii_lowercase().contains(search)
                        || entry.fields.to_string().to_ascii_lowercase().contains(search)
                })
        })
        .collect::<Vec<_>>();
    if entries.len() > limit {
        entries.drain(0..entries.len() - limit);
    }
    Json(json!({"items": entries}))
}

async fn logs_ws(ws: WebSocketUpgrade, State(state): State<DashboardState>) -> Response {
    ws.on_upgrade(move |socket| handle_logs_socket(socket, state.logs))
        .into_response()
}

async fn handle_logs_socket(mut socket: WebSocket, logs: DashboardLogStore) {
    let tail = logs.snapshot();
    for entry in tail.into_iter().rev().take(100).collect::<Vec<_>>().into_iter().rev() {
        if socket
            .send(WsMessage::Text(
                json!({"type":"log","entry":entry}).to_string().into(),
            ))
            .await
            .is_err()
        {
            return;
        }
    }
    let mut receiver = logs.subscribe();
    loop {
        tokio::select! {
            entry = receiver.recv() => {
                match entry {
                    Ok(entry) => {
                        if socket.send(WsMessage::Text(json!({"type":"log","entry":entry}).to_string().into())).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        if socket.send(WsMessage::Text(json!({"type":"warning","message":format!("skipped {skipped} log entries")}).to_string().into())).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
            incoming = socket.recv() => {
                let Some(incoming) = incoming else { break; };
                match incoming {
                    Ok(WsMessage::Ping(payload)) => {
                        if socket.send(WsMessage::Pong(payload)).await.is_err() { break; }
                    }
                    Ok(WsMessage::Close(_)) | Err(_) => break,
                    _ => {}
                }
            }
        }
    }
}

async fn serve_static(State(state): State<DashboardState>, uri: Uri) -> Response {
    if uri.path().starts_with("/api/") {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"error":"API endpoint not found"})),
        )
            .into_response();
    }
    let requested = uri.path().trim_start_matches('/');
    let relative = if requested.is_empty() {
        PathBuf::from("index.html")
    } else {
        PathBuf::from(requested)
    };
    if safe_relative_path(&relative) {
        let path = state.web_root.join(&relative);
        if let Ok(bytes) = tokio::fs::read(&path).await {
            return static_response(&path, bytes);
        }
    }
    let index = state.web_root.join("index.html");
    if let Ok(bytes) = tokio::fs::read(&index).await {
        return static_response(&index, bytes);
    }
    fallback_dashboard_html()
}

fn safe_relative_path(path: &Path) -> bool {
    !path.is_absolute()
        && path.components().all(|component| {
            matches!(component, Component::Normal(_) | Component::CurDir)
        })
}

fn static_response(path: &Path, bytes: Vec<u8>) -> Response {
    let content_type = match path.extension().and_then(|ext| ext.to_str()) {
        Some("html") => "text/html; charset=utf-8",
        Some("js") | Some("mjs") => "text/javascript; charset=utf-8",
        Some("css") => "text/css; charset=utf-8",
        Some("json") => "application/json",
        Some("svg") => "image/svg+xml",
        Some("png") => "image/png",
        Some("jpg") | Some("jpeg") => "image/jpeg",
        Some("ico") => "image/x-icon",
        Some("woff2") => "font/woff2",
        _ => "application/octet-stream",
    };
    let mut response = Response::new(Body::from(bytes));
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static(content_type),
    );
    response
}

fn fallback_dashboard_html() -> Response {
    const HTML: &str = r#"<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>omon gateway dashboard</title><style>
:root{color-scheme:dark;background:#090b10;color:#edf0f7;font-family:Inter,ui-sans-serif,system-ui,sans-serif}body{max-width:960px;margin:0 auto;padding:48px 24px}h1{font-size:2rem;margin-bottom:.25rem}.muted{color:#9aa5b5}code{background:#161b25;padding:.15rem .35rem;border-radius:.35rem}.card{border:1px solid #283244;border-radius:16px;padding:20px;margin-top:24px;background:#10141d}a{color:#8ab4ff}li{margin:.45rem 0}</style></head>
<body><h1>omon gateway dashboard</h1><p class="muted">The dashboard API is running, but <code>web/dist</code> was not found. Build the React UI with <code>cd web &amp;&amp; npm install &amp;&amp; npm run build</code>.</p>
<div class="card"><h2>API explorer</h2><ul>
<li><a href="/api/status">/api/status</a> — runtime status</li><li><a href="/api/sessions">/api/sessions</a> — sessions</li><li><a href="/api/cron/jobs">/api/cron/jobs</a> — scheduled jobs</li><li><a href="/api/tools">/api/tools</a> — registered tools</li><li><a href="/api/skills">/api/skills</a> — skills</li><li><a href="/api/approvals/pending">/api/approvals/pending</a> — pending approvals</li><li><a href="/api/logs">/api/logs</a> — recent logs</li></ul></div></body></html>"#;
    let mut response = Response::new(Body::from(HTML));
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/html; charset=utf-8"),
    );
    response
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::body::to_bytes;
    use axum::http::Request;
    use omon_gateway::{PayloadTaskExecutor, ToolRegistry};
    use sqlx::sqlite::SqlitePoolOptions;
    use tower::ServiceExt;

    use super::*;

    async fn test_state() -> DashboardState {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("in-memory database");
        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .expect("migrations");
        let scheduler = CronScheduler::new(pool.clone(), Arc::new(PayloadTaskExecutor));
        let approvals = SmartApprovalGuard::new().with_pool(pool.clone());
        let workspace = std::env::current_dir().expect("current dir");
        DashboardState::new(
            pool,
            None,
            scheduler,
            ToolRegistry::new(),
            approvals,
            WebDashboardDispatcher::new(),
            json!({"model":"test-model","providers":{"openai_api_key_configured":true}}),
            workspace,
            Vec::new(),
            0,
            PathBuf::from("does-not-exist"),
        )
    }

    async fn json_body(response: Response) -> Value {
        let bytes = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        serde_json::from_slice(&bytes).expect("json body")
    }

    #[tokio::test]
    async fn health_and_status_endpoints_report_runtime_state() {
        let app = router(test_state().await);
        let health = app
            .clone()
            .oneshot(Request::builder().uri("/api/health").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(health.status(), StatusCode::OK);
        assert_eq!(json_body(health).await["status"], "ok");

        let status = app
            .oneshot(Request::builder().uri("/api/status").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(status.status(), StatusCode::OK);
        let body = json_body(status).await;
        assert_eq!(body["database"]["sessions"], 0);
        assert_eq!(body["chat_available"], false);
    }

    #[tokio::test]
    async fn sessions_list_and_transcript_are_paginated() {
        let state = test_state().await;
        let key = SessionKey::new("web", None::<String>, "test", None::<String>, "dashboard");
        let storage_key = key.storage_key();
        sqlx::query(
            "INSERT INTO sessions (session_key, platform, guild_id, channel_id, thread_id, user_id, state_json) VALUES (?, 'web', NULL, 'test', NULL, 'dashboard', '{}')",
        )
        .bind(&storage_key)
        .execute(&state.pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO messages (id, session_key, role, content, metadata_json) VALUES (?, ?, 'user', 'hello', '{}')",
        )
        .bind(Uuid::new_v4().to_string())
        .bind(&storage_key)
        .execute(&state.pool)
        .await
        .unwrap();
        let app = router(state);
        let sessions = app
            .clone()
            .oneshot(Request::builder().uri("/api/sessions?per_page=10").body(Body::empty()).unwrap())
            .await
            .unwrap();
        let body = json_body(sessions).await;
        assert_eq!(body["total"], 1);
        assert_eq!(body["items"][0]["platform"], "web");

        let transcript = app
            .oneshot(
                Request::builder()
                    .uri("/api/sessions/test/messages")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(transcript.status(), StatusCode::OK);
        let body = json_body(transcript).await;
        assert_eq!(body["items"][0]["content"], "hello");
    }

    #[tokio::test]
    async fn cron_crud_routes_use_scheduler_semantics() {
        let app = router(test_state().await);
        let create = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/cron/jobs")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"expression":"@every 5m","payload":{"content":"ping"}}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(create.status(), StatusCode::CREATED);
        let created = json_body(create).await;
        let id = created["id"].as_str().unwrap();

        let pause = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/api/cron/jobs/{id}/pause"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(pause.status(), StatusCode::OK);

        let get_job = app
            .oneshot(
                Request::builder()
                    .uri(format!("/api/cron/jobs/{id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = json_body(get_job).await;
        assert_eq!(body["enabled"], false);
    }

    #[tokio::test]
    async fn config_endpoint_never_exposes_provider_secret_values() {
        let app = router(test_state().await);
        let response = app
            .oneshot(Request::builder().uri("/api/config").body(Body::empty()).unwrap())
            .await
            .unwrap();
        let body = json_body(response).await;
        let rendered = body.to_string();
        assert!(!rendered.contains("OPENAI_API_KEY"));
        assert_eq!(body["providers"]["openai_api_key_configured"], true);
    }

    #[test]
    fn canonical_session_key_parser_handles_embedded_separators_and_bot_identity() {
        let original = SessionKey::new(
            "discord",
            Some("guild|x"),
            "channel:1",
            Some("thread|2"),
            "user|3",
        )
        .with_bot_id("bot:4");
        let parsed = parse_storage_key(&original.storage_key()).expect("parse storage key");
        assert_eq!(parsed, original);
    }
}
