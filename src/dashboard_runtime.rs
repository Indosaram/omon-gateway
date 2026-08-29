// allow: SIZE_OK — dashboard runtime orchestration wiring
use std::env;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use omon_gateway::storage::init_pool;
use omon_gateway::{
    cron_script_timeout_secs_from, ApprovalPolicy, CronScheduler, CronTool,
    DiscordApprovalRequester, FileTool, McpTool, MessageContextPolicyMatrix,
    MessengerPolicyStore, MultiplexerConfig, OmoBackend, OmoBackendConfig, OmoDaemonSupervisor,
    OutboundDispatcher, Result, ScaleToZero, SessionMultiplexer, SmartApprovalGuard, TerminalTool,
    ToolRegistry, validate_agent_backend_env,
};
use serde_json::{json, Value};
use tokio_util::sync::CancellationToken;

use super::dashboard::{
    spawn_server, DashboardSettings, DashboardState, WebDashboardDispatcher,
};

pub async fn run_standalone_cli(settings: DashboardSettings) -> Result<()> {
    settings.validate()?;
    let shutdown = CancellationToken::new();
    let signal_shutdown = shutdown.clone();
    let signal_task = tokio::spawn(async move {
        match tokio::signal::ctrl_c().await {
            Ok(()) => signal_shutdown.cancel(),
            Err(error) => tracing::error!(%error, "failed to listen for dashboard Ctrl+C"),
        }
    });
    let result = run_standalone(settings, shutdown, true).await;
    signal_task.abort();
    result
}

pub async fn run_standalone(
    settings: DashboardSettings,
    shutdown: CancellationToken,
    start_scheduler: bool,
) -> Result<()> {
    settings.validate()?;

    let database_url =
        env::var("DATABASE_URL").unwrap_or_else(|_| "sqlite://omon_gateway.db".into());
    let workspace_root = dashboard_workspace_root();
    std::fs::create_dir_all(&workspace_root).map_err(|error| {
        omon_gateway::OmonError::Config(format!(
            "failed to create dashboard workspace {}: {error}",
            workspace_root.display()
        ))
    })?;
    let extra_tool_roots = dashboard_tool_roots();
    let pool = init_pool(&database_url).await?;

    let approval_guard = SmartApprovalGuard::new().with_pool(pool.clone());
    let loaded_allowlist = approval_guard.load_persisted_allowlist().await?;
    tracing::info!(loaded_allowlist, "loaded dashboard approval allowlist");
    let approval_timeout_secs = super::approval_timeout_secs_from(
        super::optional_env("APPROVAL_TIMEOUT_SECS").as_deref(),
    );
    let events = WebDashboardDispatcher::new();
    let dispatcher: Arc<dyn OutboundDispatcher> = Arc::new(events.clone());
    let approval_requester = Arc::new(DiscordApprovalRequester::new(
        approval_guard.clone(),
        Duration::from_secs(approval_timeout_secs),
    ));
    approval_requester.set_dispatcher(dispatcher.clone()).await;

    let approval_policy =
        ApprovalPolicy::parse(super::optional_env("APPROVAL_MODE").as_deref());
    let deny_patterns = env::var("APPROVALS_DENY")
        .or_else(|_| env::var("OMON_APPROVALS_DENY"))
        .ok()
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_owned)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    let mut tools = ToolRegistry::new().with_approval_requester(
        approval_requester.clone(),
        Duration::from_secs(approval_timeout_secs.saturating_add(5)),
    );
    tools.register(
        TerminalTool::new(&workspace_root)
            .with_authorized_roots(extra_tool_roots.clone())
            .with_approval(
                approval_policy,
                approval_requester.clone(),
                Duration::from_secs(approval_timeout_secs.saturating_add(5)),
            )
            .with_deny_globs(deny_patterns),
    );
    tools.register(
        FileTool::new(&workspace_root).with_authorized_roots(extra_tool_roots.clone()),
    );
    tools.register(McpTool::default());
    tools.register(CronTool::new(pool.clone()));
    tools.register(omon_gateway::WebSearchTool);
    tools.register(omon_gateway::WebFetchTool);
    tools.register(omon_gateway::BrowserTool::default());

    let home = env::var_os("HOME").map(PathBuf::from).unwrap_or_default();
    let hermes_root = env::var_os("HERMES_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| home.join(".hermes"));
    let skill_roots = super::hermes_skill_dirs(&hermes_root, &home);
    tools.register(omon_gateway::SkillsTool::new(skill_roots.clone()).with_pool(pool.clone()));

    validate_agent_backend_env()?;
    let omo_config = OmoBackendConfig::from_env()?;
    // Zero-config daemon lifecycle: spawn/keep-alive/kill the local
    // `omo app-server` unless an external one is already serving.
    let _daemon_supervisor = OmoDaemonSupervisor::ensure(&omo_config).await?;
    tracing::info!(
        appserver_url = %omo_config.appserver_url,
        "Configured dashboard agent backend: OMO app-server"
    );
    let omo_backend = Arc::new(
        OmoBackend::new(omo_config, dispatcher.clone()).with_pool(pool.clone()),
    );
    let mux = SessionMultiplexer::with_dispatcher(
        pool.clone(),
        omo_backend.clone(),
        Some(dispatcher.clone()),
        MultiplexerConfig::default(),
    );
    approval_requester.set_heartbeat(mux.activity_heartbeat()).await;
    let scale_to_zero = Some(ScaleToZero::start(mux.clone()));
    let multiplexer = Some(mux);

    // Cron gets its own app-server instance so a long cron turn cannot
    // occupy the interactive daemon (one turn per agent thread).
    let cron_omo_config = OmoBackendConfig::cron_from_env()?;
    let _cron_daemon_supervisor = OmoDaemonSupervisor::ensure(&cron_omo_config).await?;
    tracing::info!(
        appserver_url = %cron_omo_config.appserver_url,
        total_timeout_secs = cron_omo_config.total_timeout.as_secs(),
        "Configured isolated dashboard cron backend: OMO app-server"
    );
    let cron_backend = Arc::new(
        OmoBackend::new(cron_omo_config, dispatcher.clone()).with_pool(pool.clone()),
    );

    let scheduler = CronScheduler::with_dispatcher(
        pool.clone(),
        Arc::new(super::AgentCronExecutor {
            backend: cron_backend,
            workspace_root: workspace_root.clone(),
            pool: pool.clone(),
            cron_script_timeout_secs: cron_script_timeout_secs_from(
                super::optional_env("OMON_CRON_SCRIPT_TIMEOUT_SECS").as_deref(),
            ),
        }),
        dispatcher,
    );
    if start_scheduler {
        scheduler.start().await;
    }

    let bot_connections = configured_bot_count();
    let policy_defaults = MessageContextPolicyMatrix::from_environment();
    let policy_store = MessengerPolicyStore::new(pool.clone());
    let policy_override = policy_store.get_override("discord").await?;
    let effective_policy = policy_store.effective("discord", &policy_defaults).await?;
    let config_view = dashboard_config_view(
        &settings,
        &workspace_root,
        &extra_tool_roots,
        bot_connections,
        &policy_defaults,
        policy_override.as_ref(),
        &effective_policy,
    );
    let state = DashboardState::new(
        pool.clone(),
        multiplexer,
        scheduler.clone(),
        tools,
        approval_guard,
        events,
        config_view,
        workspace_root,
        skill_roots,
        bot_connections,
        settings.web_root.clone(),
    );
    let server = spawn_server(settings, state, shutdown.clone()).await?;
    shutdown.cancelled().await;
    let _ = server.await;
    scheduler.shutdown().await;
    if let Some(scale_to_zero) = scale_to_zero {
        scale_to_zero.shutdown().await;
    }
    pool.close().await;
    Ok(())
}

fn dashboard_workspace_root() -> PathBuf {
    env::var_os("OMON_WORKSPACE_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            env::var_os("HOME")
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("."))
                .join(".omon")
                .join("workspace")
        })
}

fn dashboard_tool_roots() -> Vec<PathBuf> {
    super::optional_env("OMON_TOOL_ROOTS")
        .map(|value| {
            value
                .split(':')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(PathBuf::from)
                .collect::<Vec<_>>()
        })
        .filter(|roots| !roots.is_empty())
        .unwrap_or_else(|| {
            vec![env::var_os("HOME")
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("."))]
        })
}

fn configured_bot_count() -> usize {
    let mut tokens = Vec::new();
    for variable in ["DISCORD_BOT_TOKEN", "DISCORD_BOT_TOKENS"] {
        if let Ok(value) = env::var(variable) {
            for token in value
                .split(',')
                .map(str::trim)
                .filter(|token| !token.is_empty())
            {
                let token_str = token.to_string();
                if !tokens.contains(&token_str) {
                    tokens.push(token_str);
                }
            }
        }
    }
    tokens.len()
}

#[allow(clippy::too_many_arguments)]
fn dashboard_config_view(
    settings: &DashboardSettings,
    workspace_root: &Path,
    tool_roots: &[PathBuf],
    bot_count: usize,
    policy_defaults: &MessageContextPolicyMatrix,
    policy_override: Option<&MessageContextPolicyMatrix>,
    effective_policy: &MessageContextPolicyMatrix,
) -> Value {
    let deny_patterns = env::var("APPROVALS_DENY")
        .or_else(|_| env::var("OMON_APPROVALS_DENY"))
        .ok()
        .map(|value| split_csv(&value))
        .unwrap_or_default();
    json!({
        "model": super::optional_env("DEFAULT_MODEL"),
        "providers": {
            "openai_base_url": super::optional_env("OPENAI_API_BASE"),
            "openai_api_key_configured": super::optional_env("OPENAI_API_KEY").is_some_and(|value| !value.trim().is_empty()),
            "anthropic_base_url": super::optional_env("ANTHROPIC_BASE_URL"),
            "anthropic_api_key_configured": super::optional_env("ANTHROPIC_API_KEY").is_some_and(|value| !value.trim().is_empty()),
        },
        "approval": {
            "policy": super::optional_env("APPROVAL_MODE").unwrap_or_else(|| "ask".into()),
            "timeout_secs": super::approval_timeout_secs_from(super::optional_env("APPROVAL_TIMEOUT_SECS").as_deref()),
            "deny_patterns": deny_patterns,
        },
        "workspace_root": workspace_root,
        "tool_roots": tool_roots,
        "discord": {
            "configured": bot_count > 0,
            "bot_count": bot_count,
            "allowed_users": csv_env("DISCORD_ALLOWED_USERS"),
            "allowed_roles": csv_env("DISCORD_ALLOWED_ROLES"),
            "allowed_channels": csv_env("DISCORD_ALLOWED_CHANNELS"),
            "ignored_channels": csv_env("DISCORD_IGNORED_CHANNELS"),
            "allow_all_users": super::parse_bool_from(super::optional_env("DISCORD_ALLOW_ALL_USERS").as_deref(), false),
            "auto_thread": super::parse_bool_from(super::optional_env("DISCORD_AUTO_THREAD").as_deref(), false),
            "thread_sessions_per_user": super::parse_bool_from(super::optional_env("DISCORD_THREAD_SESSIONS_PER_USER").as_deref(), true),
            "thread_require_mention": super::parse_bool_from(super::optional_env("DISCORD_THREAD_REQUIRE_MENTION").as_deref(), false),
        },
        "message_context_policy": {
            "platform": "discord",
            "environment_defaults": policy_defaults,
            "persisted_override": policy_override,
            "effective": effective_policy,
            "allowed_cross_channels": csv_env("DISCORD_ALLOWED_CHANNELS"),
            "ignored_channels": csv_env("DISCORD_IGNORED_CHANNELS"),
            "override_store": "messenger_policy_overrides",
            "search_backend": "sqlite_fts5+discord_rest_backfill",
        },
        "runtime": {
            "processing_reactions": super::parse_bool_from(super::optional_env("DISCORD_PROCESSING_REACTIONS").as_deref(), true),
            "runtime_footer": super::parse_bool_from(super::optional_env("DISCORD_RUNTIME_FOOTER").as_deref(), false),
            "cron_script_timeout_secs": cron_script_timeout_secs_from(super::optional_env("OMON_CRON_SCRIPT_TIMEOUT_SECS").as_deref()),
        },
        "dashboard": {
            "host": settings.host,
            "port": settings.port,
            "insecure": settings.insecure,
        }
    })
}

fn csv_env(name: &str) -> Vec<String> {
    super::optional_env(name)
        .map(|value| split_csv(&value))
        .unwrap_or_default()
}

fn split_csv(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(str::to_owned)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_csv_trims_and_drops_empty_values() {
        assert_eq!(
            split_csv("one, two, ,three"),
            vec!["one".to_owned(), "two".to_owned(), "three".to_owned()]
        );
    }

    #[test]
    fn dashboard_config_exposes_message_context_policy_matrix() {
        let settings = DashboardSettings {
            enabled: true,
            host: "127.0.0.1".into(),
            port: 9119,
            insecure: false,
            web_root: PathBuf::from("web/dist"),
        };
        let defaults = MessageContextPolicyMatrix::default();
        let effective = MessageContextPolicyMatrix {
            allow_dm_reads: false,
            ..defaults.clone()
        };
        let value = dashboard_config_view(
            &settings,
            Path::new("workspace"),
            &[],
            1,
            &defaults,
            Some(&effective),
            &effective,
        );
        assert_eq!(
            value["message_context_policy"]["effective"]["allow_dm_reads"],
            false
        );
        assert_eq!(
            value["message_context_policy"]["search_backend"],
            "sqlite_fts5+discord_rest_backfill"
        );
    }
}
