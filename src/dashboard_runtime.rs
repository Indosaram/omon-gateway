use std::collections::HashMap;
use std::env;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use omon_gateway::storage::init_pool;
use omon_gateway::{
    cron_script_timeout_secs_from, ApprovalPolicy, CronScheduler, CronTool,
    DiscordApprovalRequester, FileTool, LlmClient, LlmConfig, LlmProvider, McpTool, MemoryStore,
    MultiplexerConfig, OutboundDispatcher, PayloadTaskExecutor, Result, ScaleToZero,
    SessionMultiplexer, SmartApprovalGuard, TerminalTool, ToolRegistry,
};
use parking_lot::Mutex as ParkingMutex;
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

    let database_url = env::var("DATABASE_URL").unwrap_or_else(|_| "sqlite://omon_gateway.db".into());
    let workspace_root = dashboard_workspace_root();
    std::fs::create_dir_all(&workspace_root).map_err(|error| {
        omon_gateway::OmonError::Config(format!(
            "failed to create dashboard workspace {}: {error}",
            workspace_root.display()
        ))
    })?;
    let extra_tool_roots = dashboard_tool_roots();
    let pool = init_pool(&database_url).await?;
    let memory = MemoryStore::new(pool.clone());

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

    let model = super::optional_env("DEFAULT_MODEL");
    let mut scale_to_zero = None;
    let mut runner = None;
    let multiplexer = if let Some(model) = model.as_deref() {
        let llm = LlmClient::new(llm_config_from_environment(model))?;
        let live_runner = Arc::new(super::LiveAgentRunner {
            pool: pool.clone(),
            memory,
            tools: tools.clone(),
            llm,
            dispatcher: dispatcher.clone(),
            workspace_root: workspace_root.clone(),
            streams: ParkingMutex::new(HashMap::new()),
            processing_reactions: super::parse_bool_from(
                super::optional_env("DISCORD_PROCESSING_REACTIONS").as_deref(),
                true,
            ),
            runtime_footer: super::parse_bool_from(
                super::optional_env("DISCORD_RUNTIME_FOOTER").as_deref(),
                false,
            ),
        });
        let mux = SessionMultiplexer::with_dispatcher(
            pool.clone(),
            live_runner.clone(),
            Some(dispatcher.clone()),
            MultiplexerConfig::default(),
        );
        approval_requester.set_heartbeat(mux.activity_heartbeat()).await;
        scale_to_zero = Some(ScaleToZero::start(mux.clone()));
        runner = Some(live_runner);
        Some(mux)
    } else {
        tracing::warn!(
            "DEFAULT_MODEL is not configured; dashboard chat is disabled but administrative APIs remain available"
        );
        None
    };

    let scheduler = if let Some(runner) = runner {
        CronScheduler::with_dispatcher(
            pool.clone(),
            Arc::new(super::AgentCronExecutor {
                runner,
                cron_script_timeout_secs: cron_script_timeout_secs_from(
                    super::optional_env("OMON_CRON_SCRIPT_TIMEOUT_SECS").as_deref(),
                ),
            }),
            dispatcher,
        )
    } else {
        CronScheduler::new(pool.clone(), Arc::new(PayloadTaskExecutor))
    };
    if start_scheduler {
        scheduler.start().await;
    }

    let bot_connections = configured_bot_count();
    let config_view = dashboard_config_view(
        &settings,
        &workspace_root,
        &extra_tool_roots,
        bot_connections,
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

fn llm_config_from_environment(model: &str) -> LlmConfig {
    let anthropic = model.starts_with("claude");
    let mut config = LlmConfig::new(
        if anthropic {
            LlmProvider::Anthropic
        } else {
            LlmProvider::OpenAi
        },
        model,
    );
    if anthropic {
        config.base_url = super::optional_env("ANTHROPIC_BASE_URL");
        config.api_key = super::optional_env("ANTHROPIC_API_KEY");
    } else {
        config.base_url = super::optional_env("OPENAI_API_BASE");
        config.api_key = super::optional_env("OPENAI_API_KEY");
    }
    config
}

fn configured_bot_count() -> usize {
    let mut tokens = Vec::new();
    for variable in ["DISCORD_BOT_TOKEN", "DISCORD_BOT_TOKENS"] {
        if let Ok(value) = env::var(variable) {
            for token in value.split(',').map(str::trim).filter(|token| !token.is_empty()) {
                let token_str = token.to_string();
                if !tokens.contains(&token_str) {
                    tokens.push(token_str);
                }
            }
        }
    }
    tokens.len()
}

fn dashboard_config_view(
    settings: &DashboardSettings,
    workspace_root: &Path,
    tool_roots: &[PathBuf],
    bot_count: usize,
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
    fn llm_config_selects_provider_from_model_name() {
        let openai = llm_config_from_environment("gpt-test");
        assert_eq!(openai.provider, LlmProvider::OpenAi);
        let anthropic = llm_config_from_environment("claude-test");
        assert_eq!(anthropic.provider, LlmProvider::Anthropic);
    }

    #[test]
    fn split_csv_trims_and_drops_empty_values() {
        assert_eq!(
            split_csv("one, two, ,three"),
            vec!["one".to_owned(), "two".to_owned(), "three".to_owned()]
        );
    }
}
