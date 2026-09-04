use clap::{Parser, Subcommand};
use omon_gateway::migrate::MigrateArgs;
use omon_gateway::{OmonError, Result};
use tokio_util::sync::CancellationToken;

#[allow(dead_code, private_interfaces)]
mod legacy {
    include!("main.rs");

    pub async fn run_gateway_public() -> Result<()> {
        run_gateway().await
    }

    #[allow(unused_imports)]
    pub mod dashboard {
        use tracing_subscriber::util::SubscriberInitExt;

        trait OptionStringTrim {
            fn trim(&self) -> &str;
        }

        impl OptionStringTrim for Option<String> {
            fn trim(&self) -> &str {
                self.as_deref().unwrap_or_default().trim()
            }
        }

        include!("dashboard.rs");
    }

    pub mod dashboard_runtime {
        include!("dashboard_runtime.rs");
    }
}

#[derive(Debug, Parser)]
#[command(name = "omo-gateway")]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run the Discord gateway and, when enabled by environment, the dashboard.
    Run,
    /// Run only the Web Dashboard HTTP/WebSocket server.
    Dashboard(legacy::dashboard::DashboardArgs),
    /// Alias for `dashboard`.
    Serve(legacy::dashboard::DashboardArgs),
    /// Run database migration utilities.
    Migrate(MigrateArgs),
}

impl Cli {
    fn into_command(self) -> Command {
        self.command.unwrap_or(Command::Run)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let _ = dotenvy::dotenv();
    legacy::dashboard::init_tracing();

    match Cli::parse().into_command() {
        Command::Run => run_gateway_with_optional_dashboard().await,
        Command::Dashboard(args) | Command::Serve(args) => {
            legacy::dashboard_runtime::run_standalone_cli(
                legacy::dashboard::DashboardSettings::from_args(args),
            )
            .await
        }
        Command::Migrate(args) => omon_gateway::migrate::run_migrate(args).await,
    }
}

async fn run_gateway_with_optional_dashboard() -> Result<()> {
    let settings = legacy::dashboard::DashboardSettings::from_env();
    if !settings.enabled {
        return legacy::run_gateway_public().await;
    }
    settings.validate()?;

    let shutdown = CancellationToken::new();
    let dashboard_shutdown = shutdown.clone();
    let dashboard = legacy::dashboard_runtime::run_standalone(settings, dashboard_shutdown, false);
    let gateway = legacy::run_gateway_public();
    tokio::pin!(dashboard);
    tokio::pin!(gateway);

    tokio::select! {
        result = &mut gateway => {
            shutdown.cancel();
            dashboard.await?;
            result
        }
        result = &mut dashboard => {
            match result {
                Ok(()) => Err(OmonError::Config("dashboard server stopped unexpectedly while the Discord gateway was running".into())),
                Err(error) => Err(error),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::{Cli, Command};

    #[test]
    fn cli_defaults_to_gateway_run() {
        assert!(matches!(
            Cli::try_parse_from(["omo-gateway"]).unwrap().into_command(),
            Command::Run
        ));
    }

    #[test]
    fn cli_accepts_dashboard_and_serve_alias() {
        let dashboard = Cli::try_parse_from([
            "omo-gateway",
            "dashboard",
            "--host",
            "127.0.0.1",
            "--port",
            "9120",
        ])
        .unwrap();
        match dashboard.into_command() {
            Command::Dashboard(args) => {
                assert_eq!(args.host, "127.0.0.1");
                assert_eq!(args.port, 9120);
            }
            other => panic!("expected dashboard command, got {other:?}"),
        }

        assert!(matches!(
            Cli::try_parse_from(["omo-gateway", "serve", "--insecure"])
                .unwrap()
                .into_command(),
            Command::Serve(_)
        ));
    }
}
