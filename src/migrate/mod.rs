pub mod sys;

#[derive(clap::Args, Debug, Clone)]
pub struct MigrateArgs {
    #[arg(long)]
    pub dry_run: bool,
    #[arg(long)]
    pub no_cutover: bool,
}

pub async fn run_migrate(_args: MigrateArgs) -> crate::Result<()> {
    Ok(())
}
