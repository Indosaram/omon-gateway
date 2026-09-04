//! Delivery-verified ack execution for cron jobs.
//!
//! A cron payload may carry an `ack_command` (e.g. the KakaoTalk digest
//! checkpoint commit). The gateway runs it AFTER the job's output was
//! successfully delivered, so checkpoint commit no longer depends on the
//! agent choosing to run an ack script. Ack failures are logged and never
//! fail the turn: delivery already happened, and the producer's next-run
//! pending handling covers a missed commit.

use std::process::Output;
use std::time::Duration;

/// Upper bound for an ack command. Ack scripts commit local state and must
/// never hang the delivery path.
pub const ACK_COMMAND_TIMEOUT: Duration = Duration::from_secs(60);

/// Run `command` through `sh -c`, bounded by `timeout`.
/// The child is killed when the timeout fires.
pub async fn run_ack_command(command: &str, timeout: Duration) -> std::io::Result<Output> {
    // kill_on_drop: when the timeout drops the in-flight future, the child
    // is killed instead of left running.
    let child = tokio::process::Command::new("sh")
        .arg("-c")
        .arg(command)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()?;
    let result = tokio::time::timeout(timeout, child.wait_with_output()).await;
    match result {
        Ok(output) => output,
        Err(_) => Err(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            format!("ack command timed out after {timeout:?}: {command}"),
        )),
    }
}

/// Run an ack command and log the outcome. Never propagates an error:
/// by the time an ack runs, delivery already succeeded, and the
/// producer-side pending handling covers a missed checkpoint commit.
pub async fn run_ack_logged(command: &str) {
    match run_ack_command(command, ACK_COMMAND_TIMEOUT).await {
        Ok(output) if output.status.success() => {
            tracing::info!(
                target: "cron_ack",
                stdout = %String::from_utf8_lossy(&output.stdout),
                "cron ack command succeeded"
            );
        }
        Ok(output) => {
            tracing::warn!(
                target: "cron_ack",
                code = output.status.code(),
                stderr = %String::from_utf8_lossy(&output.stderr),
                "cron ack command failed"
            );
        }
        Err(error) => {
            tracing::warn!(target: "cron_ack", %error, "cron ack command did not complete");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn runs_command_and_captures_output() {
        let output = run_ack_command("printf hello", Duration::from_secs(5))
            .await
            .expect("ack command must run");
        assert!(output.status.success());
        assert_eq!(String::from_utf8_lossy(&output.stdout).trim(), "hello");
    }

    #[tokio::test]
    async fn failing_command_reports_failure_without_panicking() {
        let output = run_ack_command("exit 3", Duration::from_secs(5))
            .await
            .expect("failing command still yields output");
        assert!(!output.status.success());
        assert_eq!(output.status.code(), Some(3));
    }

    #[tokio::test]
    async fn timeout_bounds_runaway_command() {
        let result = run_ack_command("sleep 30", Duration::from_millis(200)).await;
        assert!(
            result.is_err(),
            "runaway ack command must be killed by the timeout"
        );
    }
}
