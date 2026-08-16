use std::sync::LazyLock;

use regex::Regex;

static GATEWAY_LIFECYCLE_PATTERNS: LazyLock<Vec<Regex>> = LazyLock::new(|| {
    vec![
        // Branch A: hermes / omon gateway restart|stop or omon restart
        Regex::new(r"(?i)\b(?:hermes|omon)\s+(?:gateway\s+)?(?:restart|stop)\b").unwrap(),
        Regex::new(r"(?i)\bomon(?:-gateway)?\s+restart\b").unwrap(),
        Regex::new(r"(?i)\bomon[_\-\s].*restart\b").unwrap(),
        // Branch B: launchctl operations
        Regex::new(r"(?i)\blaunchctl\s+kickstart\b").unwrap(),
        Regex::new(
            r"(?i)\blaunchctl\s+(?:kickstart|unload|load|stop|restart)\b[^\n]*\b(?:omon|hermes)",
        )
        .unwrap(),
        // Branch C: systemctl operations
        Regex::new(
            r"(?i)\bsystemctl\s+(?:-\S+\s+)*(?:restart|stop|start)\b[^\n]*\b(?:omon|hermes)",
        )
        .unwrap(),
        Regex::new(r"(?i)\bsystemctl\s+(?:-\S+\s+)*restart\b").unwrap(),
        // Branch D: pkill / kill / killall targeting gateway
        Regex::new(r"(?i)\bp?kill(?:all)?\b[^\n]*\b(?:omon|hermes)\b[^\n]*\bgateway").unwrap(),
        Regex::new(r"(?i)\bp?kill(?:all)?\b[^\n]*\bgateway\b[^\n]*\b(?:omon|hermes)").unwrap(),
        Regex::new(r"(?i)\bp?kill(?:all)?\b[^\n]*\b(?:omon-gateway|hermes-gateway)\b").unwrap(),
    ]
});

/// Checks if a command specification or prompt contains forbidden gateway lifecycle self-restart operations.
pub fn check_gateway_lifecycle(spec: &str) -> Result<(), String> {
    if spec.trim().is_empty() {
        return Ok(());
    }
    for pattern in GATEWAY_LIFECYCLE_PATTERNS.iter() {
        if pattern.is_match(spec) {
            return Err(format!(
                "gateway lifecycle guard: command/prompt matches forbidden self-restart pattern `{}`",
                pattern.as_str()
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rejects_gateway_lifecycle_self_restart_patterns() {
        let forbidden = [
            "launchctl kickstart gui/501/ai.hermes.gateway",
            "launchctl unload ~/Library/LaunchAgents/com.omon.gateway.plist",
            "launchctl load ~/Library/LaunchAgents/com.omon.gateway.plist",
            "systemctl restart omon-gateway",
            "systemctl restart hermes-gateway.service",
            "systemctl --user restart hermes-gateway",
            "systemctl restart nginx",
            "hermes gateway restart",
            "hermes gateway stop",
            "omon gateway restart",
            "omon restart",
            "omon-gateway restart",
            "pkill -9 omon-gateway",
            "killall hermes-gateway",
            "pkill -f 'omon.*gateway'",
            "kill -9 $(pgrep omon-gateway)",
        ];

        for cmd in forbidden {
            assert!(
                check_gateway_lifecycle(cmd).is_err(),
                "Expected command to be rejected: {cmd}"
            );
        }
    }

    #[test]
    fn test_allows_benign_commands() {
        let allowed = [
            "cargo test",
            "git status",
            "ls -la /tmp",
            "systemctl status postgresql",
            "echo 'hello omon gateway'",
            "python3 -c 'print(\"running cron task\")'",
            "curl -s https://api.weather.com/v1",
            "launchctl list",
        ];

        for cmd in allowed {
            assert!(
                check_gateway_lifecycle(cmd).is_ok(),
                "Expected command to be allowed: {cmd}"
            );
        }
    }
}
