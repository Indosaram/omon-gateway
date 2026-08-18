use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct MessageContextOperationLimits {
    pub recent: usize,
    pub search_results: usize,
    pub search_scan: usize,
    pub replies: usize,
}

impl Default for MessageContextOperationLimits {
    fn default() -> Self {
        Self {
            recent: 50,
            search_results: 50,
            search_scan: 500,
            replies: 50,
        }
    }
}

impl MessageContextOperationLimits {
    pub fn normalized(mut self) -> Self {
        self.recent = self.recent.clamp(1, 100);
        self.search_results = self.search_results.clamp(1, 100);
        self.search_scan = self.search_scan.clamp(self.search_results, 2_000);
        self.replies = self.replies.clamp(1, 100);
        self
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct MessageContextPolicyMatrix {
    pub allow_current_conversation_reads: bool,
    pub allow_dm_reads: bool,
    pub allow_cross_channel_reads: bool,
    pub same_workspace_only: bool,
    pub inherit_session_authorization: bool,
    pub limits: MessageContextOperationLimits,
}

impl Default for MessageContextPolicyMatrix {
    fn default() -> Self {
        Self {
            allow_current_conversation_reads: true,
            allow_dm_reads: true,
            // Cross-channel reads still require the platform allowlist. Keeping
            // the capability enabled here preserves the existing behavior while
            // remaining fail-closed when no target is explicitly allowlisted.
            allow_cross_channel_reads: true,
            same_workspace_only: true,
            inherit_session_authorization: true,
            limits: MessageContextOperationLimits::default(),
        }
    }
}

impl MessageContextPolicyMatrix {
    pub fn normalized(mut self) -> Self {
        self.limits = self.limits.normalized();
        self
    }

    pub fn from_environment() -> Self {
        let defaults = Self::default();
        Self {
            allow_current_conversation_reads: env_bool(
                "OMON_MESSAGE_CONTEXT_ALLOW_CURRENT",
                defaults.allow_current_conversation_reads,
            ),
            allow_dm_reads: env_bool("OMON_MESSAGE_CONTEXT_ALLOW_DM", defaults.allow_dm_reads),
            allow_cross_channel_reads: env_bool(
                "OMON_MESSAGE_CONTEXT_ALLOW_CROSS_CHANNEL",
                defaults.allow_cross_channel_reads,
            ),
            same_workspace_only: env_bool(
                "OMON_MESSAGE_CONTEXT_SAME_WORKSPACE_ONLY",
                defaults.same_workspace_only,
            ),
            inherit_session_authorization: env_bool(
                "OMON_MESSAGE_CONTEXT_INHERIT_SESSION_AUTHORIZATION",
                defaults.inherit_session_authorization,
            ),
            limits: MessageContextOperationLimits {
                recent: env_usize("OMON_MESSAGE_CONTEXT_MAX_RECENT", defaults.limits.recent),
                search_results: env_usize(
                    "OMON_MESSAGE_CONTEXT_MAX_SEARCH_RESULTS",
                    defaults.limits.search_results,
                ),
                search_scan: env_usize(
                    "OMON_MESSAGE_CONTEXT_MAX_SEARCH_SCAN",
                    defaults.limits.search_scan,
                ),
                replies: env_usize("OMON_MESSAGE_CONTEXT_MAX_REPLIES", defaults.limits.replies),
            },
        }
        .normalized()
    }
}

fn env_bool(name: &str, default: bool) -> bool {
    std::env::var(name)
        .ok()
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(default)
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .unwrap_or(default)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalization_bounds_operation_limits() {
        let matrix = MessageContextPolicyMatrix {
            limits: MessageContextOperationLimits {
                recent: 0,
                search_results: 500,
                search_scan: 2,
                replies: 0,
            },
            ..Default::default()
        }
        .normalized();
        assert_eq!(matrix.limits.recent, 1);
        assert_eq!(matrix.limits.search_results, 100);
        assert_eq!(matrix.limits.search_scan, 100);
        assert_eq!(matrix.limits.replies, 1);
    }
}
