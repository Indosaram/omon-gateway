use sha2::{Digest, Sha256};
use std::fmt::Write;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentWorkspace {
    pub cwd: PathBuf,
    pub roots: Vec<PathBuf>,
}

fn sanitize(raw: &str) -> String {
    let mut result = String::with_capacity(raw.len());
    let mut last_was_hyphen = false;

    for c in raw.chars() {
        let lower = c.to_ascii_lowercase();
        if lower.is_ascii_alphanumeric() || lower == '_' {
            result.push(lower);
            last_was_hyphen = false;
        } else if !last_was_hyphen {
            result.push('-');
            last_was_hyphen = true;
        }
    }

    result.trim_matches('-').to_string()
}

fn sha256_hex_8(input: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    let hash = hasher.finalize();
    let mut hex = String::with_capacity(8);
    for b in &hash[..4] {
        let _ = write!(&mut hex, "{b:02x}");
    }
    hex
}

pub fn agent_workspace_slug(platform: &str, user_id: &str, bot_id: Option<&str>) -> String {
    let (category, raw_value) = if let Some(bot) = bot_id.filter(|s| !s.is_empty()) {
        ("bot", bot)
    } else if let Some(job_id) = user_id.strip_prefix("cron:") {
        ("cron", job_id)
    } else if platform == "web" || user_id == "dashboard" {
        ("web", "dashboard")
    } else if !user_id.is_empty() {
        ("user", user_id)
    } else {
        ("sys", "default")
    };

    let sanitized = sanitize(raw_value);
    let initial_slug = format!("{category}-{sanitized}");
    let raw_lower = raw_value.to_lowercase();

    if sanitized != raw_lower || initial_slug.len() > 48 {
        let hash_suffix = sha256_hex_8(raw_value);
        let overhead = category.len() + 10;
        let max_sanitized_len = 48usize.saturating_sub(overhead);
        let truncated = if sanitized.len() > max_sanitized_len {
            &sanitized[..max_sanitized_len]
        } else {
            &sanitized
        };
        let truncated = truncated.trim_end_matches('-');

        if truncated.is_empty() {
            format!("{category}-{hash_suffix}")
        } else {
            format!("{category}-{truncated}-{hash_suffix}")
        }
    } else {
        initial_slug
    }
}

pub fn resolve_workspace(base: &Path, slug: &str) -> AgentWorkspace {
    let cwd = base.join("agents").join(slug);
    let roots = vec![cwd.clone(), base.join("shared")];
    AgentWorkspace { cwd, roots }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn test_slug_when_bot_session() {
        // Given: a platform and a bot_id
        let platform = "discord";
        let user_id = "some_user";
        let bot_id = Some("1465631383862120451");

        // When: generating workspace slug
        let slug = agent_workspace_slug(platform, user_id, bot_id);

        // Then: uses bot category and bot id
        assert_eq!(slug, "bot-1465631383862120451");
    }

    #[test]
    fn test_slug_when_cron_user_id() {
        // Given: a user_id with cron: prefix
        let platform = "discord";
        let user_id = "cron:omon-katok-3h-group-digest-v3";
        let bot_id = None;

        // When: generating workspace slug
        let slug = agent_workspace_slug(platform, user_id, bot_id);

        // Then: uses cron category and job id without prefix
        assert_eq!(slug, "cron-omon-katok-3h-group-digest-v3");
    }

    #[test]
    fn test_slug_when_snowflake_cron_vs_bot_collision() {
        // Given: cron:123 and bot_id 123
        let cron_user = "cron:123";
        let bot_id = "123";

        // When: generating both workspace slugs
        let cron_slug = agent_workspace_slug("discord", cron_user, None);
        let bot_slug = agent_workspace_slug("discord", "", Some(bot_id));

        // Then: slugs differ by category prefix
        assert_eq!(cron_slug, "cron-123");
        assert_eq!(bot_slug, "bot-123");
        assert_ne!(cron_slug, bot_slug);
    }

    #[test]
    fn test_slug_when_dashboard_or_web_platform() {
        // Given: web platform or dashboard user_id
        let web_slug = agent_workspace_slug("web", "user1", None);
        let dash_slug = agent_workspace_slug("discord", "dashboard", None);

        // When / Then: both resolve to web-dashboard
        assert_eq!(web_slug, "web-dashboard");
        assert_eq!(dash_slug, "web-dashboard");
    }

    #[test]
    fn test_slug_when_sanitize_and_hash_unicode_user_id() {
        // Given: user_id containing special chars and non-ascii Unicode
        let platform = "discord";
        let user_id = "Alice Kim/개발";
        let bot_id = None;

        // When: generating workspace slug
        let slug = agent_workspace_slug(platform, user_id, bot_id);

        // Then: slug starts with user-alice-kim- and ends with 8-char hex sha256 tail
        assert!(slug.starts_with("user-alice-kim-"));
        let parts: Vec<&str> = slug.split('-').collect();
        let hash_tail = parts.last().unwrap();
        assert_eq!(hash_tail.len(), 8);
        assert!(hash_tail.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_slug_when_both_missing() {
        // Given: empty platform, user_id, and no bot_id
        let platform = "";
        let user_id = "";
        let bot_id = None;

        // When: generating workspace slug
        let slug = agent_workspace_slug(platform, user_id, bot_id);

        // Then: resolves to sys-default
        assert_eq!(slug, "sys-default");
    }

    #[test]
    fn test_slug_when_length_greater_than_48_gets_hash_tail() {
        // Given: user_id that produces a slug longer than 48 chars
        let platform = "discord";
        let user_id =
            "this-is-a-very-long-user-identifier-that-exceeds-the-maximum-slug-length-limit";
        let bot_id = None;

        // When: generating workspace slug
        let slug = agent_workspace_slug(platform, user_id, bot_id);

        // Then: slug is capped <= 48 and has 8-char sha256 tail
        assert!(slug.len() <= 48);
        assert!(slug.starts_with("user-this-is-a-very-long-user-iden"));
        let parts: Vec<&str> = slug.split('-').collect();
        let hash_tail = parts.last().unwrap();
        assert_eq!(hash_tail.len(), 8);
        assert!(hash_tail.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_resolve_workspace_returns_cwd_and_roots() {
        // Given: a base path and slug
        let base = Path::new("/var/omon/data");
        let slug = "bot-1465631383862120451";

        // When: resolving workspace
        let ws = resolve_workspace(base, slug);

        // Then: cwd is base/agents/<slug> and roots has [cwd, base/shared]
        assert_eq!(
            ws.cwd,
            PathBuf::from("/var/omon/data/agents/bot-1465631383862120451")
        );
        assert_eq!(
            ws.roots,
            vec![
                PathBuf::from("/var/omon/data/agents/bot-1465631383862120451"),
                PathBuf::from("/var/omon/data/shared"),
            ]
        );
    }
}
