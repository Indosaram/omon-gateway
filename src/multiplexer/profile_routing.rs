use std::fmt;

use serde::{Deserialize, Deserializer, Serialize};

use crate::{SessionContext, SessionKey};

/// Flexible deserializer for optional u64 supporting numbers (123), strings ("123"), and null.
fn deserialize_optional_u64<'de, D>(deserializer: D) -> Result<Option<u64>, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum NumericOrString {
        U64(u64),
        I64(i64),
        String(String),
    }

    let opt: Option<NumericOrString> = Option::deserialize(deserializer)?;
    match opt {
        Some(NumericOrString::U64(v)) => Ok(Some(v)),
        Some(NumericOrString::I64(v)) => {
            if v >= 0 {
                Ok(Some(v as u64))
            } else {
                Ok(None)
            }
        }
        Some(NumericOrString::String(s)) => {
            let trimmed = s.trim();
            if trimmed.is_empty() {
                Ok(None)
            } else {
                trimmed
                    .parse::<u64>()
                    .map(Some)
                    .map_err(serde::de::Error::custom)
            }
        }
        None => Ok(None),
    }
}

fn default_true() -> bool {
    true
}

/// A routing rule that maps a Discord hierarchy (guild, channel, thread)
/// to a profile with custom model, system prompt, and enabled toolsets.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProfileRoute {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(
        default,
        alias = "guild_id",
        deserialize_with = "deserialize_optional_u64"
    )]
    pub guild: Option<u64>,
    #[serde(
        default,
        alias = "channel_id",
        alias = "chat_id",
        deserialize_with = "deserialize_optional_u64"
    )]
    pub channel: Option<u64>,
    #[serde(
        default,
        alias = "thread_id",
        deserialize_with = "deserialize_optional_u64"
    )]
    pub thread: Option<u64>,
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub system_prompt: Option<String>,
    #[serde(default, alias = "toolsets")]
    pub enabled_toolsets: Option<Vec<String>>,
}

impl ProfileRoute {
    pub fn new() -> Self {
        Self {
            name: None,
            guild: None,
            channel: None,
            thread: None,
            enabled: true,
            model: None,
            system_prompt: None,
            enabled_toolsets: None,
        }
    }

    /// Specificity score for hierarchical matching precedence.
    ///
    /// Precedence matches Hermes profile routing:
    /// - `thread` specified: +8 (highest)
    /// - `channel` specified: +4 (middle)
    /// - `guild` specified: +2 (lowest)
    ///
    /// Thus: thread matches (8..14) > channel matches (4..6) > guild matches (2).
    pub fn specificity(&self) -> u32 {
        let mut score = 0;
        if self.guild.is_some() {
            score += 2;
        }
        if self.channel.is_some() {
            score += 4;
        }
        if self.thread.is_some() {
            score += 8;
        }
        score
    }

    /// Checks if this route matches the given target context conjunctively.
    pub fn matches(&self, guild_id: Option<u64>, channel_id: u64, thread_id: Option<u64>) -> bool {
        if !self.enabled {
            return false;
        }
        if let Some(req_thread) = self.thread {
            if thread_id != Some(req_thread) {
                return false;
            }
        }
        if let Some(req_channel) = self.channel {
            if channel_id != req_channel {
                return false;
            }
        }
        if let Some(req_guild) = self.guild {
            if guild_id != Some(req_guild) {
                return false;
            }
        }
        true
    }

    /// Applies profile defaults to a session without overwriting explicitly set fields.
    pub fn apply_to_session(&self, session: &mut SessionContext) {
        if session.state.active_model.is_none() {
            if let Some(model) = &self.model {
                session.state.active_model = Some(model.clone());
            }
        }
        if session.state.system_prompt.is_none() {
            if let Some(system_prompt) = &self.system_prompt {
                session.state.system_prompt = Some(system_prompt.clone());
            }
        }
        if let Some(toolsets) = &self.enabled_toolsets {
            if session.state.enabled_toolsets.is_none() {
                session.state.enabled_toolsets = Some(toolsets.clone());
            }
            session
                .state
                .metadata
                .entry("enabled_toolsets".into())
                .or_insert_with(|| serde_json::json!(toolsets));
        }
    }
}

impl Default for ProfileRoute {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for ProfileRoute {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ProfileRoute(name={:?}, guild={:?}, channel={:?}, thread={:?}, model={:?})",
            self.name, self.guild, self.channel, self.thread, self.model
        )
    }
}

/// Hierarchical router that matches Discord contexts to profile configurations.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ProfileRouter {
    routes: Vec<ProfileRoute>,
}

impl ProfileRouter {
    /// Creates a new `ProfileRouter` with routes sorted by specificity descending.
    pub fn new(routes: Vec<ProfileRoute>) -> Self {
        let mut sorted = routes;
        // Stable sort descending by specificity ensures highest specificity matches first.
        sorted.sort_by_key(|b| std::cmp::Reverse(b.specificity()));
        Self { routes: sorted }
    }

    /// Parses routes from a JSON string.
    pub fn from_json(json_str: &str) -> Self {
        Self::new(parse_profile_routes(json_str))
    }

    pub fn routes(&self) -> &[ProfileRoute] {
        &self.routes
    }

    pub fn is_empty(&self) -> bool {
        self.routes.is_empty()
    }

    pub fn len(&self) -> usize {
        self.routes.len()
    }

    /// Finds the highest-specificity matching route for the given Discord context.
    pub fn match_route(
        &self,
        guild_id: Option<u64>,
        channel_id: u64,
        thread_id: Option<u64>,
    ) -> Option<&ProfileRoute> {
        self.routes
            .iter()
            .find(|route| route.matches(guild_id, channel_id, thread_id))
    }

    /// Alias for `match_route` to match profile-returning API style.
    pub fn match_profile(
        &self,
        guild_id: Option<u64>,
        channel_id: u64,
        thread_id: Option<u64>,
    ) -> Option<&ProfileRoute> {
        self.match_route(guild_id, channel_id, thread_id)
    }

    /// Matches a route against a `SessionKey`.
    pub fn match_session(&self, session_key: &SessionKey) -> Option<&ProfileRoute> {
        let guild_id = session_key
            .guild_id
            .as_deref()
            .and_then(|s| s.parse::<u64>().ok());
        let channel_id = session_key.channel_id.parse::<u64>().ok()?;
        let thread_id = session_key
            .thread_id
            .as_deref()
            .and_then(|s| s.parse::<u64>().ok());
        self.match_route(guild_id, channel_id, thread_id)
    }

    /// Applies matching profile overrides to a session context if a route matches.
    pub fn apply_to_session(&self, session: &mut SessionContext) {
        if let Some(route) = self.match_session(&session.key) {
            route.apply_to_session(session);
        }
    }
}

/// Parses profile routes from a JSON string. Returns an empty Vec on missing or invalid input.
pub fn parse_profile_routes(json_str: &str) -> Vec<ProfileRoute> {
    let trimmed = json_str.trim();
    if trimmed.is_empty() {
        return Vec::new();
    }
    match serde_json::from_str::<Vec<ProfileRoute>>(trimmed) {
        Ok(routes) => routes,
        Err(err) => {
            tracing::warn!(%err, raw = %trimmed, "failed to parse profile routes JSON; falling back to empty routes");
            Vec::new()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_precedence_thread_beats_channel_beats_guild() {
        let guild_route = ProfileRoute {
            name: Some("guild-route".into()),
            guild: Some(100),
            channel: None,
            thread: None,
            enabled: true,
            model: Some("guild-model".into()),
            system_prompt: Some("guild-prompt".into()),
            enabled_toolsets: None,
        };
        let channel_route = ProfileRoute {
            name: Some("channel-route".into()),
            guild: Some(100),
            channel: Some(200),
            thread: None,
            enabled: true,
            model: Some("channel-model".into()),
            system_prompt: Some("channel-prompt".into()),
            enabled_toolsets: None,
        };
        let thread_route = ProfileRoute {
            name: Some("thread-route".into()),
            guild: Some(100),
            channel: Some(200),
            thread: Some(300),
            enabled: true,
            model: Some("thread-model".into()),
            system_prompt: Some("thread-prompt".into()),
            enabled_toolsets: None,
        };

        // Insert in arbitrary order
        let router = ProfileRouter::new(vec![
            guild_route.clone(),
            thread_route.clone(),
            channel_route.clone(),
        ]);

        // 1. Thread context: Thread route wins
        let matched = router.match_route(Some(100), 200, Some(300));
        assert!(matched.is_some());
        assert_eq!(matched.unwrap().model.as_deref(), Some("thread-model"));

        // 2. Different thread in channel 200: Channel route wins
        let matched = router.match_route(Some(100), 200, Some(999));
        assert!(matched.is_some());
        assert_eq!(matched.unwrap().model.as_deref(), Some("channel-model"));

        // 3. Direct channel message in channel 200 (no thread): Channel route wins
        let matched = router.match_route(Some(100), 200, None);
        assert!(matched.is_some());
        assert_eq!(matched.unwrap().model.as_deref(), Some("channel-model"));

        // 4. Other channel in guild 100: Guild route wins
        let matched = router.match_route(Some(100), 555, None);
        assert!(matched.is_some());
        assert_eq!(matched.unwrap().model.as_deref(), Some("guild-model"));

        // 5. Unrelated guild: returns None
        let matched = router.match_route(Some(999), 555, None);
        assert!(matched.is_none());
    }

    #[test]
    fn test_no_match_returns_none() {
        let router = ProfileRouter::new(vec![ProfileRoute {
            name: Some("specific".into()),
            guild: Some(111),
            channel: Some(222),
            thread: Some(333),
            enabled: true,
            model: Some("custom".into()),
            system_prompt: None,
            enabled_toolsets: None,
        }]);

        assert!(router.match_route(Some(999), 888, Some(777)).is_none());
        assert!(router.match_route(None, 888, None).is_none());
    }

    #[test]
    fn test_disabled_route_is_ignored() {
        let router = ProfileRouter::new(vec![ProfileRoute {
            name: Some("disabled".into()),
            guild: Some(100),
            channel: Some(200),
            thread: None,
            enabled: false,
            model: Some("disabled-model".into()),
            system_prompt: None,
            enabled_toolsets: None,
        }]);

        assert!(router.match_route(Some(100), 200, None).is_none());
    }

    #[test]
    fn test_json_parsing_valid_empty_and_malformed() {
        // Valid JSON with standard keys
        let valid_json = r#"[
            {
                "guild": 123,
                "channel": 456,
                "thread": null,
                "model": "gpt-x",
                "system_prompt": "You are a specialist",
                "toolsets": ["terminal", "web"]
            },
            {
                "guild_id": "789",
                "channel_id": "101",
                "thread_id": "202",
                "model": "claude-3-5-sonnet",
                "enabled_toolsets": ["file", "cron"]
            }
        ]"#;
        let routes = parse_profile_routes(valid_json);
        assert_eq!(routes.len(), 2);
        assert_eq!(routes[0].guild, Some(123));
        assert_eq!(routes[0].channel, Some(456));
        assert_eq!(routes[0].thread, None);
        assert_eq!(routes[0].model.as_deref(), Some("gpt-x"));
        assert_eq!(
            routes[0].system_prompt.as_deref(),
            Some("You are a specialist")
        );
        assert_eq!(
            routes[0].enabled_toolsets.as_deref(),
            Some(&["terminal".to_string(), "web".to_string()][..])
        );

        assert_eq!(routes[1].guild, Some(789));
        assert_eq!(routes[1].channel, Some(101));
        assert_eq!(routes[1].thread, Some(202));
        assert_eq!(routes[1].model.as_deref(), Some("claude-3-5-sonnet"));
        assert_eq!(
            routes[1].enabled_toolsets.as_deref(),
            Some(&["file".to_string(), "cron".to_string()][..])
        );

        // Empty / whitespace strings return empty list
        assert!(parse_profile_routes("").is_empty());
        assert!(parse_profile_routes("   ").is_empty());
        assert!(parse_profile_routes("[]").is_empty());

        // Malformed JSON logs warning and returns empty list without panicking
        assert!(parse_profile_routes("{not-valid-json").is_empty());
        assert!(parse_profile_routes(r#"{"not_an_array": true}"#).is_empty());
    }

    #[test]
    fn test_apply_to_session_populates_fresh_session() {
        let route = ProfileRoute {
            name: Some("test-profile".into()),
            guild: Some(10),
            channel: Some(20),
            thread: None,
            enabled: true,
            model: Some("gpt-routed".into()),
            system_prompt: Some("Custom system prompt".into()),
            enabled_toolsets: Some(vec!["terminal".into(), "web".into()]),
        };

        let key = SessionKey::new("discord", Some("10"), "20", None::<String>, "user-1");
        let mut session = SessionContext::new(key);
        assert_eq!(session.state.active_model, None);
        assert_eq!(session.state.system_prompt, None);
        assert_eq!(session.state.enabled_toolsets, None);

        route.apply_to_session(&mut session);

        assert_eq!(session.state.active_model.as_deref(), Some("gpt-routed"));
        assert_eq!(
            session.state.system_prompt.as_deref(),
            Some("Custom system prompt")
        );
        assert_eq!(
            session.state.enabled_toolsets.as_deref(),
            Some(&["terminal".to_string(), "web".to_string()][..])
        );
        assert_eq!(
            session.state.metadata.get("enabled_toolsets"),
            Some(&serde_json::json!(["terminal", "web"]))
        );
    }

    #[test]
    fn test_apply_to_session_preserves_explicitly_set_model() {
        let route = ProfileRoute {
            name: Some("test-profile".into()),
            guild: Some(10),
            channel: Some(20),
            thread: None,
            enabled: true,
            model: Some("gpt-routed".into()),
            system_prompt: Some("Custom system prompt".into()),
            enabled_toolsets: Some(vec!["terminal".into()]),
        };

        let key = SessionKey::new("discord", Some("10"), "20", None::<String>, "user-1");
        let mut session = SessionContext::new(key);
        // Explicitly set active_model (e.g. from `/model claude-3-opus`)
        session.state.active_model = Some("claude-3-opus".into());

        route.apply_to_session(&mut session);

        // Explicit model must NOT be clobbered by the profile route
        assert_eq!(session.state.active_model.as_deref(), Some("claude-3-opus"));
        // But unset system prompt and toolsets should still be populated
        assert_eq!(
            session.state.system_prompt.as_deref(),
            Some("Custom system prompt")
        );
        assert_eq!(
            session.state.enabled_toolsets.as_deref(),
            Some(&["terminal".to_string()][..])
        );
    }

    #[test]
    fn test_match_session_key_resolution() {
        let router = ProfileRouter::new(vec![
            ProfileRoute {
                name: Some("channel-profile".into()),
                guild: Some(1234),
                channel: Some(5678),
                thread: None,
                enabled: true,
                model: Some("model-a".into()),
                system_prompt: None,
                enabled_toolsets: None,
            },
            ProfileRoute {
                name: Some("thread-profile".into()),
                guild: Some(1234),
                channel: Some(5678),
                thread: Some(9999),
                enabled: true,
                model: Some("model-b".into()),
                system_prompt: None,
                enabled_toolsets: None,
            },
        ]);

        let key_thread = SessionKey::new("discord", Some("1234"), "5678", Some("9999"), "user-1");
        let matched = router.match_session(&key_thread);
        assert_eq!(matched.unwrap().model.as_deref(), Some("model-b"));

        let key_channel =
            SessionKey::new("discord", Some("1234"), "5678", None::<String>, "user-1");
        let matched = router.match_session(&key_channel);
        assert_eq!(matched.unwrap().model.as_deref(), Some("model-a"));

        let key_other =
            SessionKey::new("discord", Some("1234"), "999999", None::<String>, "user-1");
        assert!(router.match_session(&key_other).is_none());
    }
}
