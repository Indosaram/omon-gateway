use std::collections::HashMap;
use std::fmt;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Stable routing identity for a conversation across supported platforms.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct SessionKey {
    pub platform: String,
    pub guild_id: Option<String>,
    pub channel_id: String,
    pub thread_id: Option<String>,
    pub user_id: String,
}

impl SessionKey {
    pub fn new(
        platform: impl Into<String>,
        guild_id: Option<impl Into<String>>,
        channel_id: impl Into<String>,
        thread_id: Option<impl Into<String>>,
        user_id: impl Into<String>,
    ) -> Self {
        Self {
            platform: platform.into(),
            guild_id: guild_id.map(Into::into),
            channel_id: channel_id.into(),
            thread_id: thread_id.map(Into::into),
            user_id: user_id.into(),
        }
    }

    /// Returns the canonical, collision-resistant storage key.
    ///
    /// Each component is length-prefixed so embedded separators cannot make
    /// distinct session identities produce the same key.
    pub fn storage_key(&self) -> String {
        fn component(value: Option<&str>) -> String {
            match value {
                Some(value) => format!("{}:{value}", value.len()),
                None => "-".to_owned(),
            }
        }

        [
            component(Some(&self.platform)),
            component(self.guild_id.as_deref()),
            component(Some(&self.channel_id)),
            component(self.thread_id.as_deref()),
            component(Some(&self.user_id)),
        ]
        .join("|")
    }
}

impl fmt::Display for SessionKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.storage_key())
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct SessionState {
    #[serde(default)]
    pub active_model: Option<String>,
    #[serde(default)]
    pub system_prompt: Option<String>,
    #[serde(default)]
    pub metadata: HashMap<String, serde_json::Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SessionContext {
    pub key: SessionKey,
    #[serde(default)]
    pub state: SessionState,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl SessionContext {
    pub fn new(key: SessionKey) -> Self {
        let now = Utc::now();
        Self {
            key,
            state: SessionState::default(),
            created_at: now,
            updated_at: now,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::SessionKey;

    fn discord_key(thread_id: Option<&str>) -> SessionKey {
        SessionKey::new("discord", Some("guild-1"), "channel-2", thread_id, "user-3")
    }

    #[test]
    fn derives_stable_key_from_every_routing_dimension() {
        let key = discord_key(Some("thread-4"));

        assert_eq!(
            key.storage_key(),
            "7:discord|7:guild-1|9:channel-2|8:thread-4|6:user-3"
        );
        assert_eq!(key.to_string(), key.storage_key());
    }

    #[test]
    fn differentiates_absent_values_and_separator_like_content() {
        let absent_thread = discord_key(None);
        let empty_thread = discord_key(Some(""));
        let first = SessionKey::new("a|1:b", None::<String>, "c", None::<String>, "d");
        let second = SessionKey::new("a", Some("1:b"), "c", None::<String>, "d");

        assert_ne!(absent_thread, empty_thread);
        assert_ne!(absent_thread.storage_key(), empty_thread.storage_key());
        assert_ne!(first.storage_key(), second.storage_key());
    }

    #[test]
    fn supports_hash_based_session_lookup_and_serde_round_trip() {
        let key = discord_key(Some("thread-4"));
        let mut sessions = HashSet::new();
        sessions.insert(key.clone());

        assert!(sessions.contains(&key));
        let json = serde_json::to_string(&key).expect("session key should serialize");
        let decoded: SessionKey =
            serde_json::from_str(&json).expect("session key should deserialize");
        assert_eq!(decoded, key);
    }
}
