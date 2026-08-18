use std::env;

use async_trait::async_trait;
use tokio::sync::OnceCell;

use super::message_context::{
    DiscordMessageContextProvider, MessageContextPolicy, MessageContextProvider,
    MessageContextRequest, MessageContextResult,
};
use crate::{OmonError, SessionKey};

pub struct LazyDiscordMessageContextProvider {
    tokens: Vec<String>,
    database_url: String,
    policy: MessageContextPolicy,
    inner: OnceCell<DiscordMessageContextProvider>,
}

impl LazyDiscordMessageContextProvider {
    pub fn from_environment() -> Option<Self> {
        let tokens = discord_tokens_from_environment();
        if tokens.is_empty() {
            return None;
        }
        Some(Self {
            tokens,
            database_url: env::var("DATABASE_URL")
                .unwrap_or_else(|_| "sqlite://omon_gateway.db".to_owned()),
            policy: MessageContextPolicy::from_environment(),
            inner: OnceCell::new(),
        })
    }

    async fn inner(&self) -> Result<&DiscordMessageContextProvider, OmonError> {
        self.inner
            .get_or_try_init(|| async {
                let pool = crate::storage::init_pool(&self.database_url).await?;
                DiscordMessageContextProvider::new(self.tokens.clone(), pool, self.policy.clone())
            })
            .await
    }
}

#[async_trait]
impl MessageContextProvider for LazyDiscordMessageContextProvider {
    fn platform(&self) -> &str {
        "discord"
    }

    async fn query(
        &self,
        session: &SessionKey,
        request: &MessageContextRequest,
    ) -> Result<MessageContextResult, OmonError> {
        self.inner().await?.query(session, request).await
    }
}

fn discord_tokens_from_environment() -> Vec<String> {
    let mut tokens = Vec::new();
    for name in ["DISCORD_BOT_TOKEN", "DISCORD_BOT_TOKENS"] {
        if let Ok(raw) = env::var(name) {
            for token in raw.split(',') {
                let trimmed = token.trim().trim_matches('"').trim_matches('\'');
                if !trimmed.is_empty() && !tokens.iter().any(|known| known == trimmed) {
                    tokens.push(trimmed.to_owned());
                }
            }
        }
    }
    tokens
}
