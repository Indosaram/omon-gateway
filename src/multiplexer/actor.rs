use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;
use sqlx::SqlitePool;
use tokio::sync::{mpsc, oneshot};

use crate::{
    render_user_prompt, DeliveryLedgerService, InboundEvent, OmonError, OutboundAction, Result,
    SessionContext, SessionKey,
};

#[async_trait]
pub trait AgentRunner: Send + Sync + 'static {
    async fn run(&self, session: &mut SessionContext, event: InboundEvent) -> Result<()>;
}

#[async_trait]
pub trait OutboundDispatcher: Send + Sync + 'static {
    async fn dispatch(&self, action: OutboundAction) -> Result<()>;
}

pub(crate) enum ActorCommand {
    Event(Box<InboundEvent>),
    EvictIfIdle {
        idle_timeout: Duration,
        reply: oneshot::Sender<Result<bool>>,
    },
}

pub struct SessionActor {
    context: SessionContext,
    receiver: mpsc::Receiver<ActorCommand>,
    runner: Arc<dyn AgentRunner>,
    dispatcher: Option<Arc<dyn OutboundDispatcher>>,
    pool: SqlitePool,
    last_active_at: tokio::time::Instant,
    dirty: bool,
}

impl SessionActor {
    pub(crate) async fn load(
        key: SessionKey,
        receiver: mpsc::Receiver<ActorCommand>,
        runner: Arc<dyn AgentRunner>,
        dispatcher: Option<Arc<dyn OutboundDispatcher>>,
        pool: SqlitePool,
    ) -> Result<Self> {
        let context = load_context(&pool, key).await?;
        Ok(Self {
            context,
            receiver,
            runner,
            dispatcher,
            pool,
            last_active_at: tokio::time::Instant::now(),
            dirty: false,
        })
    }

    pub(crate) async fn run(mut self) {
        while let Some(command) = self.receiver.recv().await {
            match command {
                ActorCommand::Event(event) => {
                    self.last_active_at = tokio::time::Instant::now();
                    self.context.updated_at = Utc::now();
                    self.dirty = true;
                    if let Err(error) = self.persist_inbound(&event).await {
                        tracing::error!(session = %self.context.key, %error, "failed to persist inbound event");
                    }
                    let reply_to = event.platform_message_id.clone();
                    let delivery_id = event.delivery_id.clone();
                    let result = self.runner.run(&mut self.context, *event).await;
                    if let Some(delivery_id) = delivery_id.as_deref() {
                        let ledger = DeliveryLedgerService::new(self.pool.clone());
                        match &result {
                            Ok(()) => {
                                if let Err(error) = ledger.mark_delivered(delivery_id).await {
                                    tracing::error!(%delivery_id, %error, "failed to complete delivery claim");
                                }
                            }
                            Err(error) => {
                                if let Err(ledger_error) =
                                    ledger.mark_failed(delivery_id, error.to_string()).await
                                {
                                    tracing::error!(%delivery_id, %ledger_error, "failed to mark delivery claim failed");
                                }
                            }
                        }
                    }
                    if let Err(error) = result {
                        tracing::error!(session = %self.context.key, %error, "agent runner failed");
                        if let Some(dispatcher) = &self.dispatcher {
                            let _ = dispatcher
                                .dispatch(OutboundAction::SendMessage {
                                    session: self.context.key.clone(),
                                    content: error.to_string(),
                                    reply_to: Some(reply_to),
                                })
                                .await;
                        }
                    }
                    self.context.updated_at = Utc::now();
                }
                ActorCommand::EvictIfIdle {
                    idle_timeout,
                    reply,
                } => {
                    let idle =
                        self.last_active_at.elapsed() > idle_timeout && self.receiver.is_empty();
                    if idle {
                        let result = self.flush_if_dirty().await.map(|_| true);
                        let should_stop = result.is_ok();
                        let _ = reply.send(result);
                        if should_stop {
                            break;
                        }
                    } else {
                        let _ = reply.send(Ok(false));
                    }
                }
            }
        }

        // Dropping the multiplexer closes all strong senders. Flush dirty state
        // on that graceful channel shutdown so actor memory can be reclaimed
        // without silently discarding the last in-memory session mutation.
        if let Err(error) = self.flush_if_dirty().await {
            tracing::error!(session = %self.context.key, %error, "failed to flush session actor during shutdown");
        }
    }

    async fn persist_inbound(&self, event: &InboundEvent) -> Result<()> {
        ensure_session(&self.pool, &self.context).await?;
        sqlx::query(
            "INSERT INTO messages (id, session_key, role, content, metadata_json, created_at)
             VALUES (?, ?, 'user', ?, ?, ?)
             ON CONFLICT(id) DO NOTHING",
        )
        .bind(event.id.to_string())
        .bind(self.context.key.storage_key())
        .bind(render_user_prompt(event))
        .bind(serde_json::to_string(&event.attachments).map_err(serialization_error)?)
        .bind(event.received_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn flush_if_dirty(&mut self) -> Result<()> {
        if !self.dirty {
            return Ok(());
        }
        self.flush().await?;
        self.dirty = false;
        Ok(())
    }

    async fn flush(&self) -> Result<()> {
        ensure_session(&self.pool, &self.context).await?;
        let state = serde_json::to_string(&self.context.state).map_err(serialization_error)?;
        sqlx::query("UPDATE sessions SET state_json = ?, updated_at = ? WHERE session_key = ?")
            .bind(state)
            .bind(self.context.updated_at)
            .bind(self.context.key.storage_key())
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}

async fn load_context(pool: &SqlitePool, key: SessionKey) -> Result<SessionContext> {
    let row: Option<(String, chrono::DateTime<Utc>, chrono::DateTime<Utc>)> = sqlx::query_as(
        "SELECT state_json, created_at, updated_at FROM sessions WHERE session_key = ?",
    )
    .bind(key.storage_key())
    .fetch_optional(pool)
    .await?;
    match row {
        Some((state_json, created_at, updated_at)) => Ok(SessionContext {
            key,
            state: serde_json::from_str(&state_json).map_err(serialization_error)?,
            created_at,
            updated_at,
        }),
        None => Ok(SessionContext::new(key)),
    }
}

async fn ensure_session(pool: &SqlitePool, context: &SessionContext) -> Result<()> {
    sqlx::query(
        "INSERT INTO sessions (
            session_key, platform, guild_id, channel_id, thread_id, user_id,
            state_json, created_at, updated_at
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(session_key) DO NOTHING",
    )
    .bind(context.key.storage_key())
    .bind(&context.key.platform)
    .bind(&context.key.guild_id)
    .bind(&context.key.channel_id)
    .bind(&context.key.thread_id)
    .bind(&context.key.user_id)
    .bind(serde_json::to_string(&context.state).map_err(serialization_error)?)
    .bind(context.created_at)
    .bind(context.updated_at)
    .execute(pool)
    .await?;
    Ok(())
}

fn serialization_error(error: serde_json::Error) -> OmonError {
    OmonError::Multiplexer(error.to_string())
}
