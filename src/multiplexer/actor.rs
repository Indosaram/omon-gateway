use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;
use sqlx::SqlitePool;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::{
    render_user_prompt, strip_leading_message_timestamps, DeliveryLedgerService, InboundEvent,
    OmonError, OutboundAction, ProfileRouter, Result, SessionContext, SessionKey, SessionState,
};

/// Sensible maximum number of pending events queued per session actor.
/// If the queue overflows, oldest events are preserved and new incoming events
/// are rejected to avoid unbounded memory growth.
const MAX_PENDING_EVENTS: usize = 64;

#[async_trait]
pub trait AgentRunner: Send + Sync + 'static {
    async fn run(&self, session: &mut SessionContext, event: InboundEvent) -> Result<()>;

    async fn run_cancelable(
        &self,
        session: &mut SessionContext,
        event: InboundEvent,
        cancellation: CancellationToken,
    ) -> Result<()> {
        tokio::select! {
            result = self.run(session, event) => result,
            _ = cancellation.cancelled() => Err(OmonError::Multiplexer("agent turn cancelled".into())),
        }
    }

    async fn cancel(&self, _session: &SessionContext) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
pub trait OutboundDispatcher: Send + Sync + 'static {
    async fn dispatch(&self, action: OutboundAction) -> Result<()>;
}

pub(crate) enum ActorCommand {
    Event(Box<InboundEvent>),
    Stop {
        reply: oneshot::Sender<Result<bool>>,
    },
    EvictIfIdle {
        idle_timeout: Duration,
        reply: oneshot::Sender<Result<bool>>,
    },
    TouchActivity,
}

enum TurnOutcome {
    Completed(Result<()>),
    Stopped(oneshot::Sender<Result<bool>>),
    Shutdown,
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
        profile_router: Option<Arc<ProfileRouter>>,
    ) -> Result<Self> {
        let context = load_context(&pool, key, profile_router.as_deref()).await?;
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
        let mut pending_events: VecDeque<Box<InboundEvent>> = VecDeque::new();
        loop {
            let command = if let Some(event) = pending_events.pop_front() {
                ActorCommand::Event(event)
            } else {
                match self.receiver.recv().await {
                    Some(command) => command,
                    None => break,
                }
            };

            match command {
                ActorCommand::Event(event) => {
                    self.last_active_at = tokio::time::Instant::now();
                    self.context.updated_at = Utc::now();
                    self.context.state.suspended = false;
                    self.dirty = true;

                    if !event.platform_message_id.is_empty() {
                        match crate::storage::has_platform_message_id(
                            &self.pool,
                            &self.context.key.storage_key(),
                            &event.platform_message_id,
                        )
                        .await
                        {
                            Ok(true) => {
                                tracing::info!(
                                    session = %self.context.key,
                                    platform_message_id = %event.platform_message_id,
                                    "skipping replayed inbound turn: platform_message_id already exists in transcript"
                                );
                                self.complete_delivery(event.delivery_id.as_deref(), &Ok(()))
                                    .await;
                                continue;
                            }
                            Ok(false) => {}
                            Err(error) => {
                                tracing::warn!(
                                    session = %self.context.key,
                                    platform_message_id = %event.platform_message_id,
                                    %error,
                                    "failed to check transcript dedup index"
                                );
                            }
                        }
                    }

                    if let Err(error) = self.persist_inbound(&event).await {
                        tracing::error!(session = %self.context.key, %error, "failed to persist inbound event");
                    }

                    let event_id = event.id;
                    let reply_to = event.platform_message_id.clone();
                    let delivery_id = event.delivery_id.clone();
                    let cancellation = CancellationToken::new();
                    let mut turn_context = self.context.clone();
                    let runner = self.runner.clone();
                    let mut run = Box::pin(runner.run_cancelable(
                        &mut turn_context,
                        *event,
                        cancellation.clone(),
                    ));
                    let outcome = loop {
                        tokio::select! {
                            biased;
                            command = self.receiver.recv() => {
                                match command {
                                    Some(ActorCommand::Event(next)) => {
                                        if pending_events.len() < MAX_PENDING_EVENTS {
                                            pending_events.push_back(next);
                                        } else {
                                            tracing::warn!(
                                                session = %self.context.key,
                                                "pending turn queue full (max {}); dropping new event",
                                                MAX_PENDING_EVENTS
                                            );
                                            self.complete_delivery(
                                                next.delivery_id.as_deref(),
                                                &Err(OmonError::Multiplexer("pending turn queue full".into())),
                                            )
                                            .await;
                                        }
                                    }
                                    Some(ActorCommand::Stop { reply }) => {
                                        cancellation.cancel();
                                        while let Some(pending) = pending_events.pop_front() {
                                            self.complete_delivery(
                                                pending.delivery_id.as_deref(),
                                                &Err(OmonError::Multiplexer("stopped by user".into())),
                                            )
                                            .await;
                                        }
                                        break TurnOutcome::Stopped(reply);
                                    }
                                    Some(ActorCommand::EvictIfIdle { reply, .. }) => {
                                        let _ = reply.send(Ok(false));
                                    }
                                    Some(ActorCommand::TouchActivity) => {
                                        self.last_active_at = tokio::time::Instant::now();
                                    }
                                    None => {
                                        cancellation.cancel();
                                        while let Some(pending) = pending_events.pop_front() {
                                            self.complete_delivery(
                                                pending.delivery_id.as_deref(),
                                                &Err(OmonError::Multiplexer("session actor shutting down".into())),
                                            )
                                            .await;
                                        }
                                        break TurnOutcome::Shutdown;
                                    }
                                }
                            }
                            result = &mut run => break TurnOutcome::Completed(result),
                        }
                    };
                    drop(run);

                    match outcome {
                        TurnOutcome::Completed(result) => {
                            if result.is_ok() {
                                self.context = turn_context;
                                let _ = crate::storage::clear_session_resume_pending(
                                    &self.pool,
                                    &self.context.key.storage_key(),
                                )
                                .await;
                            }
                            self.complete_delivery(delivery_id.as_deref(), &result)
                                .await;
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
                        }
                        TurnOutcome::Stopped(reply) => {
                            self.context.state.suspended = true;
                            self.dirty = true;
                            self.interrupt_turn(
                                event_id,
                                delivery_id.as_deref(),
                                "stopped by user",
                            )
                            .await;
                            let _ = self.flush_if_dirty().await;
                            let _ = reply.send(Ok(true));
                        }
                        TurnOutcome::Shutdown => {
                            self.interrupt_turn(
                                event_id,
                                delivery_id.as_deref(),
                                "session actor shutting down",
                            )
                            .await;
                            if let Err(error) = crate::storage::mark_session_resume_pending(
                                &self.pool,
                                &self.context.key.storage_key(),
                            )
                            .await
                            {
                                tracing::error!(session = %self.context.key, %error, "failed to mark resume_pending on shutdown");
                            }
                        }
                    }
                    self.context.updated_at = Utc::now();
                }
                ActorCommand::Stop { reply } => {
                    self.last_active_at = tokio::time::Instant::now();
                    self.context.state.suspended = true;
                    self.dirty = true;
                    while let Some(pending) = pending_events.pop_front() {
                        self.complete_delivery(
                            pending.delivery_id.as_deref(),
                            &Err(OmonError::Multiplexer("stopped by user".into())),
                        )
                        .await;
                    }
                    let _ = self.flush_if_dirty().await;
                    let _ = reply.send(Ok(false));
                }
                ActorCommand::TouchActivity => {
                    self.last_active_at = tokio::time::Instant::now();
                }
                ActorCommand::EvictIfIdle {
                    idle_timeout,
                    reply,
                } => {
                    let idle = self.last_active_at.elapsed() > idle_timeout
                        && self.receiver.is_empty()
                        && pending_events.is_empty();
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
        if !pending_events.is_empty() {
            if let Err(error) = crate::storage::mark_session_resume_pending(
                &self.pool,
                &self.context.key.storage_key(),
            )
            .await
            {
                tracing::error!(session = %self.context.key, %error, "failed to mark resume_pending for remaining pending events");
            }
        }
        if let Err(error) = self.flush_if_dirty().await {
            tracing::error!(session = %self.context.key, %error, "failed to flush session actor during shutdown");
        }
    }

    async fn interrupt_turn(&self, event_id: uuid::Uuid, delivery_id: Option<&str>, reason: &str) {
        if let Err(error) = self.runner.cancel(&self.context).await {
            tracing::warn!(session = %self.context.key, %error, "runner cancellation cleanup failed");
        }
        if let Err(error) = self.rollback_partial_history(event_id).await {
            tracing::error!(session = %self.context.key, %error, "failed to roll back interrupted turn history");
        }
        if let Some(delivery_id) = delivery_id {
            let ledger = DeliveryLedgerService::new(self.pool.clone());
            if let Err(error) = ledger.mark_failed(delivery_id, reason).await {
                tracing::error!(%delivery_id, %error, "failed to mark interrupted delivery claim failed");
            }
        }
        tracing::info!(session = %self.context.key, %reason, "agent turn interrupted");
    }

    async fn rollback_partial_history(&self, event_id: uuid::Uuid) -> Result<()> {
        sqlx::query(
            "DELETE FROM messages
             WHERE session_key = ? AND sequence > (
                 SELECT sequence FROM messages WHERE id = ? AND session_key = ?
             )",
        )
        .bind(self.context.key.storage_key())
        .bind(event_id.to_string())
        .bind(self.context.key.storage_key())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn complete_delivery(&self, delivery_id: Option<&str>, result: &Result<()>) {
        let Some(delivery_id) = delivery_id else {
            return;
        };
        let ledger = DeliveryLedgerService::new(self.pool.clone());
        let completion = match result {
            Ok(()) => ledger.mark_delivered(delivery_id).await,
            Err(error) => ledger.mark_failed(delivery_id, error.to_string()).await,
        };
        if let Err(error) = completion {
            tracing::error!(%delivery_id, %error, "failed to complete delivery claim");
        }
    }

    async fn persist_inbound(&self, event: &InboundEvent) -> Result<()> {
        ensure_session(&self.pool, &self.context).await?;
        sqlx::query(
            "INSERT INTO messages (id, session_key, role, content, metadata_json, created_at, platform_message_id)
             VALUES (?, ?, 'user', ?, ?, ?, ?)
             ON CONFLICT(id) DO NOTHING",
        )
        .bind(event.id.to_string())
        .bind(self.context.key.storage_key())
        .bind(strip_leading_message_timestamps(&render_user_prompt(event)))
        .bind(serde_json::to_string(&event.attachments).map_err(serialization_error)?)
        .bind(event.received_at)
        .bind(if event.platform_message_id.is_empty() {
            None
        } else {
            Some(&event.platform_message_id)
        })
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

async fn load_context(
    pool: &SqlitePool,
    key: SessionKey,
    profile_router: Option<&ProfileRouter>,
) -> Result<SessionContext> {
    let row: Option<(String, chrono::DateTime<Utc>, chrono::DateTime<Utc>)> = sqlx::query_as(
        "SELECT state_json, created_at, updated_at FROM sessions WHERE session_key = ?",
    )
    .bind(key.storage_key())
    .fetch_optional(pool)
    .await?;
    match row {
        Some((state_json, created_at, updated_at)) => {
            let mut state: SessionState =
                serde_json::from_str(&state_json).map_err(serialization_error)?;

            // Check for bot-specific profile override first if bot_id is present
            if let Some(bot_id) = key.bot_id.as_deref() {
                if let Ok(Some((model, prompt, toolsets))) = sqlx::query_as::<_, (Option<String>, Option<String>, Option<String>)>(
                    "SELECT model, system_prompt, enabled_toolsets FROM bot_profiles WHERE bot_id = ?"
                )
                .bind(bot_id)
                .fetch_optional(pool)
                .await {
                    if state.active_model.is_none() && model.is_some() {
                        state.active_model = model;
                    }
                    if state.system_prompt.is_none() && prompt.is_some() {
                        state.system_prompt = prompt;
                    }
                    if state.enabled_toolsets.is_none() && toolsets.is_some() {
                        state.enabled_toolsets = toolsets.map(|t| t.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect());
                    }
                }
            }

            if let Some(router) = profile_router {
                if let Some(route) = router.match_session(&key) {
                    if state.active_model.is_none() {
                        state.active_model = route.model.clone();
                    }
                    if state.system_prompt.is_none() {
                        state.system_prompt = route.system_prompt.clone();
                    }
                    if state.enabled_toolsets.is_none() {
                        state.enabled_toolsets = route.enabled_toolsets.clone();
                    }
                    if let Some(toolsets) = &route.enabled_toolsets {
                        state
                            .metadata
                            .entry("enabled_toolsets".into())
                            .or_insert_with(|| serde_json::json!(toolsets));
                    }
                }
            }
            Ok(SessionContext {
                key,
                state,
                created_at,
                updated_at,
            })
        }
        None => {
            let mut context = SessionContext::new(key);
            if let Some(bot_id) = context.key.bot_id.as_deref() {
                if let Ok(Some((model, prompt, toolsets))) = sqlx::query_as::<_, (Option<String>, Option<String>, Option<String>)>(
                    "SELECT model, system_prompt, enabled_toolsets FROM bot_profiles WHERE bot_id = ?"
                )
                .bind(bot_id)
                .fetch_optional(pool)
                .await {
                    if model.is_some() {
                        context.state.active_model = model;
                    }
                    if prompt.is_some() {
                        context.state.system_prompt = prompt;
                    }
                    if toolsets.is_some() {
                        context.state.enabled_toolsets = toolsets.map(|t| t.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect());
                    }
                }
            }
            if let Some(router) = profile_router {
                router.apply_to_session(&mut context);
            }
            Ok(context)
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Database;
    use tokio::sync::Barrier;

    fn test_session(user: &str) -> SessionKey {
        SessionKey::new("discord", Some("guild"), "channel", None::<String>, user)
    }

    struct TurnRecordingRunner {
        started: mpsc::UnboundedSender<String>,
        barrier: Arc<Barrier>,
        completed: mpsc::UnboundedSender<String>,
    }

    #[async_trait]
    impl AgentRunner for TurnRecordingRunner {
        async fn run(&self, _session: &mut SessionContext, event: InboundEvent) -> Result<()> {
            let _ = self.started.send(event.content.clone());
            if event.content == "blocking" {
                self.barrier.wait().await;
            }
            let _ = self.completed.send(event.content);
            Ok(())
        }
    }

    #[tokio::test]
    async fn actor_queues_turns_in_order_without_cancellation() {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        let key = test_session("actor-queue-test");
        let (started_tx, mut started_rx) = mpsc::unbounded_channel();
        let (completed_tx, mut completed_rx) = mpsc::unbounded_channel();
        let barrier = Arc::new(Barrier::new(2));

        let runner = Arc::new(TurnRecordingRunner {
            started: started_tx,
            barrier: barrier.clone(),
            completed: completed_tx,
        });

        let (cmd_tx, cmd_rx) = mpsc::channel(32);
        let actor = SessionActor::load(key.clone(), cmd_rx, runner, None, db.pool().clone(), None)
            .await
            .unwrap();
        let handle = tokio::spawn(actor.run());

        // Send first blocking turn
        cmd_tx
            .send(ActorCommand::Event(Box::new(InboundEvent::message(
                key.clone(),
                "msg-1",
                "blocking",
            ))))
            .await
            .unwrap();

        assert_eq!(started_rx.recv().await.as_deref(), Some("blocking"));

        // Send two more events while first is blocking
        cmd_tx
            .send(ActorCommand::Event(Box::new(InboundEvent::message(
                key.clone(),
                "msg-2",
                "follow-up-1",
            ))))
            .await
            .unwrap();
        cmd_tx
            .send(ActorCommand::Event(Box::new(InboundEvent::message(
                key.clone(),
                "msg-3",
                "follow-up-2",
            ))))
            .await
            .unwrap();

        // Release the first turn
        barrier.wait().await;

        assert_eq!(completed_rx.recv().await.as_deref(), Some("blocking"));
        assert_eq!(started_rx.recv().await.as_deref(), Some("follow-up-1"));
        assert_eq!(completed_rx.recv().await.as_deref(), Some("follow-up-1"));
        assert_eq!(started_rx.recv().await.as_deref(), Some("follow-up-2"));
        assert_eq!(completed_rx.recv().await.as_deref(), Some("follow-up-2"));

        drop(cmd_tx);
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn actor_stop_cancels_active_turn_immediately() {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        let key = test_session("actor-stop-test");
        let (started_tx, mut started_rx) = mpsc::unbounded_channel();
        let (completed_tx, mut completed_rx) = mpsc::unbounded_channel();
        let barrier = Arc::new(Barrier::new(2));

        let runner = Arc::new(TurnRecordingRunner {
            started: started_tx,
            barrier: barrier.clone(),
            completed: completed_tx,
        });

        let (cmd_tx, cmd_rx) = mpsc::channel(32);
        let actor = SessionActor::load(key.clone(), cmd_rx, runner, None, db.pool().clone(), None)
            .await
            .unwrap();
        let handle = tokio::spawn(actor.run());

        // Send blocking turn
        cmd_tx
            .send(ActorCommand::Event(Box::new(InboundEvent::message(
                key.clone(),
                "msg-1",
                "blocking",
            ))))
            .await
            .unwrap();

        assert_eq!(started_rx.recv().await.as_deref(), Some("blocking"));

        // Send Stop command
        let (reply_tx, reply_rx) = oneshot::channel();
        cmd_tx
            .send(ActorCommand::Stop { reply: reply_tx })
            .await
            .unwrap();

        let stop_result = reply_rx.await.unwrap();
        assert!(stop_result.unwrap());

        // Turn did not complete successfully
        assert!(completed_rx.try_recv().is_err());

        drop(cmd_tx);
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn actor_load_applies_profile_routes_to_fresh_and_preserves_explicit_model() {
        use crate::ProfileRoute;

        let db = Database::connect("sqlite::memory:").await.unwrap();
        let route = ProfileRoute {
            name: Some("profile-1".into()),
            guild: Some(123),
            channel: Some(456),
            thread: None,
            enabled: true,
            model: Some("profile-model".into()),
            system_prompt: Some("profile-prompt".into()),
            enabled_toolsets: Some(vec!["terminal".into(), "web".into()]),
        };
        let router = Arc::new(ProfileRouter::new(vec![route]));

        // 1. Fresh session (not in DB)
        let key = SessionKey::new(
            "discord",
            Some("123"),
            "456",
            None::<String>,
            "user-profile-test",
        );
        let (_tx1, rx1) = mpsc::channel(32);
        let runner = Arc::new(TurnRecordingRunner {
            started: mpsc::unbounded_channel().0,
            barrier: Arc::new(Barrier::new(1)),
            completed: mpsc::unbounded_channel().0,
        });
        let actor = SessionActor::load(
            key.clone(),
            rx1,
            runner.clone(),
            None,
            db.pool().clone(),
            Some(router.clone()),
        )
        .await
        .unwrap();

        assert_eq!(
            actor.context.state.active_model.as_deref(),
            Some("profile-model")
        );
        assert_eq!(
            actor.context.state.system_prompt.as_deref(),
            Some("profile-prompt")
        );
        assert_eq!(
            actor.context.state.enabled_toolsets.as_deref(),
            Some(&["terminal".to_string(), "web".to_string()][..])
        );

        // 2. Existing session in DB with explicit model set via `/model`
        let key2 = SessionKey::new(
            "discord",
            Some("123"),
            "456",
            None::<String>,
            "user-explicit-model",
        );
        let explicit_state = SessionState {
            active_model: Some("explicit-user-model".into()),
            ..Default::default()
        };
        let explicit_json = serde_json::to_string(&explicit_state).unwrap();

        sqlx::query(
            "INSERT INTO sessions (session_key, platform, guild_id, channel_id, user_id, state_json) VALUES (?, 'discord', '123', '456', 'user-explicit-model', ?)"
        )
        .bind(key2.storage_key())
        .bind(&explicit_json)
        .execute(db.pool())
        .await
        .unwrap();

        let (_tx2, rx2) = mpsc::channel(32);
        let actor2 = SessionActor::load(
            key2.clone(),
            rx2,
            runner.clone(),
            None,
            db.pool().clone(),
            Some(router.clone()),
        )
        .await
        .unwrap();

        // Explicit model must NOT be clobbered by profile
        assert_eq!(
            actor2.context.state.active_model.as_deref(),
            Some("explicit-user-model")
        );
        // But unset prompt and toolsets are populated from profile defaults
        assert_eq!(
            actor2.context.state.system_prompt.as_deref(),
            Some("profile-prompt")
        );
        assert_eq!(
            actor2.context.state.enabled_toolsets.as_deref(),
            Some(&["terminal".to_string(), "web".to_string()][..])
        );
    }
}
