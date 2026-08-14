use std::sync::Arc;
use std::time::Duration;

use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use sqlx::SqlitePool;
use tokio::sync::mpsc;

use crate::{InboundEvent, OmonError, Result, SessionKey};

use super::actor::{ActorCommand, AgentRunner, OutboundDispatcher, SessionActor};

const SESSION_CHANNEL_CAPACITY: usize = 64;

#[derive(Clone, Debug)]
pub struct MultiplexerConfig {
    pub idle_timeout: Duration,
    pub gc_interval: Duration,
}

impl Default for MultiplexerConfig {
    fn default() -> Self {
        Self {
            idle_timeout: Duration::from_secs(10 * 60),
            gc_interval: Duration::from_secs(60),
        }
    }
}

#[derive(Clone)]
pub struct SessionMultiplexer {
    pub(crate) sessions: Arc<DashMap<SessionKey, mpsc::Sender<ActorCommand>>>,
    runner: Arc<dyn AgentRunner>,
    dispatcher: Option<Arc<dyn OutboundDispatcher>>,
    pool: SqlitePool,
    config: MultiplexerConfig,
}

impl SessionMultiplexer {
    pub fn new(pool: SqlitePool, runner: Arc<dyn AgentRunner>, config: MultiplexerConfig) -> Self {
        Self::with_dispatcher(pool, runner, None, config)
    }

    pub fn with_dispatcher(
        pool: SqlitePool,
        runner: Arc<dyn AgentRunner>,
        dispatcher: Option<Arc<dyn OutboundDispatcher>>,
        config: MultiplexerConfig,
    ) -> Self {
        Self {
            sessions: Arc::new(DashMap::new()),
            runner,
            dispatcher,
            pool,
            config,
        }
    }

    pub async fn route(&self, event: InboundEvent) -> Result<()> {
        let key = event.session.clone();
        loop {
            let sender = self.sender_for(&key);
            match sender.send(ActorCommand::Event(event.clone())).await {
                Ok(()) => return Ok(()),
                Err(_) => {
                    self.sessions
                        .remove_if(&key, |_, current| current.same_channel(&sender));
                }
            }
        }
    }

    pub fn active_sessions(&self) -> usize {
        self.sessions.len()
    }

    pub fn contains_session(&self, key: &SessionKey) -> bool {
        self.sessions.contains_key(key)
    }

    pub async fn collect_garbage(&self) -> Result<usize> {
        super::gc::collect(self).await
    }

    fn sender_for(&self, key: &SessionKey) -> mpsc::Sender<ActorCommand> {
        if let Some(sender) = self.sessions.get(key) {
            return sender.clone();
        }

        let (sender, receiver) = mpsc::channel(SESSION_CHANNEL_CAPACITY);
        match self.sessions.entry(key.clone()) {
            Entry::Occupied(entry) => entry.get().clone(),
            Entry::Vacant(entry) => {
                entry.insert(sender.clone());
                self.spawn_actor(key.clone(), receiver, sender.clone());
                sender
            }
        }
    }

    fn spawn_actor(
        &self,
        key: SessionKey,
        receiver: mpsc::Receiver<ActorCommand>,
        sender: mpsc::Sender<ActorCommand>,
    ) {
        let sessions = self.sessions.clone();
        let runner = self.runner.clone();
        let dispatcher = self.dispatcher.clone();
        let pool = self.pool.clone();
        tokio::spawn(async move {
            match SessionActor::load(key.clone(), receiver, runner, dispatcher, pool).await {
                Ok(actor) => actor.run().await,
                Err(error) => {
                    tracing::error!(session = %key, %error, "failed to start session actor")
                }
            }
            sessions.remove_if(&key, |_, current| current.same_channel(&sender));
        });
    }

    pub(crate) fn idle_timeout(&self) -> Duration {
        self.config.idle_timeout
    }

    pub(crate) fn gc_interval(&self) -> Duration {
        self.config.gc_interval
    }
}

impl From<mpsc::error::SendError<ActorCommand>> for OmonError {
    fn from(_: mpsc::error::SendError<ActorCommand>) -> Self {
        OmonError::Multiplexer("session actor stopped before accepting an event".into())
    }
}
