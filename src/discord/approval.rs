use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serenity::all::{ButtonStyle, CreateActionRow, CreateButton};
use thiserror::Error;
use tokio::sync::{oneshot, Mutex, RwLock};
use uuid::Uuid;

use crate::{OutboundAction, OutboundDispatcher, SessionKey};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApprovalDecision {
    Approved,
    Rejected,
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum ApprovalError {
    #[error("approval request timed out")]
    Timeout,
    #[error("approval request was cancelled")]
    Cancelled,
}

#[derive(Debug)]
pub struct ApprovalPrompt {
    pub request_id: Uuid,
    pub components: Vec<CreateActionRow>,
    receiver: oneshot::Receiver<ApprovalDecision>,
}

impl ApprovalPrompt {
    pub async fn wait(self, timeout: Duration) -> Result<ApprovalDecision, ApprovalError> {
        match tokio::time::timeout(timeout, self.receiver).await {
            Ok(Ok(decision)) => Ok(decision),
            Ok(Err(_)) => Err(ApprovalError::Cancelled),
            Err(_) => Err(ApprovalError::Timeout),
        }
    }
}

/// Tracks pending approval requests and resolves Discord button interactions
/// through a one-shot channel. Custom IDs contain an unguessable request UUID.
#[derive(Clone, Default)]
pub struct SmartApprovalGuard {
    pending: Arc<Mutex<HashMap<Uuid, oneshot::Sender<ApprovalDecision>>>>,
}

#[async_trait]
pub trait ApprovalRequester: Send + Sync {
    async fn request_approval(
        &self,
        session: &SessionKey,
        command: &str,
    ) -> Result<ApprovalDecision, ApprovalError>;
}

#[derive(Clone)]
pub struct DiscordApprovalRequester {
    guard: SmartApprovalGuard,
    dispatcher: Arc<RwLock<Option<Arc<dyn OutboundDispatcher>>>>,
    timeout: Duration,
}

impl DiscordApprovalRequester {
    pub fn new(guard: SmartApprovalGuard, timeout: Duration) -> Self {
        Self {
            guard,
            dispatcher: Arc::new(RwLock::new(None)),
            timeout,
        }
    }

    pub async fn set_dispatcher(&self, dispatcher: Arc<dyn OutboundDispatcher>) {
        *self.dispatcher.write().await = Some(dispatcher);
    }
}

#[async_trait]
impl ApprovalRequester for DiscordApprovalRequester {
    async fn request_approval(
        &self,
        session: &SessionKey,
        command: &str,
    ) -> Result<ApprovalDecision, ApprovalError> {
        let dispatcher = self
            .dispatcher
            .read()
            .await
            .clone()
            .ok_or(ApprovalError::Cancelled)?;
        let prompt = self.guard.request().await;
        let request_id = prompt.request_id;
        if dispatcher
            .dispatch(OutboundAction::ApprovalRequest {
                session: session.clone(),
                request_id,
                command: command.to_owned(),
            })
            .await
            .is_err()
        {
            self.guard.cancel(request_id).await;
            return Err(ApprovalError::Cancelled);
        }
        let result = prompt.wait(self.timeout).await;
        if result.is_err() {
            self.guard.cancel(request_id).await;
        }
        result
    }
}

impl SmartApprovalGuard {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn request(&self) -> ApprovalPrompt {
        let request_id = Uuid::new_v4();
        let (sender, receiver) = oneshot::channel();
        self.pending.lock().await.insert(request_id, sender);
        ApprovalPrompt {
            request_id,
            components: approval_buttons(request_id),
            receiver,
        }
    }

    pub async fn resolve_custom_id(&self, custom_id: &str) -> bool {
        let Some((request_id, decision)) = parse_custom_id(custom_id) else {
            return false;
        };
        let Some(sender) = self.pending.lock().await.remove(&request_id) else {
            return false;
        };
        sender.send(decision).is_ok()
    }

    pub async fn cancel(&self, request_id: Uuid) {
        self.pending.lock().await.remove(&request_id);
    }

    pub async fn pending_count(&self) -> usize {
        self.pending.lock().await.len()
    }
}

pub fn approval_buttons(request_id: Uuid) -> Vec<CreateActionRow> {
    vec![CreateActionRow::Buttons(vec![
        CreateButton::new(format!("omon:approval:{request_id}:approve"))
            .label("Approve")
            .style(ButtonStyle::Success),
        CreateButton::new(format!("omon:approval:{request_id}:reject"))
            .label("Reject")
            .style(ButtonStyle::Danger),
    ])]
}

fn parse_custom_id(custom_id: &str) -> Option<(Uuid, ApprovalDecision)> {
    let mut parts = custom_id.split(':');
    if parts.next()? != "omon" || parts.next()? != "approval" {
        return None;
    }
    let request_id = Uuid::parse_str(parts.next()?).ok()?;
    let decision = match parts.next()? {
        "approve" => ApprovalDecision::Approved,
        "reject" => ApprovalDecision::Rejected,
        _ => return None,
    };
    if parts.next().is_some() {
        return None;
    }
    Some((request_id, decision))
}
