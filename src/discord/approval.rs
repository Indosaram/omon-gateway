use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serenity::all::{ButtonStyle, CreateActionRow, CreateButton};
use sqlx::SqlitePool;
use thiserror::Error;
use tokio::sync::{oneshot, Mutex, RwLock};
use uuid::Uuid;

use crate::{OutboundAction, OutboundDispatcher, SessionKey};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalDecision {
    #[serde(alias = "approved")]
    Once,
    Session,
    Always,
    #[serde(alias = "rejected")]
    Deny {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
    },
}

impl ApprovalDecision {
    pub fn is_approved(&self) -> bool {
        matches!(self, Self::Once | Self::Session | Self::Always)
    }

    pub fn deny(reason: Option<String>) -> Self {
        Self::Deny { reason }
    }
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum ApprovalError {
    #[error("approval request timed out")]
    Timeout,
    #[error("approval request was cancelled")]
    Cancelled,
}

pub const DEFAULT_APPROVAL_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);
pub type ActivityHeartbeat = Arc<dyn Fn(&SessionKey) + Send + Sync>;

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

    pub async fn wait_with_heartbeat<F>(
        self,
        timeout: Duration,
        interval: Duration,
        mut heartbeat_fn: F,
    ) -> Result<ApprovalDecision, ApprovalError>
    where
        F: FnMut() + Send,
    {
        let deadline = tokio::time::Instant::now() + timeout;
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        ticker.tick().await;

        let mut receiver = self.receiver;
        loop {
            tokio::select! {
                biased;
                res = &mut receiver => {
                    return match res {
                        Ok(decision) => Ok(decision),
                        Err(_) => Err(ApprovalError::Cancelled),
                    };
                }
                _ = tokio::time::sleep_until(deadline) => {
                    return Err(ApprovalError::Timeout);
                }
                _ = ticker.tick() => {
                    heartbeat_fn();
                }
            }
        }
    }
}

struct PendingApprovalEntry {
    session: Option<SessionKey>,
    sender: oneshot::Sender<ApprovalDecision>,
    created_at: std::time::Instant,
}

/// Tracks pending approval requests, resolves Discord button interactions
/// through a one-shot channel, and maintains per-session and global approval caches.
#[derive(Clone, Default)]
pub struct SmartApprovalGuard {
    pending: Arc<Mutex<HashMap<Uuid, PendingApprovalEntry>>>,
    session_cache: Arc<RwLock<HashMap<SessionKey, HashSet<String>>>>,
    yolo_sessions: Arc<RwLock<HashSet<SessionKey>>>,
    always_cache: Arc<RwLock<HashSet<String>>>,
    pool: Arc<RwLock<Option<SqlitePool>>>,
}

#[async_trait]
pub trait ApprovalRequester: Send + Sync {
    async fn request_approval(
        &self,
        session: &SessionKey,
        command: &str,
        reason: &str,
    ) -> Result<ApprovalDecision, ApprovalError>;

    async fn is_yolo(&self, _session: &SessionKey) -> bool {
        false
    }
}

#[derive(Clone)]
pub struct DiscordApprovalRequester {
    guard: SmartApprovalGuard,
    dispatcher: Arc<RwLock<Option<Arc<dyn OutboundDispatcher>>>>,
    heartbeat: Arc<RwLock<Option<ActivityHeartbeat>>>,
    heartbeat_interval: Duration,
    timeout: Duration,
}

impl DiscordApprovalRequester {
    pub fn new(guard: SmartApprovalGuard, timeout: Duration) -> Self {
        Self {
            guard,
            dispatcher: Arc::new(RwLock::new(None)),
            heartbeat: Arc::new(RwLock::new(None)),
            heartbeat_interval: DEFAULT_APPROVAL_HEARTBEAT_INTERVAL,
            timeout,
        }
    }

    pub fn with_heartbeat(self, heartbeat: ActivityHeartbeat) -> Self {
        *self.heartbeat.try_write().expect("uncontended lock") = Some(heartbeat);
        self
    }

    pub fn with_heartbeat_interval(mut self, interval: Duration) -> Self {
        self.heartbeat_interval = interval;
        self
    }

    pub async fn set_heartbeat(&self, heartbeat: ActivityHeartbeat) {
        *self.heartbeat.write().await = Some(heartbeat);
    }

    pub async fn set_dispatcher(&self, dispatcher: Arc<dyn OutboundDispatcher>) {
        *self.dispatcher.write().await = Some(dispatcher);
    }

    pub fn guard(&self) -> &SmartApprovalGuard {
        &self.guard
    }
}

#[async_trait]
impl ApprovalRequester for DiscordApprovalRequester {
    async fn is_yolo(&self, session: &SessionKey) -> bool {
        self.guard.is_yolo(session).await
    }

    async fn request_approval(
        &self,
        session: &SessionKey,
        command: &str,
        reason: &str,
    ) -> Result<ApprovalDecision, ApprovalError> {
        if self.guard.is_yolo(session).await {
            return Ok(ApprovalDecision::Once);
        }

        let pattern_key = crate::security::derive_pattern_key(command);
        if self.guard.is_approved(session, &pattern_key).await {
            return Ok(ApprovalDecision::Session);
        }

        let dispatcher = self
            .dispatcher
            .read()
            .await
            .clone()
            .ok_or(ApprovalError::Cancelled)?;
        let prompt = self.guard.request_with_session(Some(session.clone())).await;
        let request_id = prompt.request_id;
        if dispatcher
            .dispatch(OutboundAction::ApprovalRequest {
                session: session.clone(),
                request_id,
                command: command.to_owned(),
                reason: reason.to_owned(),
            })
            .await
            .is_err()
        {
            self.guard.cancel(request_id).await;
            return Err(ApprovalError::Cancelled);
        }
        let heartbeat = self.heartbeat.read().await.clone();
        let heartbeat_interval = self.heartbeat_interval;
        let result = match heartbeat {
            Some(hb) => {
                let session_clone = session.clone();
                prompt
                    .wait_with_heartbeat(self.timeout, heartbeat_interval, move || {
                        hb(&session_clone);
                    })
                    .await
            }
            None => prompt.wait(self.timeout).await,
        };
        if let Ok(decision) = &result {
            match decision {
                ApprovalDecision::Session => {
                    self.guard.approve_session(session, &pattern_key).await;
                }
                ApprovalDecision::Always => {
                    self.guard.approve_always(&pattern_key).await;
                }
                _ => {}
            }
        } else {
            self.guard.cancel(request_id).await;
            let _ = dispatcher
                .dispatch(OutboundAction::ExpireApproval { request_id })
                .await;
        }
        result
    }
}

impl SmartApprovalGuard {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_pool(self, pool: SqlitePool) -> Self {
        *self.pool.try_write().expect("uncontended lock") = Some(pool);
        self
    }

    pub async fn set_pool(&self, pool: SqlitePool) {
        *self.pool.write().await = Some(pool);
    }

    pub async fn load_persisted_allowlist(&self) -> Result<usize, sqlx::Error> {
        let pool = self.pool.read().await.clone();
        let Some(pool) = pool else {
            return Ok(0);
        };
        let rows: Vec<(String,)> = sqlx::query_as("SELECT pattern_key FROM approval_allowlist")
            .fetch_all(&pool)
            .await?;
        let mut always = self.always_cache.write().await;
        let count = rows.len();
        for (pattern,) in rows {
            always.insert(pattern);
        }
        Ok(count)
    }

    pub async fn is_approved(&self, session: &SessionKey, pattern_key: &str) -> bool {
        if self.always_cache.read().await.contains(pattern_key) {
            return true;
        }
        if let Some(session_patterns) = self.session_cache.read().await.get(session) {
            return session_patterns.contains(pattern_key);
        }
        false
    }

    pub async fn approve_session(&self, session: &SessionKey, pattern_key: &str) {
        let mut cache = self.session_cache.write().await;
        cache
            .entry(session.clone())
            .or_default()
            .insert(pattern_key.to_string());
    }

    pub async fn approve_always(&self, pattern_key: &str) {
        self.always_cache
            .write()
            .await
            .insert(pattern_key.to_string());

        let pool = self.pool.read().await.clone();
        if let Some(pool) = pool {
            let pattern = pattern_key.to_string();
            if let Err(error) = sqlx::query(
                "INSERT INTO approval_allowlist (pattern_key) VALUES (?) ON CONFLICT(pattern_key) DO NOTHING",
            )
            .bind(&pattern)
            .execute(&pool)
            .await
            {
                tracing::warn!(%error, pattern = %pattern, "failed to persist always-allow approval");
            }
        }
    }

    pub async fn approve_permanent(&self, pattern_key: &str) {
        self.approve_always(pattern_key).await;
    }

    pub async fn load_permanent(&self, patterns: impl IntoIterator<Item = String>) {
        let mut always = self.always_cache.write().await;
        for pat in patterns {
            always.insert(pat);
        }
    }

    pub async fn is_yolo(&self, session: &SessionKey) -> bool {
        self.yolo_sessions.read().await.contains(session)
    }

    pub async fn set_yolo(&self, session: &SessionKey, enabled: bool) {
        let mut yolo = self.yolo_sessions.write().await;
        if enabled {
            yolo.insert(session.clone());
        } else {
            yolo.remove(session);
        }
    }

    pub async fn clear_session(&self, session: &SessionKey) {
        self.session_cache.write().await.remove(session);
        self.yolo_sessions.write().await.remove(session);
    }

    pub async fn request(&self) -> ApprovalPrompt {
        self.request_with_session(None).await
    }

    pub async fn request_with_session(&self, session: Option<SessionKey>) -> ApprovalPrompt {
        let request_id = Uuid::new_v4();
        let (sender, receiver) = oneshot::channel();
        self.pending.lock().await.insert(
            request_id,
            PendingApprovalEntry {
                session,
                sender,
                created_at: std::time::Instant::now(),
            },
        );
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
        let Some(entry) = self.pending.lock().await.remove(&request_id) else {
            return false;
        };
        entry.sender.send(decision).is_ok()
    }

    pub async fn resolve_session_deny(&self, session: &SessionKey, reason: Option<String>) -> bool {
        let mut lock = self.pending.lock().await;
        let target = lock
            .iter()
            .filter(|(_, entry)| {
                if let Some(s) = &entry.session {
                    s == session
                        || (s.platform == session.platform
                            && s.channel_id == session.channel_id
                            && s.thread_id == session.thread_id)
                } else {
                    false
                }
            })
            .max_by_key(|(_, entry)| entry.created_at)
            .map(|(id, _)| *id);

        let target_id = target.or_else(|| {
            lock.iter()
                .filter(|(_, entry)| {
                    if let Some(s) = &entry.session {
                        s.channel_id == session.channel_id
                    } else {
                        false
                    }
                })
                .max_by_key(|(_, entry)| entry.created_at)
                .map(|(id, _)| *id)
        });

        if let Some(request_id) = target_id {
            if let Some(entry) = lock.remove(&request_id) {
                return entry.sender.send(ApprovalDecision::Deny { reason }).is_ok();
            }
        }
        false
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
        CreateButton::new(format!("omon:approval:{request_id}:once"))
            .label("Allow Once")
            .style(ButtonStyle::Primary),
        CreateButton::new(format!("omon:approval:{request_id}:session"))
            .label("Allow Session")
            .style(ButtonStyle::Success),
        CreateButton::new(format!("omon:approval:{request_id}:always"))
            .label("Always Allow")
            .style(ButtonStyle::Success),
        CreateButton::new(format!("omon:approval:{request_id}:deny"))
            .label("Deny")
            .style(ButtonStyle::Danger),
    ])]
}

pub fn is_approval_custom_id(custom_id: &str) -> bool {
    parse_custom_id(custom_id).is_some()
}

pub fn parse_custom_id(custom_id: &str) -> Option<(Uuid, ApprovalDecision)> {
    let mut parts = custom_id.split(':');
    if parts.next()? != "omon" || parts.next()? != "approval" {
        return None;
    }
    let request_id = Uuid::parse_str(parts.next()?).ok()?;
    let decision = match parts.next()? {
        "once" | "approve" => ApprovalDecision::Once,
        "session" => ApprovalDecision::Session,
        "always" => ApprovalDecision::Always,
        "deny" | "reject" => ApprovalDecision::Deny { reason: None },
        _ => return None,
    };
    if parts.next().is_some() {
        return None;
    }
    Some((request_id, decision))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use super::*;

    #[test]
    fn test_is_approval_custom_id() {
        let id = Uuid::new_v4();
        assert!(is_approval_custom_id(&format!("omon:approval:{id}:once")));
        assert!(is_approval_custom_id(&format!(
            "omon:approval:{id}:session"
        )));
        assert!(is_approval_custom_id(&format!("omon:approval:{id}:always")));
        assert!(is_approval_custom_id(&format!("omon:approval:{id}:deny")));
        assert!(is_approval_custom_id(&format!(
            "omon:approval:{id}:approve"
        )));
        assert!(is_approval_custom_id(&format!("omon:approval:{id}:reject")));
        assert!(!is_approval_custom_id("other:custom:id"));
        assert!(!is_approval_custom_id("omon:approval:not-a-uuid:once"));
        assert!(!is_approval_custom_id("omon:approval:"));
        assert!(!is_approval_custom_id(&format!(
            "omon:approval:{id}:unknown"
        )));
        assert!(!is_approval_custom_id(&format!(
            "omon:approval:{id}:once:extra"
        )));
    }

    #[test]
    fn test_parse_custom_id_round_trip() {
        let id = Uuid::new_v4();
        assert_eq!(
            parse_custom_id(&format!("omon:approval:{id}:once")),
            Some((id, ApprovalDecision::Once))
        );
        assert_eq!(
            parse_custom_id(&format!("omon:approval:{id}:session")),
            Some((id, ApprovalDecision::Session))
        );
        assert_eq!(
            parse_custom_id(&format!("omon:approval:{id}:always")),
            Some((id, ApprovalDecision::Always))
        );
        assert_eq!(
            parse_custom_id(&format!("omon:approval:{id}:deny")),
            Some((id, ApprovalDecision::Deny { reason: None }))
        );
        assert_eq!(
            parse_custom_id(&format!("omon:approval:{id}:approve")),
            Some((id, ApprovalDecision::Once))
        );
        assert_eq!(
            parse_custom_id(&format!("omon:approval:{id}:reject")),
            Some((id, ApprovalDecision::Deny { reason: None }))
        );
    }

    #[tokio::test]
    async fn test_resolve_custom_id_unknown_uuid() {
        let guard = SmartApprovalGuard::new();
        let unknown_id = Uuid::new_v4();
        let custom_id = format!("omon:approval:{unknown_id}:once");
        assert!(!guard.resolve_custom_id(&custom_id).await);
    }

    struct MockDispatcher;

    #[async_trait]
    impl OutboundDispatcher for MockDispatcher {
        async fn dispatch(&self, _action: OutboundAction) -> crate::Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_session_approval_cache_suppresses_repeat_prompts() {
        let guard = SmartApprovalGuard::new();
        let requester = DiscordApprovalRequester::new(guard.clone(), Duration::from_millis(50));
        requester.set_dispatcher(Arc::new(MockDispatcher)).await;

        let session_a =
            SessionKey::new("discord", None::<String>, "chan1", None::<String>, "user1");
        let session_b =
            SessionKey::new("discord", None::<String>, "chan2", None::<String>, "user2");

        let cmd = "rm -rf /tmp/build";
        let pattern = crate::security::derive_pattern_key(cmd);
        assert!(!guard.is_approved(&session_a, &pattern).await);
        assert!(!guard.is_approved(&session_b, &pattern).await);

        // Approve session A
        guard.approve_session(&session_a, &pattern).await;

        assert!(guard.is_approved(&session_a, &pattern).await);
        assert!(!guard.is_approved(&session_b, &pattern).await);

        // requester immediately auto-approves session A without prompting
        let decision = requester
            .request_approval(&session_a, cmd, "recursive delete")
            .await
            .unwrap();
        assert_eq!(decision, ApprovalDecision::Session);

        // session B is not cached and times out
        let err = requester
            .request_approval(&session_b, cmd, "recursive delete")
            .await
            .unwrap_err();
        assert_eq!(err, ApprovalError::Timeout);
    }

    #[tokio::test]
    async fn test_always_approval_cache_applies_globally() {
        let guard = SmartApprovalGuard::new();
        let session_a =
            SessionKey::new("discord", None::<String>, "chan1", None::<String>, "user1");
        let session_b =
            SessionKey::new("discord", None::<String>, "chan2", None::<String>, "user2");

        let pattern = "disk copy";
        assert!(!guard.is_approved(&session_a, pattern).await);
        assert!(!guard.is_approved(&session_b, pattern).await);

        guard.approve_always(pattern).await;

        assert!(guard.is_approved(&session_a, pattern).await);
        assert!(guard.is_approved(&session_b, pattern).await);
    }

    #[tokio::test]
    async fn test_resolve_session_deny_with_reason() {
        let guard = SmartApprovalGuard::new();
        let session = SessionKey::new("discord", None::<String>, "chan1", None::<String>, "user1");

        let prompt = guard.request_with_session(Some(session.clone())).await;

        let reason = Some("unsafe directory operation".to_string());
        assert!(guard.resolve_session_deny(&session, reason.clone()).await);

        let decision = prompt.wait(Duration::from_millis(50)).await.unwrap();
        assert_eq!(decision, ApprovalDecision::Deny { reason });

        // Subsequent resolve fails
        assert!(!guard.resolve_session_deny(&session, None).await);
    }

    #[tokio::test]
    async fn test_yolo_toggle_and_auto_approval() {
        let guard = SmartApprovalGuard::new();
        let requester = DiscordApprovalRequester::new(guard.clone(), Duration::from_millis(50));
        requester.set_dispatcher(Arc::new(MockDispatcher)).await;

        let session_a =
            SessionKey::new("discord", None::<String>, "chan1", None::<String>, "user1");
        let session_b =
            SessionKey::new("discord", None::<String>, "chan2", None::<String>, "user2");

        assert!(!guard.is_yolo(&session_a).await);
        assert!(!guard.is_yolo(&session_b).await);
        assert!(!requester.is_yolo(&session_a).await);

        guard.set_yolo(&session_a, true).await;
        assert!(guard.is_yolo(&session_a).await);
        assert!(requester.is_yolo(&session_a).await);
        assert!(!guard.is_yolo(&session_b).await);

        // Session A auto-approves via request_approval
        let decision = requester
            .request_approval(&session_a, "rm -rf /tmp/scratch", "recursive delete")
            .await
            .unwrap();
        assert!(decision.is_approved());

        // Session B still times out
        let err = requester
            .request_approval(&session_b, "rm -rf /tmp/scratch", "recursive delete")
            .await
            .unwrap_err();
        assert_eq!(err, ApprovalError::Timeout);

        // Disable YOLO
        guard.set_yolo(&session_a, false).await;
        assert!(!guard.is_yolo(&session_a).await);

        // Clear session clears YOLO
        guard.set_yolo(&session_a, true).await;
        assert!(guard.is_yolo(&session_a).await);
        guard.clear_session(&session_a).await;
        assert!(!guard.is_yolo(&session_a).await);
    }

    #[tokio::test]
    async fn test_wait_with_heartbeat_fires_periodically_and_stops_on_resolve() {
        let guard = SmartApprovalGuard::new();
        let prompt = guard.request().await;
        let request_id = prompt.request_id;

        let heartbeat_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let hb = heartbeat_count.clone();

        let wait_handle = tokio::spawn(async move {
            prompt
                .wait_with_heartbeat(
                    Duration::from_millis(200),
                    Duration::from_millis(20),
                    move || {
                        hb.fetch_add(1, Ordering::SeqCst);
                    },
                )
                .await
        });

        // Wait 55ms, verify at least 2 heartbeats fired
        tokio::time::sleep(Duration::from_millis(55)).await;
        let custom_id = format!("omon:approval:{request_id}:once");
        assert!(guard.resolve_custom_id(&custom_id).await);

        let result = wait_handle.await.unwrap();
        assert_eq!(result, Ok(ApprovalDecision::Once));

        let count_after_resolve = heartbeat_count.load(Ordering::SeqCst);
        assert!(count_after_resolve >= 2);

        // Sleep longer, verify heartbeat stopped firing after resolution
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(heartbeat_count.load(Ordering::SeqCst), count_after_resolve);
    }

    #[tokio::test]
    async fn test_requester_heartbeat_invoked_during_approval_wait() {
        let guard = SmartApprovalGuard::new();
        let heartbeat_sessions = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let hb_sessions = heartbeat_sessions.clone();

        let requester = DiscordApprovalRequester::new(guard.clone(), Duration::from_millis(300))
            .with_heartbeat_interval(Duration::from_millis(20))
            .with_heartbeat(Arc::new(move |session: &SessionKey| {
                hb_sessions.lock().push(session.clone());
            }));
        requester.set_dispatcher(Arc::new(MockDispatcher)).await;

        let session = SessionKey::new("discord", None::<String>, "chan1", None::<String>, "user1");

        let err = requester
            .request_approval(&session, "rm -rf /tmp/danger", "danger")
            .await
            .unwrap_err();
        assert_eq!(err, ApprovalError::Timeout);

        let sessions = heartbeat_sessions.lock().clone();
        assert!(
            !sessions.is_empty(),
            "expected heartbeat callback to be called"
        );
        assert_eq!(sessions[0], session);
    }

    #[tokio::test]
    async fn test_permanent_allowlist_persistence_roundtrip_and_auto_approval() {
        let db = crate::Database::connect("sqlite::memory:").await.unwrap();

        // 1. Initial guard with DB pool - approve a pattern as Always
        let guard_1 = SmartApprovalGuard::new().with_pool(db.pool().clone());
        let cmd = "rm -rf /tmp/build_cache";
        let pattern_key = crate::security::derive_pattern_key(cmd);

        let session = SessionKey::new("discord", None::<String>, "chan1", None::<String>, "user1");

        assert!(!guard_1.is_approved(&session, &pattern_key).await);

        guard_1.approve_always(&pattern_key).await;
        assert!(guard_1.is_approved(&session, &pattern_key).await);

        // 2. Verify row exists in DB
        let db_rows: Vec<(String,)> = sqlx::query_as("SELECT pattern_key FROM approval_allowlist")
            .fetch_all(db.pool())
            .await
            .unwrap();
        assert_eq!(db_rows.len(), 1);
        assert_eq!(db_rows[0].0, pattern_key);

        // 3. Simulate process restart: create fresh guard with the same pool and load allowlist
        let guard_2 = SmartApprovalGuard::new().with_pool(db.pool().clone());
        assert!(!guard_2.is_approved(&session, &pattern_key).await);

        let loaded = guard_2.load_persisted_allowlist().await.unwrap();
        assert_eq!(loaded, 1);
        assert!(guard_2.is_approved(&session, &pattern_key).await);

        // 4. Requester using restarted guard immediately auto-approves without prompting
        let requester = DiscordApprovalRequester::new(guard_2, Duration::from_millis(50));
        requester.set_dispatcher(Arc::new(MockDispatcher)).await;

        let decision = requester
            .request_approval(&session, cmd, "recursive delete")
            .await
            .unwrap();
        assert_eq!(decision, ApprovalDecision::Session);
    }
}
