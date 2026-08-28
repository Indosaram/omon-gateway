use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use crate::models::{InboundEvent, SessionContext};
use crate::{OmonError, Result};

/// Agent backend abstraction for executing conversation turns.
///
/// # Trait Boundary Decision: Runner Boundary vs LlmClient Boundary
///
/// We define `AgentBackend` at the **Runner boundary** (`run`, `run_cancelable`, `cancel`)
/// rather than the lower-level `LlmClient` boundary (`stream_with_tool_calls`) for the following reasons:
///
/// 1. **Per-Turn Model Overrides**: The session context (`session.state.active_model`) determines
///    which model is used for a given turn. At the runner boundary, the backend has full access to
///    `SessionContext` to dynamically construct, select, or configure per-turn LLM clients or
///    route to specific backend workers.
///
/// 2. **Tool Definitions and Tool Filtering Flow**: Tool availability depends on session-level
///    configuration (`session.state.enabled_toolsets`). The runner boundary encapsulates the
///    filtering of `ToolRegistry` into `Vec<ToolDefinition>`, tool dispatch execution, output
///    truncation, status notifications to the dispatcher, and the multi-round tool execution loop.
///
/// 3. **Heterogeneous Backend Architectures**: Future backends (such as an external agent server
///    or appserver backend) execute their own tool loops and agent runtimes remotely. Placing the
///    trait at the runner boundary allows `SessionActor` to treat direct-LLM execution (`LlmBackend`)
///    and remote agent protocols uniformly without leaking LLM-specific details into the multiplexer.
///
/// 4. **Delivery Ledger & Lifecycle Integrity**: Cancellation (`cancel`), streaming token chunks
///    (`StreamChunk`), media directives, silence sentinels, and message transcript persistence
///    natively align with the turn lifecycle managed at the runner boundary.
#[async_trait]
pub trait AgentBackend: Send + Sync + 'static {
    /// Executes a single inbound event turn for a session.
    async fn run(&self, session: &mut SessionContext, event: InboundEvent) -> Result<()>;

    /// Executes a single inbound event turn with cooperative cancellation.
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

    /// Cancels any active or pending background work for the specified session.
    async fn cancel(&self, _session: &SessionContext) -> Result<()> {
        Ok(())
    }
}
