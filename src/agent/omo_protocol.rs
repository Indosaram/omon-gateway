use crate::agent::agent_workspace::AgentWorkspace;
use serde_json::{json, Value};
use tokio_tungstenite::tungstenite::Message;

pub fn initialize_request() -> Message {
    Message::text(
        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "clientInfo": {
                    "name": "omon-gateway",
                    "title": "omon-gateway",
                    "version": env!("CARGO_PKG_VERSION")
                }
            }
        })
        .to_string(),
    )
}

pub fn thread_start_request(
    system_prompt: Option<&str>,
    model: Option<&str>,
    workspace: Option<&AgentWorkspace>,
) -> Message {
    let mut params = json!({});
    if let Some(prompt) = system_prompt {
        params["developerInstructions"] = json!(prompt);
    }
    if let Some(m) = model {
        params["model"] = json!(m);
    }
    if let Some(ws) = workspace {
        params["cwd"] = json!(&ws.cwd);
        params["runtimeWorkspaceRoots"] = json!(&ws.roots);
    }
    Message::text(
        json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "thread/start",
            "params": params
        })
        .to_string(),
    )
}

pub fn thread_resume_request(thread_id: &str) -> Message {
    Message::text(
        json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "thread/resume",
            "params": { "threadId": thread_id }
        })
        .to_string(),
    )
}

pub fn turn_start_request(thread_id: &str, user_prompt: &str, model: Option<&str>) -> Message {
    let mut params = json!({
        "threadId": thread_id,
        "input": [{
            "type": "text",
            "text": user_prompt
        }]
    });
    if let Some(m) = model {
        params["model"] = json!(m);
    }
    Message::text(
        json!({
            "jsonrpc": "2.0",
            "id": 3,
            "method": "turn/start",
            "params": params
        })
        .to_string(),
    )
}

pub fn approval_denial_response(req_id: &Value) -> Message {
    Message::text(
        json!({
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {
                "allow": false,
                "decision": "decline",
                "reason": "denied by gateway policy"
            }
        })
        .to_string(),
    )
}

pub fn is_approval_request(method: &str) -> bool {
    method.contains("Approval")
        || method.contains("requestApproval")
        || method.starts_with("item/") && method.ends_with("/request")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_thread_start_request_when_workspace_some() {
        // Given: system prompt, model, and an AgentWorkspace with cwd and roots
        let cwd = PathBuf::from("/var/omon/agents/bot-123");
        let shared = PathBuf::from("/var/omon/shared");
        let workspace = AgentWorkspace {
            cwd: cwd.clone(),
            roots: vec![cwd.clone(), shared.clone()],
        };

        // When: building thread start request with workspace Some
        let msg = thread_start_request(Some("instructions"), Some("model-x"), Some(&workspace));

        // Then: params contains cwd and runtimeWorkspaceRoots in order
        let val: Value = serde_json::from_str(msg.to_text().unwrap()).unwrap();
        assert_eq!(val["method"], "thread/start");
        assert_eq!(val["params"]["developerInstructions"], "instructions");
        assert_eq!(val["params"]["model"], "model-x");
        assert_eq!(val["params"]["cwd"], "/var/omon/agents/bot-123");
        assert_eq!(
            val["params"]["runtimeWorkspaceRoots"],
            json!(["/var/omon/agents/bot-123", "/var/omon/shared"])
        );
    }

    #[test]
    fn test_thread_start_request_when_workspace_none() {
        // Given: system prompt and model without workspace
        // When: building thread start request with workspace None
        let msg = thread_start_request(Some("instructions"), Some("model-x"), None);

        // Then: params has neither cwd nor runtimeWorkspaceRoots
        let val: Value = serde_json::from_str(msg.to_text().unwrap()).unwrap();
        assert_eq!(val["method"], "thread/start");
        assert_eq!(val["params"]["developerInstructions"], "instructions");
        assert_eq!(val["params"]["model"], "model-x");
        assert!(val["params"].get("cwd").is_none());
        assert!(val["params"].get("runtimeWorkspaceRoots").is_none());
    }
}
