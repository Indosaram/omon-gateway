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

pub fn thread_start_request(system_prompt: Option<&str>, model: Option<&str>) -> Message {
    let mut params = json!({});
    if let Some(prompt) = system_prompt {
        params["developerInstructions"] = json!(prompt);
    }
    if let Some(m) = model {
        params["model"] = json!(m);
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
