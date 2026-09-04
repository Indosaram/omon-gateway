//! Hermes-style activity line formatting for omo app-server items.
//!
//! Mirrors the legacy Hermes display language (see
//! ~/.hermes/hermes-agent/agent/display.py): per-tool emoji, present-tense
//! verbs, primary-arg previews with truncation, and "for"-connector phrasing.

#![allow(dead_code)]

use serde_json::Value;

const PREVIEW_CAP: usize = 60;
pub const REASONING_CAP: usize = 240;
pub const OUTPUT_CAP: usize = 200;

fn truncate(s: &str, cap: usize) -> String {
    let s = s.trim();
    if s.chars().count() <= cap {
        s.to_string()
    } else {
        let cut: String = s.chars().take(cap).collect();
        format!("{cut}…")
    }
}

fn collapse(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Format one Hermes-style activity line from an omo `item/started` or
/// `item/completed` payload's `item` object. Returns None for message items
/// and plain reasoning items (reasoning is rendered as a blockquote).
pub fn format_activity_line(item: &Value) -> Option<String> {
    let kind = item.get("type").and_then(Value::as_str)?;
    match kind {
        "commandExecution" => {
            let cmd = item.get("command").and_then(Value::as_str)?;
            Some(format!("⚙️ Running `{}`…", truncate(cmd, PREVIEW_CAP)))
        }
        "webSearch" => {
            let query = item
                .get("query")
                .and_then(Value::as_str)
                .or_else(|| item.pointer("/arguments/query").and_then(Value::as_str))?;
            Some(format!(
                "🔍 Searching the web for {}…",
                truncate(query, PREVIEW_CAP)
            ))
        }
        "fileChange" => {
            let path = item
                .get("path")
                .and_then(Value::as_str)
                .or_else(|| item.get("file").and_then(Value::as_str))?;
            Some(format!("📝 Writing {}…", truncate(path, PREVIEW_CAP)))
        }
        "mcpToolCall" => {
            let tool = item
                .get("tool")
                .and_then(Value::as_str)
                .or_else(|| item.get("name").and_then(Value::as_str))?;
            Some(format!("🔧 Calling {}…", truncate(tool, PREVIEW_CAP)))
        }
        "agentMessage" | "userMessage" | "reasoning" => None,
        other => Some(format!("⚡ {other}…")),
    }
}

pub fn reasoning_blockquote(text: &str, cap: usize) -> String {
    format!("> 💭 {}…", truncate(&collapse(text), cap))
}

pub fn command_output_excerpt(output: &str, cap: usize) -> String {
    truncate(&collapse(output), cap)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_command_execution_line() {
        let item = json!({
            "type": "commandExecution",
            "command": "echo OMOITEMPROBE-OK",
            "cwd": "/tmp",
        });
        assert_eq!(
            format_activity_line(&item).unwrap(),
            "⚙️ Running `echo OMOITEMPROBE-OK`…"
        );
    }

    #[test]
    fn test_command_without_command_field_is_none() {
        let item = json!({ "type": "commandExecution" });
        assert!(format_activity_line(&item).is_none());
    }

    #[test]
    fn test_long_command_is_truncated() {
        let long =
            "echo aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let item = json!({ "type": "commandExecution", "command": long });
        let line = format_activity_line(&item).unwrap();
        assert!(line.starts_with("⚙️ Running `echo aaa"));
        assert!(line.ends_with("…"));
        assert!(line.chars().count() < 90);
    }

    #[test]
    fn test_web_search_uses_for_connector() {
        let item = json!({ "type": "webSearch", "query": "rust tokio timeout" });
        assert_eq!(
            format_activity_line(&item).unwrap(),
            "🔍 Searching the web for rust tokio timeout…"
        );
    }

    #[test]
    fn test_file_change_line() {
        let item = json!({ "type": "fileChange", "path": "src/foo.rs" });
        assert_eq!(
            format_activity_line(&item).unwrap(),
            "📝 Writing src/foo.rs…"
        );
    }

    #[test]
    fn test_mcp_tool_call_line() {
        let item = json!({ "type": "mcpToolCall", "tool": "search_docs" });
        assert_eq!(
            format_activity_line(&item).unwrap(),
            "🔧 Calling search_docs…"
        );
    }

    #[test]
    fn test_message_and_reasoning_items_are_none() {
        assert!(format_activity_line(&json!({"type":"agentMessage"})).is_none());
        assert!(format_activity_line(&json!({"type":"userMessage"})).is_none());
        assert!(format_activity_line(&json!({"type":"reasoning","text":"hmm"})).is_none());
    }

    #[test]
    fn test_unknown_type_falls_back_to_bolt() {
        assert_eq!(
            format_activity_line(&json!({"type":"todoList"})).unwrap(),
            "⚡ todoList…"
        );
    }

    #[test]
    fn test_reasoning_blockquote_truncates_and_collapses() {
        let text = "first\n\nsecond\ttabbed   spaced";
        assert_eq!(
            reasoning_blockquote(text, REASONING_CAP),
            "> 💭 first second tabbed spaced…"
        );
        let long = "x".repeat(400);
        let line = reasoning_blockquote(&long, REASONING_CAP);
        assert!(line.starts_with("> 💭 xxxx"));
        assert!(line.ends_with("…"));
        assert!(line.chars().count() < REASONING_CAP + 12);
    }

    #[test]
    fn test_command_output_excerpt_collapses_and_truncates() {
        assert_eq!(
            command_output_excerpt("line1\n\nline2\t stuff", OUTPUT_CAP),
            "line1 line2 stuff"
        );
        let long = "y".repeat(500);
        let out = command_output_excerpt(&long, OUTPUT_CAP);
        assert!(out.ends_with("…"));
        assert!(out.chars().count() < OUTPUT_CAP + 4);
    }
}
