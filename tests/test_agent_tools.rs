use std::time::Duration;

use omon_gateway::{
    ChatMessage, Database, FileTool, LlmClient, LlmConfig, LlmProvider, MemoryStore, SessionKey,
    TerminalTool, Tool, ToolDefinition,
};
use serde_json::json;

fn session(user: &str) -> SessionKey {
    SessionKey::new("test", None::<String>, "channel", None::<String>, user)
}

#[test]
fn builds_provider_specific_llm_payloads_and_parses_tool_calls() {
    let messages = vec![
        ChatMessage::new("system", "Be concise"),
        ChatMessage::new("user", "read notes"),
    ];
    let tools = vec![ToolDefinition {
        name: "file".into(),
        description: "read files".into(),
        input_schema: json!({"type": "object"}),
    }];

    let openai = LlmClient::new(LlmConfig::new(LlmProvider::OpenAi, "gpt-test")).unwrap();
    let payload = openai.build_payload(&messages, &tools);
    assert_eq!(payload["model"], "gpt-test");
    assert_eq!(payload["stream"], true);
    assert_eq!(payload["tools"][0]["function"]["name"], "file");

    let anthropic = LlmClient::new(LlmConfig::new(LlmProvider::Anthropic, "claude-test")).unwrap();
    let payload = anthropic.build_payload(&messages, &tools);
    assert_eq!(payload["system"], "Be concise");
    assert_eq!(payload["messages"][0]["role"], "user");
    assert_eq!(payload["tools"][0]["input_schema"]["type"], "object");

    let calls = openai
        .parse_tool_calls(&json!({"choices": [{"message": {"tool_calls": [{
            "id": "call-1", "function": {"name": "file", "arguments": "{\"path\":\"a.txt\"}"}
        }]}}]}))
        .unwrap();
    assert_eq!(calls[0].name, "file");
    assert_eq!(calls[0].arguments["path"], "a.txt");
}

#[tokio::test]
async fn terminal_executes_processes_and_captures_status() {
    let root = tempfile_dir("terminal");
    let tool = TerminalTool::new(&root).with_timeout(Duration::from_secs(2));
    let output = tool
        .execute(json!({"program": "/bin/sh", "args": ["-c", "printf hello; printf warning >&2; exit 3"]}))
        .await
        .unwrap();
    assert_eq!(output["stdout"], "hello");
    assert_eq!(output["stderr"], "warning");
    assert_eq!(output["exit_code"], 3);
    assert_eq!(output["success"], false);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn file_tool_reads_writes_lists_searches_and_blocks_traversal() {
    let root = tempfile_dir("file");
    let tool = FileTool::new(&root);
    tool.execute(
        json!({"operation": "write", "path": "docs/note.txt", "content": "alpha\nneedle here\n"}),
    )
    .await
    .unwrap();
    let read = tool
        .execute(json!({"operation": "read", "path": "docs/note.txt"}))
        .await
        .unwrap();
    assert_eq!(read["content"], "alpha\nneedle here\n");
    let list = tool
        .execute(json!({"operation": "list", "path": "docs"}))
        .await
        .unwrap();
    assert_eq!(list["entries"][0]["name"], "note.txt");
    let search = tool
        .execute(json!({"operation": "search", "query": "needle"}))
        .await
        .unwrap();
    assert_eq!(search["matches"][0]["line"], 2);
    assert!(tool
        .execute(json!({"operation": "read", "path": "../outside"}))
        .await
        .is_err());
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn memory_search_ranks_relevant_session_scoped_results() {
    let database = Database::connect("sqlite::memory:").await.unwrap();
    let store = MemoryStore::new(database.pool().clone());
    let first = session("first");
    let second = session("second");
    store
        .remember(
            &first,
            "Rust async process execution with Tokio",
            json!({"kind": "code"}),
        )
        .await
        .unwrap();
    store
        .remember(&first, "Buy apples and oranges", json!({}))
        .await
        .unwrap();
    store
        .remember(&second, "Tokio belongs to another session", json!({}))
        .await
        .unwrap();

    let results = store
        .search(&first, "tokio async execution", 5)
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    assert!(results[0].content.starts_with("Rust async"));
    assert_eq!(results[0].metadata["kind"], "code");
    assert!(results[0].score > 0.0);
}

fn tempfile_dir(label: &str) -> std::path::PathBuf {
    let path = std::env::temp_dir().join(format!("omon-{label}-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&path).unwrap();
    path
}
