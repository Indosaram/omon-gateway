use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::{FromRow, SqlitePool};

use super::Tool;
use crate::{check_gateway_lifecycle, next_run, scan_cron_prompt, OmonError};

#[derive(Clone, Debug, FromRow, Serialize, Deserialize)]
pub struct DbCronJob {
    pub id: String,
    pub session_key: Option<String>,
    pub expression: String,
    pub payload_json: String,
    pub enabled: bool,
    pub next_run_at: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Clone)]
pub struct CronTool {
    pool: SqlitePool,
}

impl CronTool {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl Tool for CronTool {
    fn name(&self) -> &str {
        "cron"
    }

    fn description(&self) -> &str {
        "Manage Omon-native scheduled workflows. Jobs may run an agent prompt, a script, or both. \
        Hermes-owned jobs are synchronized read-only from their profile stores."
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "action": {
                    "type": "string",
                    "enum": ["list", "get", "add", "delete"],
                    "description": "The cron operation to perform (list, get, add, delete)."
                },
                "id": {
                    "type": "string",
                    "description": "Job ID (required for get/add/delete)."
                },
                "expression": {
                    "type": "string",
                    "description": "Cron expression (e.g. '0 */2 * * *' or '0 10 * * *') or interval ('@every 5m'). Required for add."
                },
                "prompt": {"type": "string", "description": "Self-contained agent task."},
                "script": {"type": "string", "description": "Script or shell command to execute."},
                "deliver": {"type": "string", "description": "Delivery target such as discord:123."},
                "enabled_toolsets": {"type": "array", "items": {"type": "string"}},
                "description": {
                    "type": "string",
                    "description": "Human-readable description of the cron job."
                }
            },
            "required": ["action"]
        })
    }

    async fn execute(&self, args: Value) -> Result<Value, OmonError> {
        let action = args
            .get("action")
            .and_then(Value::as_str)
            .ok_or_else(|| OmonError::ToolExecution("missing 'action'".into()))?;

        match action {
            "list" => {
                let jobs: Vec<DbCronJob> = sqlx::query_as(
                    "SELECT id, session_key, expression, payload_json, enabled, next_run_at, created_at, updated_at FROM cron_jobs ORDER BY id",
                )
                .fetch_all(&self.pool)
                .await
                .map_err(|e| OmonError::Database(e.to_string()))?;

                Ok(json!({
                    "count": jobs.len(),
                    "cron_jobs": jobs
                }))
            }
            "get" => {
                let id = args
                    .get("id")
                    .and_then(Value::as_str)
                    .ok_or_else(|| OmonError::ToolExecution("missing 'id'".into()))?;

                let job: Option<DbCronJob> = sqlx::query_as(
                    "SELECT id, session_key, expression, payload_json, enabled, next_run_at, created_at, updated_at FROM cron_jobs WHERE id = ?",
                )
                .bind(id)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| OmonError::Database(e.to_string()))?;

                match job {
                    Some(j) => Ok(json!(j)),
                    None => Err(OmonError::ToolExecution(format!(
                        "cron job not found: {id}"
                    ))),
                }
            }
            "delete" => {
                let id = args
                    .get("id")
                    .and_then(Value::as_str)
                    .ok_or_else(|| OmonError::ToolExecution("missing 'id'".into()))?;

                let res = sqlx::query("DELETE FROM cron_jobs WHERE id = ?")
                    .bind(id)
                    .execute(&self.pool)
                    .await
                    .map_err(|e| OmonError::Database(e.to_string()))?;

                Ok(json!({
                    "deleted": res.rows_affected() > 0,
                    "id": id
                }))
            }
            "add" => {
                let id = args
                    .get("id")
                    .and_then(Value::as_str)
                    .ok_or_else(|| OmonError::ToolExecution("missing 'id'".into()))?;
                let expression = args
                    .get("expression")
                    .and_then(Value::as_str)
                    .ok_or_else(|| OmonError::ToolExecution("missing 'expression'".into()))?;
                let prompt = args.get("prompt").and_then(Value::as_str);
                let script = args.get("script").and_then(Value::as_str);
                if prompt.is_none() && script.is_none() {
                    return Err(OmonError::ToolExecution(
                        "add requires at least one of 'prompt' or 'script'".into(),
                    ));
                }
                if let Some(p) = prompt {
                    check_gateway_lifecycle(p).map_err(OmonError::ToolExecution)?;
                    let threats = scan_cron_prompt(p);
                    if !threats.is_empty() {
                        return Err(OmonError::ToolExecution(format!(
                            "cron prompt rejected due to security threats: {}",
                            threats.join(", ")
                        )));
                    }
                }
                if let Some(s) = script {
                    check_gateway_lifecycle(s).map_err(OmonError::ToolExecution)?;
                }
                let desc = args
                    .get("description")
                    .and_then(Value::as_str)
                    .unwrap_or("");
                let payload = json!({
                    "name": desc,
                    "prompt": prompt.unwrap_or_default(),
                    "script": script,
                    "deliver": args.get("deliver").and_then(Value::as_str).unwrap_or("local"),
                    "enabled_toolsets": args.get("enabled_toolsets").cloned().unwrap_or(Value::Null)
                });
                let payload_json = serde_json::to_string(&payload)
                    .map_err(|error| OmonError::ToolExecution(error.to_string()))?;
                let now = chrono::Utc::now();
                let next_run_at = next_run(expression, now)?;

                sqlx::query(
                    "INSERT INTO cron_jobs (id, expression, payload_json, enabled, next_run_at, created_at, updated_at)
                     VALUES (?, ?, ?, 1, ?, ?, ?)
                     ON CONFLICT(id) DO UPDATE SET expression=excluded.expression, payload_json=excluded.payload_json,
                     enabled=1, next_run_at=excluded.next_run_at, updated_at=excluded.updated_at",
                )
                .bind(id)
                .bind(expression)
                .bind(payload_json)
                .bind(next_run_at)
                .bind(now)
                .bind(now)
                .execute(&self.pool)
                .await
                .map_err(|e| OmonError::Database(e.to_string()))?;

                Ok(json!({
                    "status": "registered",
                    "id": id,
                    "expression": expression,
                    "prompt": prompt,
                    "script": script
                }))
            }
            _ => Err(OmonError::ToolExecution(format!(
                "unknown action: {action}"
            ))),
        }
    }
}
