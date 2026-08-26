use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::{FromRow, SqlitePool};

use super::Tool;
use crate::{check_gateway_lifecycle, next_run, scan_cron_prompt, CronScheduler, OmonError};

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
    scheduler: Option<Arc<CronScheduler>>,
}

impl CronTool {
    pub fn new(pool: SqlitePool) -> Self {
        Self {
            pool,
            scheduler: None,
        }
    }

    pub fn with_scheduler(pool: SqlitePool, scheduler: Arc<CronScheduler>) -> Self {
        Self {
            pool,
            scheduler: Some(scheduler),
        }
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
                    "enum": ["list", "get", "add", "create", "delete", "remove", "pause", "resume", "trigger", "run", "run_now", "update"],
                    "description": "The cron operation to perform (list, get, add, delete, pause, resume, trigger, update)."
                },
                "id": {
                    "type": "string",
                    "description": "Job ID (required for get/add/delete/pause/resume/trigger/update)."
                },
                "job_id": {
                    "type": "string",
                    "description": "Alternative alias for id."
                },
                "expression": {
                    "type": "string",
                    "description": "Cron expression (e.g. '0 */2 * * *' or '0 10 * * *') or interval ('@every 5m'). Required for add."
                },
                "schedule": {
                    "type": "string",
                    "description": "Alternative alias for expression."
                },
                "prompt": {"type": "string", "description": "Self-contained agent task."},
                "script": {"type": "string", "description": "Script or shell command to execute."},
                "deliver": {"type": "string", "description": "Delivery target such as discord:123."},
                "enabled": {"type": "boolean", "description": "Whether the cron job is active."},
                "enabled_toolsets": {"type": "array", "items": {"type": "string"}},
                "description": {
                    "type": "string",
                    "description": "Human-readable description of the cron job."
                },
                "name": {
                    "type": "string",
                    "description": "Alternative alias for description."
                }
            },
            "required": ["action"]
        })
    }

    async fn execute_with_context(
        &self,
        mut args: Value,
        session: Option<&crate::SessionKey>,
    ) -> Result<Value, OmonError> {
        // A Discord-created reminder must have an actual delivery target.
        // Previously the tool defaulted to `local`, so the scheduler completed
        // the job but had nowhere to send the notification.
        let is_create = matches!(
            args.get("action").and_then(Value::as_str),
            Some("add" | "create")
        );
        if is_create {
            if let Some(session) = session {
                if session.platform.eq_ignore_ascii_case("discord") {
                    if args.get("deliver").is_none()
                        || args.get("deliver").and_then(Value::as_str) == Some("local")
                    {
                        args["deliver"] = Value::String(format!("discord:{}", session.channel_id));
                    }
                    args["_session_key"] = Value::String(session.storage_key());
                }
            }
        }
        self.execute(args).await
    }

    async fn execute(&self, args: Value) -> Result<Value, OmonError> {
        let action = args
            .get("action")
            .and_then(Value::as_str)
            .ok_or_else(|| OmonError::ToolExecution("missing 'action'".into()))?;

        let id_param = args
            .get("id")
            .or_else(|| args.get("job_id"))
            .and_then(Value::as_str);

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
                let id = id_param.ok_or_else(|| OmonError::ToolExecution("missing 'id'".into()))?;

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
            "delete" | "remove" => {
                let id = id_param.ok_or_else(|| OmonError::ToolExecution("missing 'id'".into()))?;

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
            "pause" => {
                let id = id_param.ok_or_else(|| OmonError::ToolExecution("missing 'id'".into()))?;

                if let Some(scheduler) = &self.scheduler {
                    let paused = scheduler.pause(id).await?;
                    if !paused {
                        return Err(OmonError::ToolExecution(format!(
                            "cron job not found: {id}"
                        )));
                    }
                } else {
                    let res = sqlx::query(
                        "UPDATE cron_jobs SET enabled = 0, next_run_at = NULL, updated_at = ? WHERE id = ?",
                    )
                    .bind(chrono::Utc::now())
                    .bind(id)
                    .execute(&self.pool)
                    .await
                    .map_err(|e| OmonError::Database(e.to_string()))?;

                    if res.rows_affected() == 0 {
                        return Err(OmonError::ToolExecution(format!(
                            "cron job not found: {id}"
                        )));
                    }
                }

                Ok(json!({
                    "status": "paused",
                    "id": id,
                    "enabled": false
                }))
            }
            "resume" => {
                let id = id_param.ok_or_else(|| OmonError::ToolExecution("missing 'id'".into()))?;

                let next_run_str = if let Some(scheduler) = &self.scheduler {
                    let resumed = scheduler.resume(id).await?;
                    if !resumed {
                        return Err(OmonError::ToolExecution(format!(
                            "cron job not found: {id}"
                        )));
                    }
                    let job = scheduler.get(id).await?.ok_or_else(|| {
                        OmonError::ToolExecution(format!("cron job not found: {id}"))
                    })?;
                    job.next_run_at.map(|t| t.to_rfc3339())
                } else {
                    let job: Option<DbCronJob> = sqlx::query_as(
                        "SELECT id, session_key, expression, payload_json, enabled, next_run_at, created_at, updated_at FROM cron_jobs WHERE id = ?",
                    )
                    .bind(id)
                    .fetch_optional(&self.pool)
                    .await
                    .map_err(|e| OmonError::Database(e.to_string()))?;

                    let job = job.ok_or_else(|| {
                        OmonError::ToolExecution(format!("cron job not found: {id}"))
                    })?;

                    let now = chrono::Utc::now();
                    let next = next_run(&job.expression, now)?;
                    sqlx::query(
                        "UPDATE cron_jobs SET enabled = 1, next_run_at = ?, updated_at = ? WHERE id = ?",
                    )
                    .bind(next)
                    .bind(now)
                    .bind(id)
                    .execute(&self.pool)
                    .await
                    .map_err(|e| OmonError::Database(e.to_string()))?;

                    Some(next.to_rfc3339())
                };

                Ok(json!({
                    "status": "resumed",
                    "id": id,
                    "enabled": true,
                    "next_run_at": next_run_str
                }))
            }
            "trigger" | "run" | "run_now" => {
                let id = id_param.ok_or_else(|| OmonError::ToolExecution("missing 'id'".into()))?;

                let exists: Option<(String,)> =
                    sqlx::query_as("SELECT id FROM cron_jobs WHERE id = ?")
                        .bind(id)
                        .fetch_optional(&self.pool)
                        .await
                        .map_err(|e| OmonError::Database(e.to_string()))?;

                if exists.is_none() {
                    return Err(OmonError::ToolExecution(format!(
                        "cron job not found: {id}"
                    )));
                }

                if let Some(scheduler) = &self.scheduler {
                    let triggered = scheduler.trigger(id).await?;
                    if !triggered {
                        return Err(OmonError::ToolExecution(format!(
                            "failed to trigger cron job: {id}"
                        )));
                    }
                } else {
                    let now = chrono::Utc::now();
                    sqlx::query(
                        "UPDATE cron_jobs SET next_run_at = ?, updated_at = ? WHERE id = ?",
                    )
                    .bind(now)
                    .bind(now)
                    .bind(id)
                    .execute(&self.pool)
                    .await
                    .map_err(|e| OmonError::Database(e.to_string()))?;
                }

                Ok(json!({
                    "status": "triggered",
                    "id": id
                }))
            }
            "update" => {
                let id = id_param.ok_or_else(|| OmonError::ToolExecution("missing 'id'".into()))?;

                let job: Option<DbCronJob> = sqlx::query_as(
                    "SELECT id, session_key, expression, payload_json, enabled, next_run_at, created_at, updated_at FROM cron_jobs WHERE id = ?",
                )
                .bind(id)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| OmonError::Database(e.to_string()))?;

                let job = job
                    .ok_or_else(|| OmonError::ToolExecution(format!("cron job not found: {id}")))?;

                let mut payload: Value =
                    serde_json::from_str(&job.payload_json).unwrap_or_else(|_| json!({}));

                if let Some(p) = args.get("prompt").and_then(Value::as_str) {
                    check_gateway_lifecycle(p).map_err(OmonError::ToolExecution)?;
                    let threats = scan_cron_prompt(p);
                    if !threats.is_empty() {
                        return Err(OmonError::ToolExecution(format!(
                            "cron prompt rejected due to security threats: {}",
                            threats.join(", ")
                        )));
                    }
                    payload["prompt"] = Value::String(p.to_string());
                }

                if let Some(s) = args.get("script").and_then(Value::as_str) {
                    check_gateway_lifecycle(s).map_err(OmonError::ToolExecution)?;
                    payload["script"] = Value::String(s.to_string());
                }

                if let Some(desc) = args
                    .get("description")
                    .or_else(|| args.get("name"))
                    .and_then(Value::as_str)
                {
                    payload["name"] = Value::String(desc.to_string());
                }

                if let Some(deliver) = args.get("deliver").and_then(Value::as_str) {
                    payload["deliver"] = Value::String(deliver.to_string());
                }

                if let Some(toolsets) = args.get("enabled_toolsets") {
                    payload["enabled_toolsets"] = toolsets.clone();
                }

                let expression = args
                    .get("expression")
                    .or_else(|| args.get("schedule"))
                    .and_then(Value::as_str)
                    .unwrap_or(&job.expression);

                let enabled = args
                    .get("enabled")
                    .and_then(Value::as_bool)
                    .unwrap_or(job.enabled);

                let now = chrono::Utc::now();
                let next_run_at = if enabled {
                    Some(next_run(expression, now)?)
                } else {
                    None
                };

                let payload_json = serde_json::to_string(&payload)
                    .map_err(|error| OmonError::ToolExecution(error.to_string()))?;

                sqlx::query(
                    "UPDATE cron_jobs \
                     SET expression = ?, payload_json = ?, enabled = ?, next_run_at = ?, updated_at = ? \
                     WHERE id = ?",
                )
                .bind(expression)
                .bind(&payload_json)
                .bind(enabled)
                .bind(next_run_at)
                .bind(now)
                .bind(id)
                .execute(&self.pool)
                .await
                .map_err(|e| OmonError::Database(e.to_string()))?;

                Ok(json!({
                    "status": "updated",
                    "id": id,
                    "expression": expression,
                    "enabled": enabled,
                    "next_run_at": next_run_at
                }))
            }
            "add" | "create" => {
                let id = id_param.ok_or_else(|| OmonError::ToolExecution("missing 'id'".into()))?;
                let expression = args
                    .get("expression")
                    .or_else(|| args.get("schedule"))
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
                    .or_else(|| args.get("name"))
                    .and_then(Value::as_str)
                    .unwrap_or("");
                let payload = json!({
                    "name": desc,
                    "prompt": prompt.unwrap_or_default(),
                    "script": script,
                    "deliver": args.get("deliver").and_then(Value::as_str).unwrap_or("local"),
                    "enabled_toolsets": args.get("enabled_toolsets").cloned().unwrap_or(Value::Null)
                });
                let session_key = args
                    .get("_session_key")
                    .and_then(Value::as_str)
                    .map(str::to_owned);
                let payload_json = serde_json::to_string(&payload)
                    .map_err(|error| OmonError::ToolExecution(error.to_string()))?;
                let now = chrono::Utc::now();
                let next_run_at = next_run(expression, now)?;

                sqlx::query(
                    "INSERT INTO cron_jobs (id, session_key, expression, payload_json, enabled, next_run_at, created_at, updated_at)
                     VALUES (?, ?, ?, ?, 1, ?, ?, ?)
                     ON CONFLICT(id) DO UPDATE SET session_key=excluded.session_key, expression=excluded.expression, payload_json=excluded.payload_json,
                     enabled=1, next_run_at=excluded.next_run_at, updated_at=excluded.updated_at",
                )
                .bind(id)
                .bind(session_key)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Database;

    #[tokio::test]
    async fn test_cron_tool_lifecycle_actions() {
        let database = Database::connect("sqlite::memory:").await.unwrap();
        let tool = CronTool::new(database.pool().clone());

        // 1. Add job
        let add_res = tool
            .execute(json!({
                "action": "add",
                "id": "my_job",
                "expression": "interval:5m",
                "prompt": "Say hello",
                "description": "Daily greeting"
            }))
            .await
            .unwrap();
        assert_eq!(add_res["status"], "registered");
        assert_eq!(add_res["id"], "my_job");

        // 2. Get job
        let get_res = tool
            .execute(json!({
                "action": "get",
                "id": "my_job"
            }))
            .await
            .unwrap();
        assert_eq!(get_res["id"], "my_job");
        assert_eq!(get_res["enabled"], true);

        // 3. List jobs
        let list_res = tool
            .execute(json!({
                "action": "list"
            }))
            .await
            .unwrap();
        assert_eq!(list_res["count"], 1);

        // 4. Pause job
        let pause_res = tool
            .execute(json!({
                "action": "pause",
                "id": "my_job"
            }))
            .await
            .unwrap();
        assert_eq!(pause_res["status"], "paused");
        assert_eq!(pause_res["enabled"], false);

        let get_paused = tool
            .execute(json!({
                "action": "get",
                "id": "my_job"
            }))
            .await
            .unwrap();
        assert_eq!(get_paused["enabled"], false);
        assert!(get_paused["next_run_at"].is_null());

        // 5. Resume job
        let resume_res = tool
            .execute(json!({
                "action": "resume",
                "id": "my_job"
            }))
            .await
            .unwrap();
        assert_eq!(resume_res["status"], "resumed");
        assert_eq!(resume_res["enabled"], true);
        assert!(resume_res["next_run_at"].is_string());

        // 6. Update job
        let update_res = tool
            .execute(json!({
                "action": "update",
                "id": "my_job",
                "expression": "interval:10m",
                "prompt": "Updated greeting"
            }))
            .await
            .unwrap();
        assert_eq!(update_res["status"], "updated");
        assert_eq!(update_res["expression"], "interval:10m");

        let get_updated = tool
            .execute(json!({
                "action": "get",
                "id": "my_job"
            }))
            .await
            .unwrap();
        assert_eq!(get_updated["expression"], "interval:10m");
        let payload: Value =
            serde_json::from_str(get_updated["payload_json"].as_str().unwrap()).unwrap();
        assert_eq!(payload["prompt"], "Updated greeting");

        // 7. Trigger job
        let trigger_res = tool
            .execute(json!({
                "action": "trigger",
                "id": "my_job"
            }))
            .await
            .unwrap();
        assert_eq!(trigger_res["status"], "triggered");

        // 8. Delete job
        let delete_res = tool
            .execute(json!({
                "action": "delete",
                "id": "my_job"
            }))
            .await
            .unwrap();
        assert_eq!(delete_res["deleted"], true);

        // Verify gone
        assert!(tool
            .execute(json!({
                "action": "get",
                "id": "my_job"
            }))
            .await
            .is_err());
    }
}
