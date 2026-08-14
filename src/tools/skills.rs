use async_trait::async_trait;
use serde_json::{json, Value};
use std::path::{Path, PathBuf};

use super::Tool;
use crate::OmonError;

#[derive(Clone)]
pub struct SkillsTool {
    skills_dirs: Vec<PathBuf>,
}

impl Default for SkillsTool {
    fn default() -> Self {
        let mut dirs = Vec::new();
        if let Ok(home) = std::env::var("HOME") {
            dirs.push(PathBuf::from(&home).join(".hermes").join("skills"));
            dirs.push(PathBuf::from(&home).join(".omon").join("skills"));
        }
        Self { skills_dirs: dirs }
    }
}

impl SkillsTool {
    pub fn new(skills_dirs: Vec<PathBuf>) -> Self {
        Self { skills_dirs }
    }

    fn find_all_skills(&self) -> Vec<(String, PathBuf)> {
        let mut results = Vec::new();
        for base in &self.skills_dirs {
            if !base.exists() {
                continue;
            }
            Self::scan_dir(base, &mut results);
        }
        results.sort_by(|a, b| a.0.cmp(&b.0));
        results.dedup_by(|a, b| a.0 == b.0);
        results
    }

    fn scan_dir(dir: &Path, acc: &mut Vec<(String, PathBuf)>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                let skill_file = path.join("SKILL.md");
                if skill_file.exists() {
                    let name = path
                        .file_name()
                        .unwrap_or_default()
                        .to_string_lossy()
                        .to_string();
                    acc.push((name, skill_file));
                } else {
                    Self::scan_dir(&path, acc);
                }
            }
        }
    }
}

#[async_trait]
impl Tool for SkillsTool {
    fn name(&self) -> &str {
        "skills"
    }

    fn description(&self) -> &str {
        "Discover, list, and read specialized capability skills. Actions: 'list' (shows all available skills), 'search' (find skill by keyword), 'read' (reads the complete SKILL.md guide for a specific skill)."
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "action": {
                    "type": "string",
                    "enum": ["list", "search", "read"],
                    "description": "Action to perform (list, search, read)."
                },
                "name": {
                    "type": "string",
                    "description": "Skill name to read (required for read action)."
                },
                "query": {
                    "type": "string",
                    "description": "Keyword to search (required for search action)."
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

        let all_skills = self.find_all_skills();

        match action {
            "list" => {
                let skill_names: Vec<String> = all_skills.iter().map(|(n, _)| n.clone()).collect();
                Ok(json!({
                    "total_skills": skill_names.len(),
                    "skills": skill_names
                }))
            }
            "search" => {
                let query = args
                    .get("query")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_lowercase();
                let matches: Vec<String> = all_skills
                    .iter()
                    .filter(|(n, _)| n.to_lowercase().contains(&query))
                    .map(|(n, _)| n.clone())
                    .collect();
                Ok(json!({
                    "query": query,
                    "count": matches.len(),
                    "matches": matches
                }))
            }
            "read" => {
                let name = args
                    .get("name")
                    .and_then(Value::as_str)
                    .ok_or_else(|| OmonError::ToolExecution("missing 'name'".into()))?;

                let found = all_skills.iter().find(|(n, _)| n == name);
                match found {
                    Some((_, path)) => {
                        let content = std::fs::read_to_string(path).map_err(|e| {
                            OmonError::ToolExecution(format!("failed to read skill {name}: {e}"))
                        })?;
                        Ok(json!({
                            "name": name,
                            "path": path.display().to_string(),
                            "content": content
                        }))
                    }
                    None => Err(OmonError::ToolExecution(format!(
                        "skill '{name}' not found. Use action='list' to see all skills."
                    ))),
                }
            }
            _ => Err(OmonError::ToolExecution(format!(
                "unknown skills action: {action}"
            ))),
        }
    }
}
