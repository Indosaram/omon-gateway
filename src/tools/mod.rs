mod browser;
mod cron;
mod file;
mod mcp;
mod skills;
mod terminal;
mod web;

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;

use crate::OmonError;

pub use browser::BrowserTool;
pub use cron::CronTool;
pub use file::FileTool;
pub use mcp::{McpClientTool, McpTool, McpTransport};
pub use skills::SkillsTool;
pub use terminal::TerminalTool;
pub use web::{WebFetchTool, WebSearchTool};

#[async_trait]
pub trait Tool: Send + Sync {
    fn name(&self) -> &str;
    fn description(&self) -> &str;
    fn input_schema(&self) -> Value;
    async fn execute(&self, args: Value) -> Result<Value, OmonError>;
}

#[derive(Clone, Default)]
pub struct ToolRegistry {
    tools: HashMap<String, Arc<dyn Tool>>,
}

impl ToolRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register<T: Tool + 'static>(&mut self, tool: T) -> Option<Arc<dyn Tool>> {
        self.tools.insert(tool.name().to_owned(), Arc::new(tool))
    }

    pub fn register_arc(&mut self, tool: Arc<dyn Tool>) -> Option<Arc<dyn Tool>> {
        self.tools.insert(tool.name().to_owned(), tool)
    }

    pub fn get(&self, name: &str) -> Option<Arc<dyn Tool>> {
        self.tools.get(name).cloned()
    }

    pub fn names(&self) -> Vec<String> {
        let mut names: Vec<_> = self.tools.keys().cloned().collect();
        names.sort();
        names
    }

    pub async fn execute(&self, name: &str, args: Value) -> Result<Value, OmonError> {
        let tool = self
            .get(name)
            .ok_or_else(|| OmonError::ToolExecution(format!("unknown tool: {name}")))?;
        tool.execute(args).await
    }
}
