use thiserror::Error;

#[derive(Debug, Error)]
pub enum OmonError {
    #[error("database error: {0}")]
    Database(String),

    #[error("multiplexer error: {0}")]
    Multiplexer(String),

    #[error("Discord error: {0}")]
    Discord(Box<serenity::Error>),

    #[error("LLM error: {0}")]
    Llm(String),

    #[error("tool execution error: {0}")]
    ToolExecution(String),

    #[error("configuration error: {0}")]
    Config(String),
}

impl From<serenity::Error> for OmonError {
    fn from(error: serenity::Error) -> Self {
        Self::Discord(Box::new(error))
    }
}

impl From<sqlx::Error> for OmonError {
    fn from(error: sqlx::Error) -> Self {
        Self::Database(error.to_string())
    }
}

impl From<sqlx::migrate::MigrateError> for OmonError {
    fn from(error: sqlx::migrate::MigrateError) -> Self {
        Self::Database(error.to_string())
    }
}

pub type Result<T, E = OmonError> = std::result::Result<T, E>;
