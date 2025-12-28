#[derive(Debug, Clone)]
pub enum WorkerError {
    Transient(String),
    Permanent(String),
}

impl WorkerError {
    pub fn transient<S: Into<String>>(msg: S) -> Self {
        Self::Transient(msg.into())
    }
    pub fn permanent<S: Into<String>>(msg: S) -> Self {
        Self::Permanent(msg.into())
    }
    pub fn is_transient(&self) -> bool {
        matches!(self, WorkerError::Transient(_))
    }
    pub fn message(&self) -> &str {
        match self {
            WorkerError::Transient(s) | WorkerError::Permanent(s) => s.as_str(),
        }
    }
}

pub fn classify_publish_error<E: std::fmt::Display>(e: &E) -> WorkerError {
    let s = e.to_string();
    if s.contains("connection") || s.contains("timeout") || s.contains("broken pipe") {
        WorkerError::transient(s)
    } else {
        WorkerError::permanent(s)
    }
}
