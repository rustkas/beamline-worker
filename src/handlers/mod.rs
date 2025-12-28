use crate::protocol::ExecStatus;
use serde_json::Value;

pub type HandlerResult = (ExecStatus, String, Option<Value>, Option<String>, Option<String>);

pub mod common;
pub mod http;
pub mod script;
pub mod sql;
pub mod fs;
pub mod human;
