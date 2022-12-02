use crate::executor::{execute, ExecuteError};
use crate::parser::{parse, ParserError};

#[derive(Default)]
pub struct Database {}

impl Database {
    pub fn new() -> Self {
        Database {}
    }

    pub fn run_sql(&self, sql: &str) -> Result<Vec<String>, Error> {
        // 1. parse
        let stmts = parse(sql)?;

        let mut outputs = vec![];
        for stmt in stmts {
            debug!("execute: {:#?}", stmt);
            let output = execute(&stmt);
            outputs.extend(output);
        }
        Ok(outputs)
    }
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("parse error: {0}")]
    Parse(#[from] ParserError),
    #[error("execute error: {0}")]
    Execute(#[from] ExecuteError),
}
