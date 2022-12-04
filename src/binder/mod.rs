use std::vec::Vec;

use crate::catalog::*;
use crate::parser::{Ident, ObjectName, Statement};

mod statement;

pub use self::statement::*;

#[derive(Debug, PartialEq, Clone)]
pub enum BoundStatement {
    CreateTable(BoundCreateTable),
    Select(BoundSelect),
}

#[derive(thiserror::Error, Debug, PartialEq)]
pub enum BindError {
    #[error("table must have at least one column")]
    EmptyColumns,
    #[error("schema not found: {0}")]
    SchemaNotFound(String),
    #[error("table not found: {0}")]
    TableNotFound(String),
    #[error("duplicated table: {0}")]
    DuplicatedTable(String),
    #[error("duplicated column: {0}")]
    DuplicatedColumn(String),
    #[error("invalid table name: {0:?}")]
    InvalidTableName(Vec<Ident>),
}

pub struct Binder {
    catalog: CatalogRef,
}

impl Binder {
    pub fn new(catalog: CatalogRef) -> Self {
        Binder { catalog }
    }

    pub fn bind(&mut self, stmt: &Statement) -> Result<BoundStatement, BindError> {
        use Statement::*;

        match stmt {
            CreateTable { .. } => Ok(BoundStatement::CreateTable(self.bind_create_table(stmt)?)),
            Query(query) => Ok(BoundStatement::Select(self.bind_select(query)?)),
            _ => todo!("bind statement: {:#?}", stmt),
        }
    }
}

fn split_name(name: &ObjectName) -> Result<(&str, &str), BindError> {
    Ok(match name.0.as_slice() {
        [table] => (DEFAULT_SCHEMA_NAME, &table.value),
        [schema, table] => (&schema.value, &table.value),
        _ => return Err(BindError::InvalidTableName(name.0.clone())),
    })
}
