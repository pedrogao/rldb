use std::collections::HashMap;
use std::vec::Vec;

use crate::catalog::*;
use crate::parser::{Ident, ObjectName, Statement};

mod expression;
mod statement;
mod table_ref;

pub use self::expression::*;
pub use self::statement::*;
pub use self::table_ref::*;

#[derive(Debug, PartialEq, Clone)]
pub enum BoundStatement {
    CreateTable(BoundCreateTable),
    Select(BoundSelect),
    Insert(BoundInsert),
    Explain(Box<BoundStatement>),
}

#[derive(thiserror::Error, Debug, PartialEq)]
pub enum BindError {
    #[error("table must have at least one column")]
    EmptyColumns,
    #[error("schema not found: {0}")]
    SchemaNotFound(String),
    #[error("table not found: {0}")]
    TableNotFound(String),
    #[error("column not found: {0}")]
    ColumnNotFound(String),
    #[error("duplicated table: {0}")]
    DuplicatedTable(String),
    #[error("duplicated column: {0}")]
    DuplicatedColumn(String),
    #[error("invalid table name: {0:?}")]
    InvalidTableName(Vec<Ident>),
    #[error("duplicated alias: {0}")]
    DuplicatedAlias(String),
    #[error("ambiguous column name: {0}")]
    AmbiguousColumnName(String),
    #[error("not nullable column: {0}")]
    NotNullableColumn(String),
    #[error("tuple length mismatch: expected {expected} but got {actual}")]
    TupleLengthMismatch { expected: usize, actual: usize },
    #[error("value should not be null in column: {0}")]
    NullValueInColumn(String),
}

type TableName = String;

pub struct Binder {
    catalog: CatalogRef,
    tables: HashMap<TableName, TableRefId>,
}

impl Binder {
    pub fn new(catalog: CatalogRef) -> Self {
        Binder {
            catalog,
            tables: HashMap::default(),
        }
    }

    pub fn bind(&mut self, stmt: &Statement) -> Result<BoundStatement, BindError> {
        match stmt {
            Statement::CreateTable { .. } => {
                Ok(BoundStatement::CreateTable(self.bind_create_table(stmt)?))
            }
            Statement::Insert { .. } => Ok(BoundStatement::Insert(self.bind_insert(stmt)?)),
            Statement::Explain { statement, .. } => {
                Ok(BoundStatement::Explain(self.bind(&*statement)?.into()))
            }
            Statement::Query(query) => Ok(BoundStatement::Select(self.bind_select(&*query)?)),
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
