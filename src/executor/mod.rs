use crate::array::DataChunk;
use crate::binder::BoundStatement;
use crate::catalog::CatalogRef;
use crate::storage::{StorageError, StorageRef};

mod create;
mod insert;
mod select;
mod values;

#[derive(thiserror::Error, Debug)]
pub enum ExecuteError {
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),
}

pub struct Executor {
    catalog: CatalogRef,
    storage: StorageRef,
}

impl Executor {
    /// Create a new executor.
    pub fn new(catalog: CatalogRef, storage: StorageRef) -> Executor {
        Executor { catalog, storage }
    }

    /// Execute a bound statement.
    pub fn execute(&self, stmt: BoundStatement) -> Result<DataChunk, ExecuteError> {
        match stmt {
            BoundStatement::CreateTable(stmt) => self.execute_create_table(stmt),
            BoundStatement::Select(stmt) => self.execute_select(stmt),
            BoundStatement::Insert(stmt) => self.execute_insert(stmt),
        }
    }
}
