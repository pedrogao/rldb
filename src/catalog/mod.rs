use std::sync::Arc;

mod column;
mod database;
mod schema;
mod table;

pub use self::column::*;
pub use self::database::*;
pub use self::schema::*;
pub use self::table::*;

pub type CatalogRef = Arc<DatabaseCatalog>;

pub type SchemaId = u32;

pub type TableId = u32;

pub type ColumnId = u32;

pub const DEFAULT_SCHEMA_NAME: &str = "postgres";

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub struct TableRefId {
    pub schema_id: SchemaId,
    pub table_id: TableId,
}

impl TableRefId {
    pub const fn new(schema_id: SchemaId, table_id: TableId) -> Self {
        TableRefId {
            schema_id,
            table_id,
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Copy, Clone)]
pub struct ColumnRefId {
    pub schema_id: SchemaId,
    pub table_id: TableId,
    pub column_id: ColumnId,
}

impl ColumnRefId {
    pub const fn from_table(table: TableRefId, column_id: ColumnId) -> Self {
        ColumnRefId {
            schema_id: table.schema_id,
            table_id: table.table_id,
            column_id,
        }
    }

    pub const fn new(schema_id: SchemaId, table_id: TableId, column_id: ColumnId) -> Self {
        ColumnRefId {
            schema_id,
            table_id,
            column_id,
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum CatalogError {
    #[error("{0} not found: {1}")]
    NotFound(&'static str, String),
    #[error("duplicated {0}: {1}")]
    Duplicated(&'static str, String),
}
