use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

use anyhow::anyhow;

use super::*;
use crate::array::DataChunk;
use crate::catalog::TableRefId;

pub type InMemoryTableRef = Arc<InMemoryTable>;

pub struct InMemoryStorage {
    tables: Mutex<HashMap<TableRefId, InMemoryTableRef>>,
}

impl Default for InMemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryStorage {
    pub fn new() -> Self {
        InMemoryStorage {
            tables: Mutex::new(HashMap::new()),
        }
    }

    pub fn add_table(&self, id: TableRefId) -> StorageResult<()> {
        let table = Arc::new(InMemoryTable::new(id));
        self.tables.lock().unwrap().insert(id, table);
        Ok(())
    }

    pub fn get_table(&self, id: TableRefId) -> StorageResult<InMemoryTableRef> {
        self.tables
            .lock()
            .unwrap()
            .get(&id)
            .cloned()
            .ok_or(anyhow!("table not found: {:?}", id).into())
    }
}

pub struct InMemoryTable {
    #[allow(dead_code)]
    id: TableRefId,
    inner: RwLock<InMemoryTableInner>,
}

#[derive(Default)]
struct InMemoryTableInner {
    chunks: Vec<DataChunk>,
}

impl InMemoryTable {
    fn new(id: TableRefId) -> Self {
        Self {
            id,
            inner: RwLock::new(InMemoryTableInner::default()),
        }
    }

    pub fn append(&self, chunk: DataChunk) -> StorageResult<()> {
        let mut inner = self.inner.write().unwrap();
        inner.chunks.push(chunk);
        Ok(())
    }

    pub fn all_chunks(&self) -> StorageResult<Vec<DataChunk>> {
        let inner = self.inner.read().unwrap();
        Ok(inner.chunks.clone())
    }
}
