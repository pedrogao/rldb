use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{atomic::AtomicU32, Arc, RwLock},
};

use anyhow::anyhow;

use crate::{
    array::DataChunk,
    catalog::{ColumnDesc, TableRefId},
};

use super::{DiskRowset, RowsetBuilder, StorageResult};

pub type StorageTableRef = Arc<DiskTable>;

pub struct DiskStorage {
    tables: RwLock<HashMap<TableRefId, StorageTableRef>>,
    rowset_id_generator: Arc<AtomicU32>,
    options: Arc<StorageOptions>,
}

pub struct StorageOptions {
    pub base_path: PathBuf,
}

impl DiskStorage {
    pub fn new(options: StorageOptions) -> Self {
        DiskStorage {
            tables: RwLock::new(HashMap::new()),
            options: Arc::new(options),
            rowset_id_generator: Arc::new(AtomicU32::new(0)),
        }
    }

    pub fn add_table(&self, id: TableRefId, column_descs: &[ColumnDesc]) -> StorageResult<()> {
        let mut tables = self.tables.write().unwrap();
        let table = DiskTable {
            id,
            options: self.options.clone(),
            column_descs: column_descs.into(),
            rowsets: RwLock::new(Vec::new()),
            rowset_id_generator: self.rowset_id_generator.clone(),
        };
        let res = tables.insert(id, table.into());
        if res.is_some() {
            return Err(anyhow!("table already exists: {:?}", id).into());
        }
        Ok(())
    }

    pub fn get_table(&self, id: TableRefId) -> StorageResult<StorageTableRef> {
        let tables = self.tables.read().unwrap();
        tables
            .get(&id)
            .ok_or_else(|| anyhow!("table not found: {:?}", id).into())
            .cloned()
    }
}

pub struct DiskTable {
    id: TableRefId,
    column_descs: Arc<[ColumnDesc]>,
    options: Arc<StorageOptions>,
    rowset_id_generator: Arc<AtomicU32>,
    rowsets: RwLock<Vec<DiskRowset>>,
}

impl DiskTable {
    pub async fn write(self: &Arc<Self>) -> StorageResult<DiskTransaction> {
        let rowsets = self.rowsets.read().unwrap();
        Ok(DiskTransaction {
            read_only: false,
            table: self.clone(),
            rowset_snapshot: rowsets.clone(),
            builder: None,
            finished: false,
        })
    }

    pub async fn read(self: &Arc<Self>) -> StorageResult<DiskTransaction> {
        let rowsets = self.rowsets.read().unwrap();
        Ok(DiskTransaction {
            read_only: true,
            table: self.clone(),
            rowset_snapshot: rowsets.clone(),
            builder: None,
            finished: false,
        })
    }

    pub fn table_path(&self) -> PathBuf {
        self.options.base_path.join(self.id.table_id.to_string())
    }

    pub fn rowset_path_of(&self, rowset_id: u32) -> PathBuf {
        self.table_path().join(rowset_id.to_string())
    }
}

pub struct DiskTransaction {
    read_only: bool,
    table: Arc<DiskTable>,
    rowset_snapshot: Vec<DiskRowset>,
    builder: Option<RowsetBuilder>,
    finished: bool,
}

impl Drop for DiskTransaction {
    fn drop(&mut self) {
        if !self.finished {
            warn!("Transaction dropped without committing or aborting");
        }
    }
}

impl DiskTransaction {
    pub async fn append(&mut self, chunk: DataChunk) -> StorageResult<()> {
        if self.read_only {
            return Err(anyhow!("cannot append chunks in read only txn!").into());
        }
        if self.builder.is_none() {
            self.builder = Some(RowsetBuilder::new(self.table.column_descs.clone()));
        }
        let builder = self.builder.as_mut().unwrap();
        builder.append(chunk)?;

        Ok(())
    }

    pub async fn commit(mut self) -> StorageResult<()> {
        self.finished = true;

        if let Some(builder) = self.builder.take() {
            use std::sync::atomic::Ordering::SeqCst; // 强制有序
            let rowset_id = self.table.rowset_id_generator.fetch_add(1, SeqCst);
            let rowset_path = self
                .table
                .options
                .base_path
                .join(self.table.rowset_path_of(rowset_id));
            let rowset = builder.flush(rowset_id, rowset_path).await?;
            let mut rowsets = self.table.rowsets.write().unwrap();
            rowsets.push(rowset);
        }
        Ok(())
    }

    pub async fn all_chunks(&self) -> StorageResult<Vec<DataChunk>> {
        let mut chunks = vec![];
        for rowset in &self.rowset_snapshot {
            chunks.push(rowset.as_chunk().await?);
        }
        Ok(chunks)
    }
}
