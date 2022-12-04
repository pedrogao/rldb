use super::*;
use crate::catalog::TableRefId;
use crate::physical_planner::PhysicalCreateTable;
use crate::storage::StorageRef;

pub struct CreateTableExecutor {
    pub plan: PhysicalCreateTable,
    pub catalog: CatalogRef,
    pub storage: StorageRef,
}

impl CreateTableExecutor {
    #[try_stream(boxed, ok = DataChunk, error = ExecuteError)]
    pub async fn execute(self) {
        let schema = self.catalog.get_schema(self.plan.schema_id).unwrap();
        let table_id = schema.add_table(&self.plan.table_name).unwrap();
        let table = schema.get_table(table_id).unwrap();
        for (name, desc) in &self.plan.columns {
            table.add_column(name, desc.clone()).unwrap();
        }
        self.storage
            .add_table(TableRefId::new(self.plan.schema_id, table_id))?;
        yield DataChunk::single(1);
    }
}
