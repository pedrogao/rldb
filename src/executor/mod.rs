use futures::stream::{BoxStream, StreamExt};
use futures_async_stream::try_stream;

use crate::array::DataChunk;
use crate::catalog::CatalogRef;
use crate::physical_planner::PhysicalPlan;
use crate::storage::{StorageError, StorageRef};

mod create;
mod dummy;
mod evaluator;
mod explain;
mod insert;
mod projection;
mod seq_scan;
mod values;

use self::create::*;
use self::dummy::*;
use self::explain::*;
use self::insert::*;
use self::projection::*;
use self::seq_scan::*;
use self::values::*;

const PROCESSING_WINDOW_SIZE: usize = 1024;

#[derive(thiserror::Error, Debug)]
pub enum ExecuteError {
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),
}

pub trait Executor {
    fn execute(&mut self) -> Result<DataChunk, ExecuteError>;
}

pub type BoxedExecutor = BoxStream<'static, Result<DataChunk, ExecuteError>>;

pub struct ExecutorBuilder {
    catalog: CatalogRef,
    storage: StorageRef,

    handle: Option<tokio::runtime::Handle>,
}

impl ExecutorBuilder {
    pub fn new(
        catalog: CatalogRef,
        storage: StorageRef,
        handle: Option<tokio::runtime::Handle>,
    ) -> ExecutorBuilder {
        ExecutorBuilder {
            catalog,
            storage,
            handle,
        }
    }

    pub fn build(&self, plan: PhysicalPlan) -> BoxedExecutor {
        use PhysicalPlan::*;

        let mut executor: BoxedExecutor = match plan {
            PhysicalCreateTable(plan) => CreateTableExecutor {
                plan,
                catalog: self.catalog.clone(),
                storage: self.storage.clone(),
            }
            .execute(),
            PhysicalInsert(plan) => InsertExecutor {
                table_ref_id: plan.table_ref_id,
                column_ids: plan.column_ids,
                catalog: self.catalog.clone(),
                storage: self.storage.clone(),
                child: self.build(*plan.child),
            }
            .execute(),
            PhysicalValues(plan) => ValuesExecutor {
                column_types: plan.column_types,
                values: plan.values,
            }
            .execute(),
            PhysicalExplain(plan) => ExplainExecutor { plan: plan.child }.execute(),
            PhysicalDummy(_) => DummyExecutor.execute(),
            PhysicalSeqScan(plan) => SeqScanExecutor {
                table_ref_id: plan.table_ref_id,
                column_ids: plan.column_ids,
                storage: self.storage.clone(),
            }
            .execute(),
            PhysicalProjection(plan) => ProjectionExecutor {
                exprs: plan.exprs,
                child: self.build(*plan.child),
            }
            .execute(),
        };

        if let Some(handle) = &self.handle {
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            handle.spawn(async move {
                while let Some(e) = executor.next().await {
                    tx.send(e).await.unwrap();
                }
            });
            tokio_stream::wrappers::ReceiverStream::new(rx).boxed()
        } else {
            executor
        }
    }
}
