use super::*;
use crate::array::{ArrayImpl, DataChunk};
use crate::physical_planner::PhysicalPlan;

pub struct ExplainExecutor {
    pub plan: Box<PhysicalPlan>,
}

impl Executor for ExplainExecutor {
    fn execute(&mut self) -> Result<DataChunk, ExecuteError> {
        let explain_result = format!("{}", *self.plan);
        let chunk = DataChunk::from_iter([ArrayImpl::Utf8(
            [Some(explain_result)].into_iter().collect(),
        )]);
        Ok(chunk)
    }
}
