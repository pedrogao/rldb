use super::*;

pub struct DummyExecutor;

impl Executor for DummyExecutor {
    fn execute(&mut self) -> Result<DataChunk, ExecuteError> {
        Ok(DataChunk::single(0))
    }
}
