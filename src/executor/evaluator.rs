use crate::array::*;
use crate::binder::BoundExpr;
use crate::executor::ExecuteError;
use crate::types::DataValue;

impl BoundExpr {
    pub fn eval_const(&self) -> Result<DataValue, ExecuteError> {
        match &self {
            Self::Constant(v) => Ok(v.clone()),
            Self::ColumnRef(_) => panic!("can not evaluate on ColumnRef"),
        }
    }

    pub fn eval_array(&self, chunk: &DataChunk) -> Result<ArrayImpl, ExecuteError> {
        match &self {
            Self::ColumnRef(v) => Ok(chunk.arrays()[v.column_ref_id.column_id as usize].clone()),
            Self::Constant(v) => {
                let mut builder = ArrayBuilderImpl::with_capacity(
                    chunk.cardinality(),
                    &self.return_type().unwrap(),
                );
                // TODO: optimize this
                for _ in 0..chunk.cardinality() {
                    builder.push(v);
                }
                Ok(builder.finish())
            }
        }
    }
}
