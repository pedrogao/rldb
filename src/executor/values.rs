use itertools::Itertools;

use super::*;
use crate::array::{ArrayBuilderImpl, DataChunk};
use crate::binder::BoundExpr;
use crate::types::DataType;

pub struct ValuesExecutor {
    pub column_types: Vec<DataType>,
    pub values: Vec<Vec<BoundExpr>>,
}

impl ValuesExecutor {
    #[try_stream(boxed, ok = DataChunk, error = ExecuteError)]
    pub async fn execute(self) {
        for chunk in self.values.chunks(PROCESSING_WINDOW_SIZE) {
            let mut builders = self
                .column_types
                .iter()
                .map(|ty| ArrayBuilderImpl::with_capacity(chunk.len(), ty))
                .collect_vec();
            for row in chunk {
                for (expr, builder) in row.iter().zip(&mut builders) {
                    let value = expr.eval_const()?;
                    builder.push(&value);
                }
            }
            let chunk = builders
                .into_iter()
                .map(|builder| builder.finish())
                .collect::<DataChunk>();
            yield chunk;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::ArrayImpl;
    use crate::binder::BoundExpr;
    use crate::types::{DataTypeExt, DataTypeKind, DataValue};

    #[tokio::test]
    async fn values() {
        let values = [[0, 100], [1, 101], [2, 102], [3, 103]];
        let mut executor = ValuesExecutor {
            column_types: vec![DataTypeKind::Int(None).nullable(); 2],
            values: values
                .iter()
                .map(|row| {
                    row.iter()
                        .map(|&v| BoundExpr::Constant(DataValue::Int32(v)))
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>(),
        }
        .execute();
        let output = executor.next().await.unwrap().unwrap();
        let expected = [
            ArrayImpl::Int32((0..4).collect()),
            ArrayImpl::Int32((100..104).collect()),
        ]
        .into_iter()
        .collect::<DataChunk>();
        assert_eq!(output, expected);
    }
}
