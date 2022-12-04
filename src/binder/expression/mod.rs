use super::*;
use crate::parser::{Expr, Value};
use crate::types::{DataType, DataValue};

#[derive(Debug, PartialEq, Clone)]
pub enum BoundExpr {
    Constant(DataValue),
}

impl BoundExpr {
    pub fn return_type(&self) -> Option<DataType> {
        match self {
            Self::Constant(v) => v.datatype(),
        }
    }
}

impl Binder {
    pub fn bind_expr(&mut self, expr: &Expr) -> Result<BoundExpr, BindError> {
        match expr {
            Expr::Value(v) => Ok(BoundExpr::Constant(v.into())),
            _ => todo!("bind expression: {:?}", expr),
        }
    }
}

impl From<&Value> for DataValue {
    fn from(v: &Value) -> Self {
        match v {
            Value::Number(n, _) => {
                if let Ok(int) = n.parse::<i32>() {
                    Self::Int32(int)
                } else if let Ok(float) = n.parse::<f64>() {
                    Self::Float64(float)
                } else {
                    panic!("invalid digit: {}", n);
                }
            }
            Value::SingleQuotedString(s) => Self::String(s.clone()),
            Value::DoubleQuotedString(s) => Self::String(s.clone()),
            Value::Boolean(b) => Self::Bool(*b),
            Value::Null => Self::Null,
            _ => todo!("parse value: {:?}", v),
        }
    }
}
