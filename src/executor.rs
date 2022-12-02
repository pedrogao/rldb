use std::fmt::Write;

use crate::parser::{Expr, SelectItem, SetExpr, Statement, Value};

pub fn execute(stmt: &Statement) -> Result<String, ExecuteError> {
    match stmt {
        Statement::Query(query) => match query.body.as_ref() {
            SetExpr::Select(select) => {
                let mut output = String::new();
                for item in &select.projection {
                    write!(output, " ").unwrap();
                    match item {
                        SelectItem::UnnamedExpr(Expr::Value(v)) => match v {
                            Value::SingleQuotedString(s) => write!(output, "{}", s).unwrap(),
                            Value::Number(s, _) => write!(output, "{}", s).unwrap(),
                            _ => todo!("not supported statement: {:#?}", stmt),
                        },
                        _ => todo!("not supported statement: {:#?}", stmt),
                    }
                }
                return Ok(output.trim().to_string());
            }
            _ => todo!("not supported statement: {:#?}", stmt),
        },
        _ => todo!("not supported statement: {:#?}", stmt),
    }
}

#[derive(thiserror::Error, Debug)]
pub enum ExecuteError {}
