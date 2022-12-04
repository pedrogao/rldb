use std::rc::Rc;

use enum_dispatch::enum_dispatch;

use crate::binder::BoundStatement;

mod create;
mod explain;
mod insert;
mod select;

pub use self::create::*;
pub use self::explain::*;
pub use self::insert::*;
pub use self::select::*;

#[enum_dispatch(Explain)]
#[derive(Debug, PartialEq, Clone)]
pub enum LogicalPlan {
    LogicalCreateTable,
    LogicalInsert,
    LogicalValues,
    LogicalExplain,
    LogicalDummy,
    LogicalGet,
    LogicalProjection,
}

pub type LogicalPlanRef = Rc<LogicalPlan>;

impl std::fmt::Display for LogicalPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.explain(0, f)
    }
}

#[derive(Default)]
pub struct LogicalPlanner;

#[derive(thiserror::Error, Debug, PartialEq)]
pub enum LogicalPlanError {}

impl LogicalPlanner {
    pub fn plan(&self, stmt: BoundStatement) -> Result<LogicalPlan, LogicalPlanError> {
        match stmt {
            BoundStatement::CreateTable(stmt) => self.plan_create_table(stmt),
            BoundStatement::Insert(stmt) => self.plan_insert(stmt),
            BoundStatement::Select(stmt) => self.plan_select(stmt),
            BoundStatement::Explain(stmt) => self.plan_explain(*stmt),
        }
    }
}

#[enum_dispatch]
pub trait Explain {
    fn explain_inner(&self, level: usize, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result;

    fn explain(&self, level: usize, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", "  ".repeat(level))?;
        self.explain_inner(level, f)
    }
}
