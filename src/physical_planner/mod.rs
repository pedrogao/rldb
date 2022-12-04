use enum_dispatch::enum_dispatch;

use crate::logical_planner::{Explain, LogicalPlan};

mod create;
mod explain;
mod insert;

pub use self::create::*;
pub use self::explain::*;
pub use self::insert::*;

#[enum_dispatch(Explain)]
#[derive(Debug, PartialEq, Clone)]
pub enum PhysicalPlan {
    PhysicalCreateTable,
    PhysicalInsert,
    PhysicalValues,
    PhysicalExplain,
}

impl std::fmt::Display for PhysicalPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.explain(0, f)
    }
}

#[derive(Default)]
pub struct PhysicalPlanner;

#[derive(thiserror::Error, Debug, PartialEq)]
pub enum PhysicalPlanError {}

impl PhysicalPlanner {
    pub fn plan(&self, plan: &LogicalPlan) -> Result<PhysicalPlan, PhysicalPlanError> {
        use LogicalPlan::*;
        match plan {
            LogicalCreateTable(plan) => self.plan_create_table(plan),
            LogicalInsert(plan) => self.plan_insert(plan),
            LogicalValues(plan) => self.plan_values(plan),
            LogicalExplain(plan) => self.plan_explain(plan),
        }
    }
}
