use crate::logical_plan::plan::{LogicalPlan, PlanVisitor};
use std::fmt;

/// Formats plans with a single line per node.
pub struct IndentVisitor<'a, 'b> {
    f: &'a mut fmt::Formatter<'b>,
    /// The current indent
    indent: usize,
}

impl<'a, 'b> IndentVisitor<'a, 'b> {
    pub fn new(f: &'a mut fmt::Formatter<'b>) -> Self {
        Self { f, indent: 0 }
    }
}

impl<'a, 'b> PlanVisitor for IndentVisitor<'a, 'b> {
    type Error = fmt::Error;

    fn pre_visit(
        &mut self,
        plan: &LogicalPlan,
    ) -> Result<bool, Self::Error> {
        if self.indent > 0 {
            writeln!(self.f)?;
        }
        write!(
            self.f,
            "{:indent$}",
            "",
            indent = self.indent * 2
        )?;
        write!(self.f, "{}", plan.display())?;
        self.indent += 1;
        Ok(true)
    }

    fn post_visit(
        &mut self,
        _plan: &LogicalPlan,
    ) -> Result<bool, Self::Error> {
        self.indent -= 1;
        Ok(true)
    }
}
