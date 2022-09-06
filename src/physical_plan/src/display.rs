use crate::plan::{PhysicalPlan, PlanVisitor};
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
    fn pre_visit(
        &mut self,
        plan: &PhysicalPlan,
    ) -> Result<bool, fmt::Error> {
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
        _plan: &PhysicalPlan,
    ) -> Result<bool, fmt::Error> {
        self.indent -= 1;
        Ok(true)
    }
}
