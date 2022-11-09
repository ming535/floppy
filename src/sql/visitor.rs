use crate::sql::logical_plan::LogicalPlan;
use std::fmt;

/// Trait that implements the [Visitor
/// pattern](https://en.wikipedia.org/wiki/Visitor_pattern) for a
/// depth first walk of a tree of nodes. `pre_visit` is
/// called before any children are visited, and then
/// `post_visit` is called after all children have been
/// visited.
pub trait ExprVisitor<Node> {
    /// The type of error returned by this visitor.
    type Error;

    /// Invoked on a logical sql before any of its child
    /// inputs have been visited. If Ok(true) is
    /// returned, the recursion continues. If Err(..) or
    /// Ok(false) are returned, the recursion stops
    /// immediately and the error, if any, is returned to
    /// `accept`
    fn pre_visit(&mut self, node: &Node) -> Result<bool, Self::Error>;

    /// Invoked on a logical sql after all of its child
    /// inputs have been visited. The return value is
    /// handled the same as the return value of
    /// `pre_visit`. The provided default implementation
    /// returns `Ok(true)`.
    fn post_visit(&mut self, node: &Node) -> Result<bool, Self::Error> {
        Ok(true)
    }
}

/// Formats relation expressions with a single line per
/// node.
pub struct IndentVisitor<'a, 'b> {
    f: &'a mut fmt::Formatter<'b>,
    /// The current indent.
    indent: usize,
}

impl<'a, 'b> IndentVisitor<'a, 'b> {
    pub fn new(f: &'a mut fmt::Formatter<'b>) -> Self {
        Self { f, indent: 0 }
    }
}

impl<'a, 'b> ExprVisitor<LogicalPlan> for IndentVisitor<'a, 'b> {
    type Error = fmt::Error;

    fn pre_visit(&mut self, node: &LogicalPlan) -> Result<bool, Self::Error> {
        if self.indent > 0 {
            writeln!(self.f)?;
        }
        write!(self.f, "{:indent$}", "", indent = self.indent * 2)?;
        write!(self.f, "{}", node.display_node())?;
        self.indent += 1;
        Ok(true)
    }

    fn post_visit(&mut self, _: &LogicalPlan) -> Result<bool, Self::Error> {
        self.indent -= 1;
        Ok(true)
    }
}
