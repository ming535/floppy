use crate::expr::LogicalExpr;
use common::error::Result;

/// Controls how the visitor recursion should proceed.
pub enum Recursion<V: ExpressionVisitor> {
    /// Attempt to visit all the children, recursively, of this expression.
    Continue(V),
    /// Do not visit the children of this expression, though the walk
    /// of parents of this expression will not be affected
    Stop(V),
}

/// Encode the traversal of an expression tree. When passed to
/// `Expr::accept`, `ExpressionVisitor::visit` is invoked
/// recursively on all nodes of an expression tree. See the comments
/// on `Expr::accept` for details on its use
pub trait ExpressionVisitor<E: ExprVisitable = LogicalExpr>: Sized {
    /// Invoked before any children of `expr` are visisted.
    fn pre_visit(self, expr: &E) -> Result<Recursion<Self>>
    where
        Self: ExpressionVisitor;

    /// Invoked after all children of `expr` are visited. Default
    /// implementation does nothing.
    fn post_visit(self, _expr: &E) -> Result<Self> {
        Ok(self)
    }
}

/// trait for types that can be visited by [`ExpressionVisitor`]
pub trait ExprVisitable: Sized {
    /// accept a visitor, calling `visit` on all children of this
    fn accept<V: ExpressionVisitor<Self>>(&self, visitor: V) -> Result<V>;
}

impl ExprVisitable for LogicalExpr {
    /// Performs a depth first walk of an expression and
    /// its children, calling [`ExpressionVisitor::pre_visit`] and
    /// `visitor.post_visit`.
    ///
    /// Implements the [visitor pattern](https://en.wikipedia.org/wiki/Visitor_pattern) to
    /// separate expression algorithms from the structure of the
    /// `Expr` tree and make it easier to add new types of expressions
    /// and algorithms that walk the tree.
    ///
    /// For an expression tree such as
    /// ```text
    /// BinaryExpr (GT)
    ///    left: Column("foo")
    ///    right: Column("bar")
    /// ```
    ///
    /// The nodes are visited using the following order
    /// ```text
    /// pre_visit(BinaryExpr(GT))
    /// pre_visit(Column("foo"))
    /// post_visit(Column("foo"))
    /// pre_visit(Column("bar"))
    /// post_visit(Column("bar"))
    /// post_visit(BinaryExpr(GT))
    /// ```
    ///
    /// If an Err result is returned, recursion is stopped immediately
    ///
    /// If `Recursion::Stop` is returned on a call to pre_visit, no
    /// children of that expression are visited, nor is post_visit
    /// called on that expression
    ///
    fn accept<V: ExpressionVisitor>(&self, visitor: V) -> Result<V> {
        let visitor = match visitor.pre_visit(self)? {
            Recursion::Continue(visitor) => visitor,
            // If the recursion should stop, do not visit children
            Recursion::Stop(visitor) => return Ok(visitor),
        };

        // recurse (and cover all expression types)
        let visitor = match self {
            LogicalExpr::Column(_) | LogicalExpr::Literal(_, _) => Ok(visitor),
            LogicalExpr::BinaryExpr { left, right, .. } => {
                let visitor = left.accept(visitor)?;
                right.accept(visitor)
            }
        }?;

        visitor.post_visit(self)
    }
}
