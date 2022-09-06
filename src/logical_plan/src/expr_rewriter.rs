use crate::plan::LogicalPlan;
use common::error::Result;
use common::schema::Schema;
use logical_expr::expr::LogicalExpr;
use std::sync::Arc;

/// Controls how the [ExprRewriter] recursion should proceed.
pub enum RewriteRecursion {
    /// Continue rewrite / visit this expression.
    Continue,
    /// Call [ExprRewriter::mutate()] immediately and return.
    Mutate,
    /// Do not rewrite / visit the children of this expression.
    Stop,
    /// Keep recursive but skip mutate on this expression
    Skip,
}

/// Trait for potentially recursively rewriting an [`Expr`] expression
/// tree. When passed to `Expr::rewrite`, `ExpressionVisitor::mutate` is
/// invoked recursively on all nodes of an expression tree. See the
/// comments on `Expr::rewrite` for details on its use
pub trait ExprRewriter<E: ExprRewritable = LogicalExpr>:
    Sized
{
    /// Invoked before any children of `expr` are rewritten /
    /// visited. Default implementation returns `Ok(RewriteRecursion::Continue)`
    fn pre_visit(
        &mut self,
        _expr: &E,
    ) -> Result<RewriteRecursion> {
        Ok(RewriteRecursion::Continue)
    }

    /// Invoked after all children of `expr` have been mutated and
    /// returns a potentially modified expr.
    fn mutate(&mut self, expr: E) -> Result<E>;
}

/// a trait for marking types that are rewritable by [ExprRewriter]
pub trait ExprRewritable: Sized {
    /// rewrite the expression tree using the given [ExprRewriter]
    fn rewrite<R: ExprRewriter<Self>>(
        self,
        rewriter: &mut R,
    ) -> Result<Self>;
}

impl ExprRewritable for LogicalExpr {
    /// Performs a depth first walk of an expression and its children
    /// to rewrite an expression, consuming `self` producing a new
    /// [`Expr`].
    ///
    /// Implements a modified version of the [visitor
    /// pattern](https://en.wikipedia.org/wiki/Visitor_pattern) to
    /// separate algorithms from the structure of the `Expr` tree and
    /// make it easier to write new, efficient expression
    /// transformation algorithms.
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
    /// mutatate(Column("foo"))
    /// pre_visit(Column("bar"))
    /// mutate(Column("bar"))
    /// mutate(BinaryExpr(GT))
    /// ```
    ///
    /// If an Err result is returned, recursion is stopped immediately
    ///
    /// If [`false`] is returned on a call to pre_visit, no
    /// children of that expression are visited, nor is mutate
    /// called on that expression
    ///
    fn rewrite<R>(self, rewriter: &mut R) -> Result<Self>
    where
        R: ExprRewriter<Self>,
    {
        let need_mutate = match rewriter.pre_visit(&self)? {
            RewriteRecursion::Mutate => {
                return rewriter.mutate(self)
            }
            RewriteRecursion::Stop => return Ok(self),
            RewriteRecursion::Continue => true,
            RewriteRecursion::Skip => false,
        };

        // recurse into all sub expressions(and cover all expression types)
        let expr = match self {
            LogicalExpr::Column(_) => self.clone(),
            LogicalExpr::Literal(value) => {
                LogicalExpr::Literal(value)
            }
            LogicalExpr::BinaryExpr { left, op, right } => {
                LogicalExpr::BinaryExpr {
                    left: rewrite_boxed(left, rewriter)?,
                    op,
                    right: rewrite_boxed(right, rewriter)?,
                }
            }
        };

        // now rewrite this expression itself
        if need_mutate {
            rewriter.mutate(expr)
        } else {
            Ok(expr)
        }
    }
}

#[allow(clippy::boxed_local)]
fn rewrite_boxed<R>(
    boxed_expr: Box<LogicalExpr>,
    rewriter: &mut R,
) -> Result<Box<LogicalExpr>>
where
    R: ExprRewriter,
{
    // TODO: It might be possible to avoid an allocation (the
    // Box::new) below by reusing the box.
    let expr: LogicalExpr = *boxed_expr;
    let rewritten_expr = expr.rewrite(rewriter)?;
    Ok(Box::new(rewritten_expr))
}

/// Recursively call [`Column::normalize_with_schemas`] on all Column expressions
/// in the `expr` expression tree.
pub fn normalize_col(
    expr: LogicalExpr,
    plan: &LogicalPlan,
) -> Result<LogicalExpr> {
    normalize_col_with_schemas(expr, &plan.all_schemas())
}

/// Recursively call [`Column::normalize_with_schemas`] on all Column expressions
/// in the `expr` expression tree.
pub fn normalize_col_with_schemas(
    expr: LogicalExpr,
    schemas: &[&Arc<Schema>],
) -> Result<LogicalExpr> {
    struct ColumnNormalizer<'a> {
        schemas: &'a [&'a Arc<Schema>],
    }

    impl<'a> ExprRewriter for ColumnNormalizer<'a> {
        fn mutate(
            &mut self,
            expr: LogicalExpr,
        ) -> Result<LogicalExpr> {
            if let LogicalExpr::Column(c) = expr {
                Ok(LogicalExpr::Column(
                    c.normalize_with_schemas(self.schemas)?,
                ))
            } else {
                Ok(expr)
            }
        }
    }

    expr.rewrite(&mut ColumnNormalizer { schemas })
}

/// Recursively normalize all Column expressions in a list of expression trees
pub fn normalize_cols(
    exprs: impl IntoIterator<Item = impl Into<LogicalExpr>>,
    plan: &LogicalPlan,
) -> Result<Vec<LogicalExpr>> {
    exprs
        .into_iter()
        .map(|e| normalize_col(e.into(), plan))
        .collect()
}
