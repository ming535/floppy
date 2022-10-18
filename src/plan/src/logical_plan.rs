use crate::{Expr, ExprVisitor, IndentVisitor};
use catalog::names::FullObjectName;
use common::relation::{GlobalId, RelationDesc};
use std::fmt;
use std::fmt::Formatter;

pub mod log_planner;

/// A `LogicalPlan` computes a table. It is also called a logical plan.
/// It represents a graph of data flow where each node in the graph
/// computes a table from the input of the node.
/// The `LogicalPlan` is not ready to be executed yet.
#[derive(Debug)]
pub enum LogicalPlan {
    /// An empty relation exists in queries without a `From` clause, eg
    /// ```sql
    /// SELECT 1 + 1;
    /// ```
    Empty,
    /// Table is the leaf of the LogicalPlan tree.
    Table {
        table_id: GlobalId,
        /// The relation description of the output.
        rel_desc: RelationDesc,
        /// Partial table name.
        name: FullObjectName,
    },
    Projection {
        /// The list of expressions
        exprs: Vec<Expr>,
        /// The incoming LogicalPlan
        input: Box<LogicalPlan>,
        /// The relation description of the output
        rel_desc: RelationDesc,
    },
    Filter {
        input: Box<LogicalPlan>,
        predicate: Expr,
    },
}

impl LogicalPlan {
    pub fn rel_desc(&self) -> RelationDesc {
        match self {
            Self::Empty => RelationDesc::empty(),
            Self::Filter { input, .. } => input.rel_desc(),
            Self::Projection { rel_desc, .. } => rel_desc.clone(),
            Self::Table { rel_desc, .. } => rel_desc.clone(),
        }
    }
}

impl LogicalPlan {
    pub fn accept<V>(&self, visitor: &mut V) -> std::result::Result<bool, V::Error>
    where
        V: ExprVisitor<LogicalPlan>,
    {
        if !visitor.pre_visit(self)? {
            return Ok(false);
        }

        let recurse = match self {
            Self::Projection { input, .. } => input.accept(visitor)?,
            Self::Filter { input, .. } => input.accept(visitor)?,
            Self::Table { .. } | Self::Empty => true,
        };

        if !recurse {
            return Ok(false);
        }

        if !visitor.post_visit(self)? {
            return Ok(false);
        }

        Ok(true)
    }
}

impl LogicalPlan {
    /// Return a `format`able structure that produces a single line
    /// per node. For example:
    /// ```text
    /// Projection: #employee.id
    ///   Filter: #employee.state Eq Utf8(\"CO\")\
    ///     Table: employee
    /// ```
    pub fn display_tree(&self) -> impl fmt::Display + '_ {
        struct Wrapper<'a>(&'a LogicalPlan);
        impl<'a> fmt::Display for Wrapper<'a> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                let mut visitor = IndentVisitor::new(f);
                self.0.accept(&mut visitor).unwrap();
                Ok(())
            }
        }
        Wrapper(self)
    }

    /// display a single node in the tree.
    pub fn display_node(&self) -> impl fmt::Display + '_ {
        struct Wrapper<'a>(&'a LogicalPlan);
        impl<'a> fmt::Display for Wrapper<'a> {
            fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
                match self.0 {
                    LogicalPlan::Table { name, .. } => {
                        write!(f, "Table: {}", name.item)?;
                        Ok(())
                    }
                    LogicalPlan::Projection { exprs, .. } => {
                        write!(f, "Projection: ")?;
                        for (i, expr_item) in exprs.iter().enumerate() {
                            if i > 0 {
                                write!(f, ", ")?;
                            }
                            write!(f, "{}", expr_item,)?;
                        }
                        Ok(())
                    }
                    LogicalPlan::Filter { predicate, .. } => {
                        write!(f, "Filter: {}", predicate,)
                    }
                    LogicalPlan::Empty => write!(f, "EmptyTable"),
                }
            }
        }
        Wrapper(self)
    }
}

impl fmt::Display for LogicalPlan {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.display_tree().fmt(f)
    }
}
