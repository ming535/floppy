use crate::display::IndentVisitor;
use common::relation::RelationDescRef;
use plan::expr::ScalarExpr;
use std::fmt::{self, Display, Formatter};
use std::sync::Arc;

/// A LogicalPlan represents transforming an input relation (table) to
/// an output relation with a potentially different schema.
/// A plan represents a dataflow tree where data flows from leaves
/// up to the root to produce the query result.
/// LogicalPlan is defined as a recursive enum,
/// each node in the tree is a LogicalPlan.
#[derive(Clone)]
pub enum LogicalPlan {
    EmptyRelation(EmptyRelation),
    TableScan(TableScan),
    Projection(Projection),
    Filter(Filter),
}

impl LogicalPlan {
    /// Get a reference to the logical plan's schema
    pub fn relation_desc(&self) -> &RelationDescRef {
        match self {
            Self::Projection(Projection { rel, .. }) => rel,
            Self::TableScan(TableScan {
                projected_rel: rel, ..
            }) => rel,
            Self::Filter(Filter { input, .. }) => input.relation_desc(),
            Self::EmptyRelation(EmptyRelation { rel, .. }) => rel,
        }
    }

    /// Get a vector of reference to all relation descriptions in every node of the logical plan
    pub fn all_rel_descs(&self) -> Vec<&RelationDescRef> {
        match self {
            Self::TableScan(TableScan {
                projected_rel: rel, ..
            }) => vec![rel],
            Self::Projection(Projection { input, rel, .. }) => {
                let mut rel_descs = input.all_rel_descs();
                rel_descs.insert(0, schema);
                rel_descs
            }
            Self::Filter(Filter { input, .. }) => input.all_rel_descs(),
            Self::EmptyRelation(EmptyRelation { rel: schema, .. }) => vec![schema],
        }
    }
}

/// Trait that implements the [Visitor
/// pattern](https://en.wikipedia.org/wiki/Visitor_pattern) for a
/// depth first walk of `LogicalPlan` nodes. `pre_visit` is called
/// before any children are visited, and then `post_visit` is called
/// after all children have been visited.
pub trait PlanVisitor {
    /// The type of error returned by this visitor
    type Error;

    /// Invoked on a logical plan before any of its child inputs have been
    /// visited. If Ok(true) is returned, the recursion continues. If
    /// Err(..) or Ok(false) are returned, the recursion stops
    /// immediately and the error, if any, is returned to `accept`
    fn pre_visit(&mut self, plan: &LogicalPlan) -> Result<bool, Self::Error>;

    /// Invoked on a logical plan after all of its child inputs have
    /// been visited. The return value is handled the same as the
    /// return value of `pre_visit`. The provided default implementation
    /// returns `Ok(true)`.
    fn post_visit(&mut self, _plan: &LogicalPlan) -> Result<bool, Self::Error> {
        Ok(true)
    }
}

impl LogicalPlan {
    pub fn accept<V>(&self, visitor: &mut V) -> Result<bool, V::Error>
    where
        V: PlanVisitor,
    {
        if !visitor.pre_visit(self)? {
            return Ok(false);
        }

        let recurse = match self {
            LogicalPlan::Projection(Projection { input, .. }) => input.accept(visitor)?,
            LogicalPlan::Filter(Filter { input, .. }) => input.accept(visitor)?,
            // plans without inputs
            LogicalPlan::TableScan { .. } | LogicalPlan::EmptyRelation(_) => true,
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

// Various implementations for printing out LogicalPlan
impl LogicalPlan {
    /// Return a `format`able structure that produces a single line
    /// per node. For example:
    /// ```text
    /// Projection: #employee.id
    ///   Filter: #employee.state Eq Utf8(\"CO\")\
    ///     TableScan: employee
    /// ```
    pub fn display_indent(&self) -> impl fmt::Display + '_ {
        struct Wrapper<'a>(&'a LogicalPlan);
        impl<'a> fmt::Display for Wrapper<'a> {
            fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
                let mut visitor = IndentVisitor::new(f);
                self.0.accept(&mut visitor).unwrap();
                Ok(())
            }
        }
        Wrapper(self)
    }

    /// Return a `format`able structure with a human readable
    /// description of this LogicalPlan node, not including
    /// children. For example:
    ///
    /// ```text
    /// Projection: #id
    /// ```
    pub fn display(&self) -> impl fmt::Display + '_ {
        // Boilerplate structure to wrap LogicalPlan with something
        // that that can be formatted
        struct Wrapper<'a>(&'a LogicalPlan);
        impl<'a> fmt::Display for Wrapper<'a> {
            fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
                match self.0 {
                    LogicalPlan::TableScan(TableScan { ref table_name, .. }) => {
                        write!(f, "TableScan: {}", table_name)?;
                        Ok(())
                    }
                    LogicalPlan::Projection(Projection { ref expr, .. }) => {
                        write!(f, "Projection: ")?;
                        for (i, expr_item) in expr.iter().enumerate() {
                            if i > 0 {
                                write!(f, ", ")?;
                            }
                            write!(f, "{:?}", expr_item)?;
                        }
                        Ok(())
                    }
                    LogicalPlan::Filter(Filter { ref predicate, .. }) => {
                        write!(f, "Filter: {:?}", predicate)
                    }
                    LogicalPlan::EmptyRelation(_EmptyRelation_) => {
                        write!(f, "EmptyRelation")
                    }
                }
            }
        }
        Wrapper(self)
    }
}

impl fmt::Debug for LogicalPlan {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.display_indent().fmt(f)
    }
}

impl Display for LogicalPlan {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.display_indent().fmt(f)
    }
}

#[derive(Clone)]
pub struct Projection {
    /// The list of expressions
    pub expr: Vec<ScalarExpr>,
    /// The incoming logical plan
    pub input: Arc<LogicalPlan>,
    /// The relation description of the output
    pub rel: RelationDescRef,
}

#[derive(Clone)]
pub struct TableScan {
    /// The name of the table
    pub table_name: String,

    /// The schema description of the output
    pub projected_rel: RelationDescRef,

    /// Optional expressions to be used as filters
    pub filters: Vec<ScalarExpr>,
}

#[derive(Clone)]
pub struct Filter {
    pub predicate: ScalarExpr,
    pub input: Arc<LogicalPlan>,
}

#[derive(Clone)]
pub struct EmptyRelation {
    /// The schema description of the output
    pub rel: RelationDescRef,
}
