use crate::common::error::{FloppyError, Result};
use crate::common::row::Row;
use crate::common::schema::{
    DataType, Field, Schema, SchemaRef,
};
use crate::common::value::Value;
use crate::physical_plan::display::IndentVisitor;
use crate::physical_plan::empty::EmptyExec;
use crate::physical_plan::heap_scan::HeapScanExec;
use crate::physical_plan::projection::ProjectionExec;
use crate::physical_plan::SendableTupleStream;
use futures::Stream;
use std::fmt::{self, Display, Formatter};
use std::io::Empty;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

pub enum PhysicalPlan {
    EmptyExec(EmptyExec),
    HeapScanExec(HeapScanExec),
    ProjectionExec(ProjectionExec),
    FilterExec(FilterExec),
}

impl PhysicalPlan {
    pub fn next(&mut self) -> Result<Option<Row>> {
        match self {
            Self::EmptyExec(p) => p.next(),
            Self::ProjectionExec(p) => p.next(),
            Self::HeapScanExec(p) => p.next(),
            _ => Err(FloppyError::NotImplemented(
                "physical expression not supported"
                    .to_owned(),
            )),
        }
    }
}

/// Trait that implements the [Visitor
/// pattern](https://en.wikipedia.org/wiki/Visitor_pattern) for a
/// depth first walk of `LogicalPlan` nodes. `pre_visit` is called
/// before any children are visited, and then `post_visit` is called
/// after all children have been visited.
pub trait PlanVisitor {
    /// Invoked on a logical plan before any of its child inputs have been
    /// visited. If Ok(true) is returned, the recursion continues. If
    /// Err(..) or Ok(false) are returned, the recursion stops
    /// immediately and the error, if any, is returned to `accept`
    fn pre_visit(
        &mut self,
        plan: &PhysicalPlan,
    ) -> Result<bool>;

    /// Invoked on a logical plan after all of its child inputs have
    /// been visited. The return value is handled the same as the
    /// return value of `pre_visit`. The provided default implementation
    /// returns `Ok(true)`.
    fn post_visit(
        &mut self,
        _plan: &PhysicalPlan,
    ) -> Result<bool> {
        Ok(true)
    }
}

impl PhysicalPlan {
    pub fn accept<V>(&self, visitor: &mut V) -> Result<bool>
    where
        V: PlanVisitor,
    {
        if !visitor.pre_visit(self)? {
            return Ok(false);
        }

        let recurse = match self {
            PhysicalPlan::ProjectionExec(
                ProjectionExec { input, .. },
            ) => input.accept(visitor)?,
            PhysicalPlan::FilterExec(FilterExec {}) => {
                // input.accept(visitor)?
                todo!()
            }
            // plans without inputs
            PhysicalPlan::HeapScanExec { .. }
            | PhysicalPlan::EmptyExec(_) => true,
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
impl PhysicalPlan {
    /// Return a `format`able structure that produces a single line
    /// per node. For example:
    /// ```text
    /// Projection: #employee.id
    ///   Filter: #employee.state Eq Utf8(\"CO\")\
    ///     TableScan: employee
    /// ```
    pub fn display_indent(&self) -> impl fmt::Display + '_ {
        struct Wrapper<'a>(&'a PhysicalPlan);
        impl<'a> fmt::Display for Wrapper<'a> {
            fn fmt(
                &self,
                f: &mut Formatter<'_>,
            ) -> fmt::Result {
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
        struct Wrapper<'a>(&'a PhysicalPlan);
        impl<'a> fmt::Display for Wrapper<'a> {
            fn fmt(
                &self,
                f: &mut Formatter<'_>,
            ) -> fmt::Result {
                match self.0 {
                    PhysicalPlan::HeapScanExec(
                        HeapScanExec { table_name, .. },
                    ) => {
                        write!(
                            f,
                            "HeapScanExec: {}",
                            table_name
                        )
                    }
                    PhysicalPlan::ProjectionExec(
                        ProjectionExec { ref expr, .. },
                    ) => {
                        write!(f, "ProjectionExec: ")?;
                        for (i, expr_item) in
                            expr.iter().enumerate()
                        {
                            if i > 0 {
                                write!(f, ", ")?;
                            }
                            write!(f, "{:?}", expr_item)?;
                        }
                        Ok(())
                    }
                    PhysicalPlan::FilterExec(
                        FilterExec {},
                    ) => {
                        // write!(f, "Filter: {:?}", predicate)
                        todo!()
                    }
                    PhysicalPlan::EmptyExec(
                        EmptyRelation_,
                    ) => {
                        write!(f, "EmptyExec")
                    }
                }
            }
        }
        Wrapper(self)
    }
}

impl fmt::Debug for PhysicalPlan {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.display_indent().fmt(f)
    }
}

impl Display for PhysicalPlan {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.display_indent().fmt(f)
    }
}

#[derive(Clone)]
pub struct FilterExec {}
