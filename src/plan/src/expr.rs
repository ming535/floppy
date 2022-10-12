use crate::context::ScalarExprContext;
use crate::func::{BinaryExpr, VariadicExpr};
use crate::visitor::{ExprVisitor, IndentVisitor};
use catalog::names::FullObjectName;
use common::error::{FloppyError, Result};
use common::relation::{ColumnRef, ColumnType, GlobalId, RelationDesc, RelationType};
use common::scalar::{Datum, ScalarType};
use rust_decimal::Decimal;
use std::fmt;
use std::fmt::Formatter;

/// A `ScalarExpr` computes a scalar.
///  https://www.postgresql.org/docs/current/sql-expressions.html
#[derive(Debug, Clone)]
pub enum ScalarExpr {
    /// A column reference.
    Column(ColumnRef),
    /// Positional parameter when prepare a SQL statement for execution:
    /// https://www.postgresql.org/docs/current/sql-prepare.html
    Parameter(usize),
    /// A constant value.
    Literal(Literal),
    /// A binary expression.
    CallBinary(BinaryExpr),
    /// An expression that have variable number of parameters.
    /// for example: 1 == 2 AND 2 == 3 OR 4 > 5
    CallVariadic(VariadicExpr),
}

impl ScalarExpr {
    pub fn typ(&self, ecx: &ScalarExprContext) -> ColumnType {
        match self {
            Self::Column(ColumnRef { id, .. }) => {
                ecx.rel_desc.rel_type().column_type(*id).clone()
            }
            Self::Parameter(n) => ecx.param_types().borrow()[n].clone().nullable(true),
            Self::Literal(Literal { datum, scalar_type }) => ColumnType {
                scalar_type: scalar_type.clone(),
                nullable: datum.is_null(),
            },
            Self::CallBinary(e) => e.typ(),
            Self::CallVariadic(e) => e.typ(),
        }
    }

    pub fn cast_to(
        &self,
        ecx: &ScalarExprContext,
        ty: &ScalarType,
    ) -> Result<ScalarExpr> {
        match self {
            Self::Literal(Literal { datum: Datum::String(s), scalar_type }) => {
                match ty {
                    ScalarType::Int32 => {
                        Ok(literal(Datum::Int32(Decimal::from_str_exact(s)?.try_into()?), ScalarType::Int32))
                    }
                    ScalarType::Int64 => {
                        Ok(literal(Datum::Int32(Decimal::from_str_exact(s)?.try_into()?), ScalarType::Int64))
                    }
                    _ => Err(FloppyError::NotImplemented(format!(
                        "only support implicit cast from string to numeric, explicit cast also not supported. err from {} to {}", self, ty
                    )))
                }
            }
            _ => Err(FloppyError::NotImplemented(format!(
                "only support implicit cast from string to numeric, explicit cast also not supported. err from {} to {}", self, ty
            ))),
        }
    }
}

impl fmt::Display for ScalarExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Column(c) => write!(f, "{}", c.name),
            Self::Parameter(n) => write!(f, "${}", n),
            Self::Literal(l) => write!(f, "{}", l),
            Self::CallBinary(e) => write!(f, "{}", e),
            Self::CallVariadic(e) => write!(f, "{}", e),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Literal {
    pub datum: Datum,
    pub scalar_type: ScalarType,
}

impl fmt::Display for Literal {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", format!("{}({})", self.scalar_type, self.datum))
    }
}

pub fn literal(datum: Datum, scalar_type: ScalarType) -> ScalarExpr {
    ScalarExpr::Literal(Literal { datum, scalar_type })
}

/// A `CoercibleScalarExpr` is a [`ScalarExpr`] whose type is not fully
/// determined. Several SQL expressions can be freely coerced based upon where
/// in the expression tree they appear. For example, the string literal '42'
/// will be automatically coerced to the integer 42 if used in a numeric
/// context:
///
/// ```sql
/// SELECT '42' + 42
/// ```
///
/// This separate type gives the code that needs to interact with coercions very
/// fine-grained control over what coercions happen and when.
///
/// SQl expressions will be translated to [`CoercibleScalarExpr`]first, and then
/// translated to [`ScalarExpr`] based on the expression's context.
///
/// For example in
///
/// ```sql
/// SELECT ... WHERE $1
/// ```
///
/// the `WHERE` clause will coerce the contained unconstrained type parameter
/// `$1` to have type bool.
///
/// Another example is [`CallBinary`], the exact type of the parameter depends on
/// specific function.
#[derive(Debug, Clone)]
pub enum CoercibleScalarExpr {
    Coerced(ScalarExpr),
    Parameter(usize),
    LiteralNull,
    /// A string where the type is not determined.
    /// For example in `SELECT 1 + '2'`, the actual type of '2' is
    /// determined based on the context.
    LiteralString(String),
}

impl CoercibleScalarExpr {
    pub fn typ(&self, ecx: &ScalarExprContext) -> Option<ColumnType> {
        match self {
            Self::Coerced(e) => Some(e.typ(ecx)),
            _ => None,
        }
    }

    pub fn type_as(
        &self,
        ecx: &ScalarExprContext,
        ty: &ScalarType,
    ) -> Result<ScalarExpr> {
        let expr = self.coerce_type(ecx, ty)?;
        let expr_ty = expr.typ(ecx).scalar_type;
        if expr_ty != *ty {
            Err(FloppyError::Plan(format!(
                "must have type {}, not type {}",
                ty, expr_ty
            )))
        } else {
            Ok(expr)
        }
    }

    /// Convert a `CoercibleScalarExpr` into a `ScalarExpr`.
    /// The type of `CoercibleScalarExpr::Coerced` is already known, so we actually don't do
    /// andy conversion.
    /// For other `CoercibleScalarExpr`, we convert it into `ScalarType::String`.
    pub fn type_as_any(&self, ecx: &ScalarExprContext) -> Result<ScalarExpr> {
        self.coerce_type(ecx, &ScalarType::String)
    }

    pub fn cast_to(
        &self,
        ecx: &ScalarExprContext,
        ty: &ScalarType,
    ) -> Result<ScalarExpr> {
        let expr = self.coerce_type(ecx, ty)?;
        let expr_ty = expr.typ(ecx).scalar_type;
        if expr_ty == *ty {
            return Ok(expr);
        }
        expr.cast_to(ecx, ty)
    }

    fn coerce_type(
        &self,
        ecx: &ScalarExprContext,
        ty: &ScalarType,
    ) -> Result<ScalarExpr> {
        let expr = match self {
            Self::Coerced(e) => e.clone(),
            Self::LiteralNull => literal(Datum::Null, ty.clone()),
            Self::LiteralString(s) => {
                cast(&Datum::String(s.clone()), &ScalarType::String, ty)?
            }
            Self::Parameter(n) => {
                let prev = ecx.param_types().borrow_mut().insert(*n, ty.clone());
                assert!(prev.is_none());
                ScalarExpr::Parameter(*n)
            }
        };
        Ok(expr)
    }
}

impl From<ScalarExpr> for CoercibleScalarExpr {
    fn from(expr: ScalarExpr) -> Self {
        CoercibleScalarExpr::Coerced(expr)
    }
}

/// A `RelationExpr` computes a table. It is also called a logical plan.
/// It represents a graph of data flow where each node in the graph
/// computes a table from the input of the node.
/// The `RelationExpr` is not ready to be executed yet.
#[derive(Debug)]
pub enum RelationExpr {
    /// An empty relation exists in queries without a `From` clause, eg
    /// ```sql
    /// SELECT 1 + 1;
    /// ```
    Empty,
    /// Table is the leaf of the RelationExpr tree.
    Table {
        table_id: GlobalId,
        /// The relation description of the output.
        rel_desc: RelationDesc,
        /// Partial table name.
        name: FullObjectName,
    },
    Projection {
        /// The list of expressions
        exprs: Vec<ScalarExpr>,
        /// The incoming RelationExpr
        input: Box<RelationExpr>,
        /// The relation description of the output
        rel_desc: RelationDesc,
    },
    Filter {
        input: Box<RelationExpr>,
        predicate: ScalarExpr,
    },
}

impl RelationExpr {
    pub fn rel_desc(&self) -> RelationDesc {
        match self {
            Self::Empty => RelationDesc::empty(),
            Self::Filter { input, .. } => input.rel_desc(),
            Self::Projection { rel_desc, .. } => rel_desc.clone(),
            Self::Table { rel_desc, .. } => rel_desc.clone(),
        }
    }
}

impl RelationExpr {
    pub fn accept<V>(&self, visitor: &mut V) -> std::result::Result<bool, V::Error>
    where
        V: ExprVisitor<RelationExpr>,
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

impl RelationExpr {
    /// Return a `format`able structure that produces a single line
    /// per node. For example:
    /// ```text
    /// Projection: #employee.id
    ///   Filter: #employee.state Eq Utf8(\"CO\")\
    ///     Table: employee
    /// ```
    pub fn display_tree(&self) -> impl fmt::Display + '_ {
        struct Wrapper<'a>(&'a RelationExpr);
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
        struct Wrapper<'a>(&'a RelationExpr);
        impl<'a> fmt::Display for Wrapper<'a> {
            fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
                match self.0 {
                    RelationExpr::Table { name, .. } => {
                        write!(f, "Table: {}", name.item)?;
                        Ok(())
                    }
                    RelationExpr::Projection { exprs, .. } => {
                        write!(f, "Projection: ")?;
                        for (i, expr_item) in exprs.iter().enumerate() {
                            if i > 0 {
                                write!(f, ", ")?;
                            }
                            write!(f, "{}", expr_item,)?;
                        }
                        Ok(())
                    }
                    RelationExpr::Filter { predicate, .. } => {
                        write!(f, "Filter: {}", predicate,)
                    }
                    RelationExpr::Empty => write!(f, "EmptyTable"),
                }
            }
        }
        Wrapper(self)
    }
}

impl fmt::Display for RelationExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.display_tree().fmt(f)
    }
}

pub fn parse_sql_number(n: &str) -> Result<ScalarExpr> {
    let d = Decimal::from_str_exact(n)?;
    if let Ok(n) = d.try_into() {
        Ok(literal(Datum::Int32(n), ScalarType::Int32).into())
    } else if let Ok(n) = d.try_into() {
        Ok(literal(Datum::Int64(n), ScalarType::Int64).into())
    } else {
        Err(FloppyError::NotImplemented(format!(
            "sql number not supported: {:?}",
            n
        )))
    }
}

fn cast(datum: &Datum, scalar_type: &ScalarType, to: &ScalarType) -> Result<ScalarExpr> {
    match (datum, scalar_type, to) {
        (Datum::String(s), ScalarType::String, ScalarType::Int32) => {
            let d = Decimal::from_str_exact(s)?;
            if let Ok(n) = d.try_into() {
                Ok(literal(Datum::Int32(n), ScalarType::Int32))
            } else {
                Err(FloppyError::Plan(format!(
                    "cannot cast from String to Int32: {}",
                    s
                )))
            }
        }
        (Datum::String(s), ScalarType::String, ScalarType::Int64) => {
            let d = Decimal::from_str_exact(s)?;
            if let Ok(n) = d.try_into() {
                Ok(literal(Datum::Int64(n), ScalarType::Int64))
            } else {
                Err(FloppyError::Plan(format!(
                    "cannot cast from String to Int64: {}",
                    s
                )))
            }
        }
        (Datum::String(s), ScalarType::String, ScalarType::String) => {
            Ok(literal(Datum::String(s.clone()), ScalarType::String))
        }
        _ => Err(FloppyError::NotImplemented(format!(
            "cast not implemented from datum: {} typ: {}, to : {}",
            datum, scalar_type, to
        ))),
    }
}
