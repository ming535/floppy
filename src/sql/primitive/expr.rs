use crate::common::error::{FloppyError, Result};
use crate::common::relation::{ColumnRef, ColumnType, RelationDesc, Row};
use crate::common::scalar::{Datum, ScalarType};
use crate::sql::context::ExprContext;
use crate::sql::primitive::func::{BinaryExpr, VariadicExpr};
use rust_decimal::Decimal;
use std::fmt;
use std::fmt::Formatter;

/// A `Expr` computes a scalar.
///  https://www.postgresql.org/docs/current/sql-expressions.html
#[derive(Debug, Clone)]
pub enum Expr {
    /// A column reference.
    Column(ColumnRef),
    /// Positional parameter when prepare a SQL statement
    /// for execution: https://www.postgresql.org/docs/current/sql-prepare.html
    Parameter(usize),
    /// A constant value.
    Literal(Literal),
    /// A binary expression.
    CallBinary(BinaryExpr),
    /// An expression that have variable number of
    /// parameters. for example: 1 == 2 AND 2 == 3 OR 4
    /// > 5
    CallVariadic(VariadicExpr),
}

impl Expr {
    pub fn typ(&self, ecx: &ExprContext) -> ColumnType {
        match self {
            Self::Column(ColumnRef { id, .. }) => {
                ecx.rel_desc.rel_type().column_type(*id).clone()
            }
            Self::Parameter(n) => {
                ecx.param_types().borrow()[n].clone().nullable(true)
            }
            Self::Literal(Literal { datum, scalar_type }) => ColumnType {
                scalar_type: scalar_type.clone(),
                nullable: datum.is_null(),
            },
            Self::CallBinary(e) => e.typ(),
            Self::CallVariadic(e) => e.typ(),
        }
    }

    pub fn cast_to(&self, ecx: &ExprContext, ty: &ScalarType) -> Result<Expr> {
        if self.typ(ecx).scalar_type == *ty {
            return Ok(self.clone());
        }

        match self {
            Self::Literal(Literal {
                datum: Datum::Text(s),
                scalar_type: _,
            }) => match ty {
                ScalarType::Int64 => Ok(literal_i64(Decimal::from_str_exact(s)?.try_into()?)),
                _ => Err(FloppyError::NotImplemented(format!(
                    "only support implicit cast from string to numeric, explicit cast also not \
                     supported. err from {self} to {ty}"
                ))),
            },
            _ => Err(FloppyError::NotImplemented(format!(
                "only support implicit cast from string to numeric, explicit cast also not \
                 supported. err from {self} to {ty}"
            ))),
        }
    }

    pub fn evaluate(&self, ecx: &ExprContext, row: &Row) -> Result<Datum> {
        match self {
            Self::Column(ColumnRef { id, .. }) => row.column_value(*id),
            Self::Parameter(n) => Ok(ecx.param_values().borrow()[n].clone()),
            Self::Literal(Literal { datum, .. }) => Ok(datum.clone()),
            Self::CallBinary(e) => e.evaluate(ecx, row),
            Self::CallVariadic(e) => e.evaluate(ecx, row),
        }
    }
}

impl fmt::Display for Expr {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Column(c) => write!(f, "{}", c.name),
            Self::Parameter(n) => write!(f, "${n}"),
            Self::Literal(l) => write!(f, "{l}"),
            Self::CallBinary(e) => write!(f, "{e}"),
            Self::CallVariadic(e) => write!(f, "{e}"),
        }
    }
}

pub fn wildcard_column_ref(rel_desc: &RelationDesc) -> Vec<Expr> {
    rel_desc
        .column_names()
        .iter()
        .enumerate()
        .map(|(id, name)| {
            Expr::Column(ColumnRef {
                id,
                name: name.clone(),
            })
        })
        .collect::<Vec<Expr>>()
}

#[derive(Debug, Clone)]
pub struct Literal {
    pub datum: Datum,
    pub scalar_type: ScalarType,
}

impl fmt::Display for Literal {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}({})", self.scalar_type, self.datum)
    }
}

pub fn literal_true() -> Expr {
    Expr::Literal(Literal {
        datum: Datum::Boolean(true),
        scalar_type: ScalarType::Boolean,
    })
}

pub fn literal_false() -> Expr {
    Expr::Literal(Literal {
        datum: Datum::Boolean(false),
        scalar_type: ScalarType::Boolean,
    })
}

pub fn literal_boolean(b: bool) -> Expr {
    Expr::Literal(Literal {
        datum: Datum::Boolean(b),
        scalar_type: ScalarType::Boolean,
    })
}

pub fn literal_i64(i: i64) -> Expr {
    Expr::Literal(Literal {
        datum: Datum::Int64(i),
        scalar_type: ScalarType::Int64,
    })
}

pub fn literal_text(s: &str) -> Expr {
    Expr::Literal(Literal {
        datum: Datum::Text(s.to_string()),
        scalar_type: ScalarType::Text,
    })
}

pub fn literal_null(ty: ScalarType) -> Expr {
    Expr::Literal(Literal {
        datum: Datum::Null,
        scalar_type: ty,
    })
}

/// A `CoercibleExpr` is a [`Expr`] whose type is not fully
/// determined. Several SQL expressions can be freely
/// coerced based upon where in the expression tree they
/// appear. For example, the string literal '42'
/// will be automatically coerced to the integer 42 if used
/// in a numeric context:
///
/// ```sql
/// SELECT '42' + 42
/// ```
///
/// This separate type gives the code that needs to interact
/// with coercions very fine-grained control over what
/// coercions happen and when.
///
/// SQl expressions will be translated to
/// [`CoercibleExpr`]first, and then translated to [`Expr`]
/// based on the expression's context.
///
/// For example in
///
/// ```sql
/// SELECT ... WHERE $1
/// ```
///
/// the `WHERE` clause will coerce the contained
/// unconstrained type parameter `$1` to have type bool.
///
/// Another example is [`CallBinary`], the exact type of the
/// parameter depends on specific function.
#[derive(Debug, Clone)]
pub enum CoercibleExpr {
    Coerced(Expr),
    Parameter(usize),
    LiteralNull,
    /// A string where the type is not determined.
    /// For example in `SELECT 1 + '2'`, the actual type of
    /// '2' is determined based on the context.
    LiteralString(String),
}

impl CoercibleExpr {
    pub fn typ(&self, ecx: &ExprContext) -> Option<ColumnType> {
        match self {
            Self::Coerced(e) => Some(e.typ(ecx)),
            _ => None,
        }
    }

    pub fn type_as(&self, ecx: &ExprContext, ty: &ScalarType) -> Result<Expr> {
        let expr = self.coerce_type(ecx, ty)?;
        let expr_ty = expr.typ(ecx).scalar_type;
        if expr_ty != *ty {
            Err(FloppyError::Plan(format!(
                "must have type {ty}, not type {expr_ty}"
            )))
        } else {
            Ok(expr)
        }
    }

    /// Convert a `CoercibleExpr` into a `Expr`.
    /// The type of `CoercibleExpr::Coerced` is already
    /// known, so we actually don't do andy conversion.
    /// For other `CoercibleExpr`, we convert it into
    /// `ScalarType::String`.
    pub fn type_as_any(&self, ecx: &ExprContext) -> Result<Expr> {
        self.coerce_type(ecx, &ScalarType::Text)
    }

    pub fn cast_to(&self, ecx: &ExprContext, ty: &ScalarType) -> Result<Expr> {
        let expr = self.coerce_type(ecx, ty)?;
        let expr_ty = expr.typ(ecx).scalar_type;
        if expr_ty == *ty {
            return Ok(expr);
        }
        expr.cast_to(ecx, ty)
    }

    fn coerce_type(&self, ecx: &ExprContext, ty: &ScalarType) -> Result<Expr> {
        let expr = match self {
            Self::Coerced(e) => e.clone(),
            Self::LiteralNull => literal_null(ty.clone()),
            Self::LiteralString(s) => {
                cast(&Datum::Text(s.clone()), &ScalarType::Text, ty)?
            }
            Self::Parameter(n) => {
                let prev =
                    ecx.param_types().borrow_mut().insert(*n, ty.clone());
                assert!(prev.is_none());
                Expr::Parameter(*n)
            }
        };
        Ok(expr)
    }
}

impl From<Expr> for CoercibleExpr {
    fn from(expr: Expr) -> Self {
        CoercibleExpr::Coerced(expr)
    }
}

pub fn parse_sql_number(n: &str) -> Result<Expr> {
    let d = Decimal::from_str_exact(n)?;
    if let Ok(n) = d.try_into() {
        Ok(literal_i64(n))
    } else {
        Err(FloppyError::NotImplemented(format!(
            "sql number not supported: {n:?}"
        )))
    }
}

fn cast(
    datum: &Datum,
    scalar_type: &ScalarType,
    to: &ScalarType,
) -> Result<Expr> {
    match (datum, scalar_type, to) {
        (Datum::Text(s), ScalarType::Text, ScalarType::Int64) => {
            let d = Decimal::from_str_exact(s)?;
            if let Ok(n) = d.try_into() {
                Ok(literal_i64(n))
            } else {
                Err(FloppyError::Plan(format!(
                    "cannot cast from String to Int64: {s}"
                )))
            }
        }
        (Datum::Text(s), ScalarType::Text, ScalarType::Text) => {
            Ok(literal_text(s))
        }
        _ => Err(FloppyError::NotImplemented(format!(
            "cast not implemented from datum: {datum} typ: {scalar_type}, to : {to}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog;

    use crate::common::relation::RelationDesc;
    use crate::sql::context::StatementContext;
    use crate::sql::primitive::func::{add, and, equal, gt, or};

    use std::sync::Arc;

    fn seed_catalog(catalog: &mut catalog::memory::MemCatalog) {
        let desc = RelationDesc::new(
            vec![
                ColumnType::new(ScalarType::Int64, false),
                ColumnType::new(ScalarType::Int64, false),
            ],
            vec!["c1".to_string(), "c2".to_string()],
            vec![],
            vec![],
        );
        catalog.insert_table("test", 1, desc)
    }

    #[test]
    fn addition() -> Result<()> {
        let catalog = Arc::new(catalog::memory::MemCatalog::default());
        // seed_catalog(&mut catalog);
        // let partial_obj_name: PartialObjectName = "test".into();
        // let full_obj_name: FullObjectName = "test".into();
        // let rel_desc = catalog
        //     .resolve_item(&partial_obj_name)?
        //     .desc(&full_obj_name)?;

        let ecx = ExprContext {
            scx: Arc::new(StatementContext::new(catalog)),
            rel_desc: Arc::new(RelationDesc::empty()),
        };

        let l1 = literal_i64(1);
        let l2 = l1.clone();

        // 1 + 1 = 2
        let l3 = add(&ecx, &l1, &l2)?;
        assert_eq!(format!("{l3}"), "Int64(1) + Int64(1)");
        let d = l3.evaluate(&ecx, &Row::empty())?;
        assert_eq!(format!("{d}"), "2");

        // (1 + 1) + 100 = 102
        let l4 = literal_i64(100);
        let l5 = add(&ecx, &l3, &l4)?;
        let d = l5.evaluate(&ecx, &Row::empty())?;
        assert_eq!(format!("{d}"), "102");

        Ok(())
    }

    #[test]
    fn logical_expr() -> Result<()> {
        let catalog = Arc::new(catalog::memory::MemCatalog::default());
        let ecx = ExprContext {
            scx: Arc::new(StatementContext::new(catalog)),
            rel_desc: Arc::new(RelationDesc::empty()),
        };

        // TRUE == FALSE
        let l1 = literal_true();
        let l2 = literal_false();
        let l3 = equal(&ecx, &l1, &l2)?;
        let d = l3.evaluate(&ecx, &Row::empty())?;
        assert_eq!(format!("{d}"), "FALSE");

        // TRUE AND FALSE
        let l1 = literal_true();
        let l2 = literal_false();
        let l3 = and(vec![l1, l2]);
        let d = l3.evaluate(&ecx, &Row::empty())?;
        assert_eq!(format!("{d}"), "FALSE");

        // TRUE AND TRUE
        let l1 = literal_true();
        let l2 = literal_true();
        let l3 = and(vec![l1, l2]);
        let d = l3.evaluate(&ecx, &Row::empty())?;
        assert_eq!(format!("{d}"), "TRUE");

        // TRUE OR FALSE
        let l1 = literal_true();
        let l2 = literal_false();
        let l3 = or(vec![l1, l2]);
        let d = l3.evaluate(&ecx, &Row::empty())?;
        assert_eq!(format!("{d}"), "TRUE");

        // FALSE OR FALSE
        let l1 = literal_false();
        let l2 = literal_false();
        let l3 = or(vec![l1, l2]);
        let d = l3.evaluate(&ecx, &Row::empty())?;
        assert_eq!(format!("{d}"), "FALSE");

        // 2 == 3
        let l1 = literal_i64(2);
        let l2 = literal_i64(3);

        let l3 = equal(&ecx, &l1, &l2)?;
        let d = l3.evaluate(&ecx, &Row::empty())?;
        assert_eq!(format!("{d}"), "FALSE");

        // ((1 + 4) == 5) AND (6 > 3)
        let l1 = add(&ecx, &literal_i64(1), &literal_i64(4))?;
        let l2 = equal(&ecx, &l1, &literal_i64(5))?;
        let l3 = gt(&ecx, &literal_i64(6), &literal_i64(3))?;
        let l4 = and(vec![l2, l3]);
        let d = l4.evaluate(&ecx, &Row::empty())?;
        assert_eq!(format!("{d}"), "TRUE");

        Ok(())
    }
}
