use crate::context::{ScalarExprContext, StatementContext};
use crate::expr;
use crate::expr::{CoercibleScalarExpr, RelationExpr, ScalarExpr};
use crate::func::{add, gt, BinaryExpr, BinaryFunc};
use catalog::names::{FullObjectName, PartialObjectName};
use common::error::{FloppyError, Result};
use common::relation::{ColumnName, ColumnRef, ColumnType, RelationDesc};
use common::scalar::{Datum, ScalarType};
use sqlparser::ast::{
    BinaryOperator, Expr as SqlExpr, Ident as SqlIdent, Query as SqlQuery, Select,
    SelectItem, SetExpr, TableFactor, TableWithJoins, Value as SqlValue,
};

/// plan_query translate [`sqlparser::ast::Query`] into a logical plan [`PlannedQuery`]
/// which contains [`RelationExpr`] and [`RelationDesc`].
pub fn plan_query(scx: &StatementContext, query: &SqlQuery) -> Result<RelationExpr> {
    let set_expr = &query.body;
    plan_set_expr(scx, set_expr)
    // todo! order_by, limit, offset, fetch
}

fn plan_set_expr(scx: &StatementContext, set_expr: &SetExpr) -> Result<RelationExpr> {
    match set_expr {
        SetExpr::Select(select) => plan_select(scx, select),
        _ => Err(FloppyError::NotImplemented(format!(
            "Query {} not implemented yet",
            set_expr
        ))),
    }
}

fn plan_select(scx: &StatementContext, select: &Select) -> Result<RelationExpr> {
    let planned_query = plan_table_with_joins(scx, &select.from)?;
    let planned_query = plan_filter(scx, planned_query, &select.selection)?;
    plan_projection(scx, planned_query, &select.projection)
}

fn plan_table_with_joins(
    scx: &StatementContext,
    from: &Vec<TableWithJoins>,
) -> Result<RelationExpr> {
    if from.is_empty() {
        return Ok(RelationExpr::Empty);
    }

    // we only consider single table without Join for now.
    // todo JOIN
    if from.len() > 1 {
        return Err(FloppyError::NotImplemented(format!(
            "FROM with multiple table is not implemented yet: {:?}",
            from
        )));
    }

    let table_factor = &from[0].relation;
    match table_factor {
        // alias, args, with_hints are not supported
        TableFactor::Table { alias: Some(_), .. } => Err(FloppyError::NotImplemented(
            format!("table alias {} not implemented yet", table_factor),
        )),
        TableFactor::Table { args: Some(_), .. } => Err(FloppyError::NotImplemented(
            format!("table args {} not implemented yet", table_factor),
        )),
        TableFactor::Table { name, .. } => {
            let partial_object_name: PartialObjectName = name.try_into()?;
            let table = scx.catalog.resolve_item(&partial_object_name)?;
            let full_name: FullObjectName = partial_object_name.into();
            Ok(RelationExpr::Table {
                table_id: table.id(),
                rel_desc: table.desc(&full_name)?.into_owned(),
                name: full_name,
            })
        }
        _ => Err(FloppyError::NotImplemented(format!(
            "table factor {} not implemented yet",
            table_factor
        ))),
    }
}

fn plan_filter(
    scx: &StatementContext,
    input: RelationExpr,
    filter: &Option<SqlExpr>,
) -> Result<RelationExpr> {
    match filter {
        Some(filter) => {
            let ecx = ScalarExprContext {
                scx,
                rel_desc: &input.rel_desc(),
            };
            let expr = plan_expr(&ecx, filter)?;
            let expr = expr.type_as(&ecx, &ScalarType::Boolean)?;
            Ok(RelationExpr::Filter {
                input: Box::new(input),
                predicate: expr,
            })
        }
        None => Ok(input),
    }
}

struct ProjectionCtx {
    expr: ScalarExpr,
    column_name: ColumnName,
    typ: ColumnType,
}

fn plan_projection(
    scx: &StatementContext,
    input: RelationExpr,
    projection: &Vec<SelectItem>,
) -> Result<RelationExpr> {
    let ecx = ScalarExprContext {
        scx,
        rel_desc: &input.rel_desc(),
    };
    let ctxs = projection
        .into_iter()
        .map(|e| {
            let expr = plan_select_item(&ecx, e)?.type_as_any(&ecx)?;
            let column_name = match &expr {
                ScalarExpr::Column(ColumnRef { name, .. }) => name.clone(),
                _ => "?column?".to_string(),
            };
            let typ = expr.typ(&ecx);
            Ok(ProjectionCtx {
                expr,
                column_name,
                typ,
            })
        })
        .collect::<Result<Vec<ProjectionCtx>>>()?;

    let column_types = ctxs
        .iter()
        .map(|c| c.typ.clone())
        .collect::<Vec<ColumnType>>();
    let column_names = ctxs
        .iter()
        .map(|c| c.column_name.clone())
        .collect::<Vec<ColumnName>>();

    let rel_desc = RelationDesc::new(column_types, column_names);
    let exprs = ctxs
        .iter()
        .map(|c| c.expr.clone())
        .collect::<Vec<ScalarExpr>>();
    Ok(RelationExpr::Projection {
        exprs,
        input: Box::new(input),
        rel_desc,
    })
}

fn plan_select_item(
    ecx: &ScalarExprContext,
    item: &SelectItem,
) -> Result<CoercibleScalarExpr> {
    match item {
        SelectItem::UnnamedExpr(expr) => plan_expr(ecx, expr),
        _ => Err(FloppyError::NotImplemented(format!(
            "select item not supported: {}",
            item
        ))),
    }
}

pub fn plan_expr(
    ecx: &ScalarExprContext,
    sql_expr: &SqlExpr,
) -> Result<CoercibleScalarExpr> {
    match sql_expr {
        SqlExpr::Value(v) => plan_literal(ecx, v),
        SqlExpr::Identifier(name) => plan_identifier(ecx, name),
        SqlExpr::BinaryOp { left, op, right } => plan_binary_op(ecx, left, op, right),
        _ => Err(FloppyError::NotImplemented(format!(
            "Unsupported expression {:?}",
            sql_expr
        ))),
    }
}

fn plan_literal(
    ecx: &ScalarExprContext,
    literal: &SqlValue,
) -> Result<CoercibleScalarExpr> {
    match literal {
        SqlValue::Number(n, _) => expr::parse_sql_number(&n).map(|e| e.into()),
        SqlValue::SingleQuotedString(s) => {
            Ok(CoercibleScalarExpr::LiteralString(s.to_string()))
        }
        SqlValue::DoubleQuotedString(s) => {
            Ok(CoercibleScalarExpr::LiteralString(s.to_string()))
        }
        SqlValue::Boolean(b) => {
            Ok(expr::literal(Datum::Boolean(b.clone()), ScalarType::Boolean).into())
        }
        SqlValue::Null => Ok(CoercibleScalarExpr::LiteralNull),
        SqlValue::Placeholder(p) => plan_parameter(ecx, p.to_string()),
        _ => Err(FloppyError::NotImplemented(format!(
            "literal not supported: {}",
            literal
        ))),
    }
}

fn plan_identifier(
    ecx: &ScalarExprContext,
    name: &SqlIdent,
) -> Result<CoercibleScalarExpr> {
    let rel_desc = ecx.rel_desc;
    let id = rel_desc.column_idx(&name.value)?;
    let name = rel_desc.column_name(id).to_string();
    Ok(ScalarExpr::Column(ColumnRef { id, name }).into())
}

fn plan_binary_op(
    ecx: &ScalarExprContext,
    left: &SqlExpr,
    op: &BinaryOperator,
    right: &SqlExpr,
) -> Result<CoercibleScalarExpr> {
    let rel_desc = ecx.rel_desc;
    let left = plan_expr(ecx, left)?;
    let right = plan_expr(ecx, right)?;
    match op {
        BinaryOperator::Plus => plan_bop_plus(ecx, left, right),
        BinaryOperator::Minus => plan_bop_minus(ecx, left, right),
        BinaryOperator::Gt => plan_bop_gt(ecx, left, right),
        BinaryOperator::Lt => plan_bop_lt(ecx, left, right),
        BinaryOperator::GtEq => plan_bop_gte(ecx, left, right),
        BinaryOperator::LtEq => plan_bop_lte(ecx, left, right),
        BinaryOperator::Eq => plan_bop_eq(ecx, left, right),
        BinaryOperator::NotEq => plan_bop_neq(ecx, left, right),
        BinaryOperator::And => plan_bop_and(ecx, left, right),
        BinaryOperator::Or => plan_bop_or(ecx, left, right),
        _ => Err(FloppyError::NotImplemented(format!(
            "binary op not implemented: {:?}",
            op
        ))),
    }
}

fn plan_parameter(ecx: &ScalarExprContext, p: String) -> Result<CoercibleScalarExpr> {
    let param = p.strip_prefix("$");
    if param.is_none() {
        return Err(FloppyError::Plan(format!("invalid parameter: {}", p)));
    }

    let n = param
        .unwrap()
        .parse::<i32>()
        .map_err(|e| FloppyError::Plan(format!("parse parameter error: {}", p)))?
        as usize;

    if ecx.param_types().borrow().contains_key(&n) {
        Ok(ScalarExpr::Parameter(n).into())
    } else {
        Ok(CoercibleScalarExpr::Parameter(n))
    }
}

/// Valid binary expressions in PostgreSQL:
/// ```sql
/// SELECT 1 + 2;
/// SELECT 1 + '2';
/// SELECT 1 + NULL;
/// ```
/// Invalid numberic expressions in PostgreSQL:
/// ```sql
/// SELECT '1' + '2';
/// SELECT '1' + NULL;
/// ```
///
///  At least one of the expression is a numeric type.
fn plan_bop_plus(
    ecx: &ScalarExprContext,
    cexpr1: CoercibleScalarExpr,
    cexpr2: CoercibleScalarExpr,
) -> Result<CoercibleScalarExpr> {
    let expr1 = cexpr1.type_as_any(ecx)?;
    let expr2 = cexpr2.type_as_any(ecx)?;

    let (expr1, expr2) = numeric_op_cast(ecx, expr1, expr2)?;
    add(ecx, &expr1, &expr2).map(|e| e.into())
}

fn plan_bop_minus(
    ecx: &ScalarExprContext,
    left: CoercibleScalarExpr,
    right: CoercibleScalarExpr,
) -> Result<CoercibleScalarExpr> {
    unimplemented!()
}

fn plan_bop_gt(
    ecx: &ScalarExprContext,
    left: CoercibleScalarExpr,
    right: CoercibleScalarExpr,
) -> Result<CoercibleScalarExpr> {
    let expr1 = left.type_as_any(ecx)?;
    let expr2 = right.type_as_any(ecx)?;

    let (expr1, expr2) = numeric_op_cast(ecx, expr1, expr2)?;
    gt(ecx, &expr1, &expr2).map(|e| e.into())
}

fn plan_bop_lt(
    ecx: &ScalarExprContext,
    left: CoercibleScalarExpr,
    right: CoercibleScalarExpr,
) -> Result<CoercibleScalarExpr> {
    unimplemented!()
}

fn plan_bop_gte(
    ecx: &ScalarExprContext,
    left: CoercibleScalarExpr,
    right: CoercibleScalarExpr,
) -> Result<CoercibleScalarExpr> {
    unimplemented!()
}

fn plan_bop_lte(
    ecx: &ScalarExprContext,
    left: CoercibleScalarExpr,
    right: CoercibleScalarExpr,
) -> Result<CoercibleScalarExpr> {
    unimplemented!()
}

fn plan_bop_eq(
    ecx: &ScalarExprContext,
    left: CoercibleScalarExpr,
    right: CoercibleScalarExpr,
) -> Result<CoercibleScalarExpr> {
    unimplemented!()
}

fn plan_bop_neq(
    ecx: &ScalarExprContext,
    left: CoercibleScalarExpr,
    right: CoercibleScalarExpr,
) -> Result<CoercibleScalarExpr> {
    unimplemented!()
}

fn plan_bop_and(
    ecx: &ScalarExprContext,
    left: CoercibleScalarExpr,
    right: CoercibleScalarExpr,
) -> Result<CoercibleScalarExpr> {
    unimplemented!()
}

fn plan_bop_or(
    ecx: &ScalarExprContext,
    left: CoercibleScalarExpr,
    right: CoercibleScalarExpr,
) -> Result<CoercibleScalarExpr> {
    unimplemented!()
}

fn numeric_op_cast(
    ecx: &ScalarExprContext,
    expr1: ScalarExpr,
    expr2: ScalarExpr,
) -> Result<(ScalarExpr, ScalarExpr)> {
    let c1_type = expr1.typ(ecx);
    let c2_type = expr2.typ(ecx);
    let is_c1_numeric = c1_type.scalar_type.is_numeric();
    let is_c2_numeric = c2_type.scalar_type.is_numeric();

    match (is_c1_numeric, is_c2_numeric) {
        (true, true) => match (c1_type.scalar_type, c2_type.scalar_type) {
            (ScalarType::Int64, _) | (_, ScalarType::Int64) => {
                let expr1 = expr1.cast_to(ecx, &ScalarType::Int64)?;
                let expr2 = expr2.cast_to(ecx, &ScalarType::Int64)?;
                Ok((expr1, expr2))
            }
            (ScalarType::Int32, _) | (_, ScalarType::Int32) => {
                let expr1 = expr1.cast_to(ecx, &ScalarType::Int32)?;
                let expr2 = expr2.cast_to(ecx, &ScalarType::Int32)?;
                Ok((expr1, expr2))
            }
            (ScalarType::Int16, _) | (_, ScalarType::Int16) => {
                let expr1 = expr1.cast_to(ecx, &ScalarType::Int16)?;
                let expr2 = expr2.cast_to(ecx, &ScalarType::Int16)?;
                Ok((expr1, expr2))
            }
            _ => Err(FloppyError::Internal(format!("numeric type error"))),
        },
        (true, false) => {
            let expr2 = expr2.cast_to(ecx, &c1_type.scalar_type)?;
            Ok((expr1, expr2))
        }
        (false, true) => {
            let expr1 = expr1.cast_to(ecx, &c2_type.scalar_type)?;
            Ok((expr1, expr2))
        }
        (false, false) => Err(FloppyError::Plan(format!(
            "Could not choose a best candidate operator"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::error::CatalogError;
    use sqlparser::ast::Statement;
    use sqlparser::dialect::PostgreSqlDialect;
    use sqlparser::parser::Parser;
    use std::cell::RefCell;

    fn seed_catalog(catalog: &mut catalog::memory::MemCatalog) {
        let desc = RelationDesc::new(
            vec![
                ColumnType::new(ScalarType::Int32, false),
                ColumnType::new(ScalarType::Int32, false),
            ],
            vec!["c1".to_string(), "c2".to_string()],
        );
        catalog.insert_table("test", 1, desc)
    }

    fn logical_plan(scx: &StatementContext, sql: &str) -> Result<RelationExpr> {
        let dialect = PostgreSqlDialect {};
        let ast = &Parser::parse_sql(&dialect, sql)?[0];
        match ast {
            Statement::Query(q) => plan_query(scx, q),
            _ => Err(FloppyError::NotImplemented(format!(
                "not implemented {}",
                ast
            ))),
        }
    }

    fn quick_test_eq(scx: &StatementContext, sql: &str, expected: &str) -> Result<()> {
        let plan = logical_plan(scx, sql)?;
        assert_eq!(format!("{}", plan), expected);
        Ok(())
    }

    fn quick_test_fail(scx: &StatementContext, sql: &str) -> Result<()> {
        let plan = logical_plan(scx, sql)?;
        Ok(())
    }

    #[test]
    fn select_no_relation_single_column() {
        let scx = StatementContext {
            catalog: &catalog::memory::MemCatalog::default(),
            param_types: RefCell::default(),
        };

        quick_test_eq(
            &scx,
            "SELECT 1",
            "Projection: Int32(1)\
                   \n  EmptyTable",
        );

        quick_test_eq(
            &scx,
            "SELECT 1 + 1",
            "Projection: Int32(1) + Int32(1)\
                    \n  EmptyTable",
        );

        quick_test_eq(
            &scx,
            "SELECT 1 + '2'",
            "Projection: Int32(1) + Int32(2)\
                    \n  EmptyTable",
        );

        quick_test_eq(
            &scx,
            "SELECT 1 + ?",
            "Projection: Int32(1) + Int32(?)\
                   \n  EmptyTable",
        );

        quick_test_eq(
            &scx,
            "SELECT 2, 3",
            "Projection: Int32(2), Int32(3)\
                    \n  EmptyTable",
        );

        quick_test_eq(
            &scx,
            "SELECT 2 + 4, 3",
            "Projection: Int32(2) + Int32(4), Int32(3)\
                    \n  EmptyTable",
        );

        quick_test_eq(
            &scx,
            "SELECT 2 + 2147483648, 3",
            "Projection: Int64(2) + Int64(4), Int32(3)\
                    \n  EmptyTable",
        );

        let err = quick_test_fail(&scx, "SELECT '1' + '2'").expect_err("sql error");
        assert_eq!(
            err.to_string()
                .contains("Could not choose a best candidate operator"),
            true
        );

        let err = quick_test_fail(&scx, "SELECT $1 + $2").expect_err("sql error");
        assert_eq!(
            err.to_string()
                .contains("Could not choose a best candidate operator"),
            true
        );
    }

    #[test]
    fn select_table_not_exists() {
        let mut catalog = catalog::memory::MemCatalog::default();
        seed_catalog(&mut catalog);
        let scx = StatementContext {
            catalog: &catalog,
            param_types: RefCell::default(),
        };

        let err =
            logical_plan(&scx, "SELECT * FROM faketable").expect_err("query is invalid");
        assert!(matches!(
            err,
            FloppyError::Catalog(CatalogError::TableNotFound(_))
        ));
    }

    #[test]
    fn select_column_not_exists() {
        let mut catalog = catalog::memory::MemCatalog::default();
        seed_catalog(&mut catalog);
        let scx = StatementContext {
            catalog: &catalog,
            param_types: RefCell::default(),
        };

        let err =
            logical_plan(&scx, "SELECT fake FROM test").expect_err("query is invalid");
        println!("err: {}", err);
        assert!(matches!(
            err,
            FloppyError::Catalog(CatalogError::ColumnNotFound { .. })
        ));
    }

    #[test]
    fn select_column() {
        let mut catalog = catalog::memory::MemCatalog::default();
        seed_catalog(&mut catalog);
        let scx = StatementContext {
            catalog: &catalog,
            param_types: RefCell::default(),
        };

        quick_test_eq(
            &scx,
            "SELECT c1 FROM test",
            "Projection: c1\
                   \n  Table: test",
        );

        quick_test_eq(
            &scx,
            "SELECT * FROM test",
            "Projection: c1, c2\
                   \n  Table: test",
        );
    }

    #[test]
    fn select_filter() {
        let mut catalog = catalog::memory::MemCatalog::default();
        seed_catalog(&mut catalog);
        let scx = StatementContext {
            catalog: &catalog,
            param_types: RefCell::default(),
        };

        quick_test_eq(
            &scx,
            "SELECT c1 FROM test WHERE c2 > 100",
            "Projection: c1\
                   \n  Filter: c2 > 100\
                   \n    Table: test",
        );
    }
}
