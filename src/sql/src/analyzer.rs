use crate::context::{ExprContext, StatementContext};
use crate::primitive::expr;
use crate::primitive::expr::{wildcard_column_ref, CoercibleExpr, Expr};
use crate::primitive::func::{add, gt};
use crate::LogicalPlan;
use catalog::names::{FullObjectName, PartialObjectName};
use common::error::{FloppyError, Result};
use common::relation::{ColumnName, ColumnRef, ColumnType, RelationDesc};
use common::scalar::ScalarType;
use sqlparser::ast::{
    BinaryOperator, Expr as AstExpr, Ident as AstIdent, Query as AstQuery, Select,
    SelectItem, SetExpr, Statement as SqlStatement, TableFactor, TableWithJoins,
    Value as SqlValue,
};
use std::sync::Arc;

pub fn transform_statement(
    scx: &StatementContext,
    s: &SqlStatement,
) -> Result<LogicalPlan> {
    match s {
        SqlStatement::Query(q) => transform_query(scx, &q),
        _ => Err(FloppyError::NotImplemented(format!(
            "statement not implemented yet: {}",
            s
        ))),
    }
}

/// transform_query translate [`sqlparser::ast::Query`] into a logical sql [`PlannedQuery`]
/// which contains [`LogicalPlan`] and [`RelationDesc`].
pub(crate) fn transform_query(
    scx: &StatementContext,
    query: &AstQuery,
) -> Result<LogicalPlan> {
    let set_expr = &query.body;
    transform_set_expr(scx, set_expr)
    // todo! order_by, limit, offset, fetch
}

fn transform_set_expr(scx: &StatementContext, set_expr: &SetExpr) -> Result<LogicalPlan> {
    match set_expr {
        SetExpr::Select(select) => transform_select(scx, select),
        _ => Err(FloppyError::NotImplemented(format!(
            "Query {} not implemented yet",
            set_expr
        ))),
    }
}

fn transform_select(scx: &StatementContext, select: &Select) -> Result<LogicalPlan> {
    let planned_query = transform_table_with_joins(scx, &select.from)?;
    let planned_query = transform_filter(scx, planned_query, &select.selection)?;
    transform_projection(scx, planned_query, &select.projection)
}

fn transform_table_with_joins(
    scx: &StatementContext,
    from: &Vec<TableWithJoins>,
) -> Result<LogicalPlan> {
    if from.is_empty() {
        return Ok(LogicalPlan::Empty);
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
            Ok(LogicalPlan::Table {
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

fn transform_filter(
    scx: &StatementContext,
    input: LogicalPlan,
    filter: &Option<AstExpr>,
) -> Result<LogicalPlan> {
    match filter {
        Some(filter) => {
            let ecx = ExprContext {
                scx: Arc::new(scx.clone()),
                rel_desc: Arc::new(input.rel_desc()),
            };
            let expr = transform_expr(&ecx, filter)?;
            let expr = expr.type_as(&ecx, &ScalarType::Boolean)?;
            Ok(LogicalPlan::Filter {
                input: Box::new(input),
                predicate: expr,
            })
        }
        None => Ok(input),
    }
}

struct ProjectionCtx {
    expr: Expr,
    column_name: ColumnName,
    typ: ColumnType,
}

fn transform_projection(
    scx: &StatementContext,
    input: LogicalPlan,
    projection: &Vec<SelectItem>,
) -> Result<LogicalPlan> {
    let ecx = ExprContext {
        scx: Arc::new(scx.clone()),
        rel_desc: Arc::new(input.rel_desc()),
    };
    let exprs = projection
        .into_iter()
        .map(|e| {
            transform_select_item(&ecx, e)
            // let column_name = match &expr {
            //     Expr::Column(ColumnRef { name, .. }) => name.clone(),
            //     _ => "?column?".to_string(),
            // };
            // let typ = expr.typ(&ecx);
            // Ok(ProjectionCtx {
            //     expr,
            //     column_name,
            //     typ,
            // })
        })
        .collect::<Result<Vec<Vec<CoercibleExpr>>>>()?;

    let exprs = exprs
        .into_iter()
        .flat_map(|e| e.into_iter())
        .collect::<Vec<CoercibleExpr>>();

    let exprs = exprs
        .into_iter()
        .map(|e| e.type_as_any(&ecx))
        .collect::<Result<Vec<Expr>>>()?;

    // let column_types = ctxs
    //     .iter()
    //     .map(|c| c.typ.clone())
    //     .collect::<Vec<ColumnType>>();
    // let column_names = ctxs
    //     .iter()
    //     .map(|c| c.column_name.clone())
    //     .collect::<Vec<ColumnName>>();
    let column_names = exprs
        .iter()
        .map(|e| match e {
            Expr::Column(ColumnRef { name, .. }) => name.to_string(),
            _ => "?column?".to_string(),
        })
        .collect::<Vec<String>>();

    let column_types = exprs
        .iter()
        .map(|e| e.typ(&ecx))
        .collect::<Vec<ColumnType>>();

    let rel_desc = RelationDesc::new(column_types, column_names, vec![], vec![]);
    // let exprs = exprs.iter().map(|c| c.expr.clone()).collect::<Vec<Expr>>();
    Ok(LogicalPlan::Projection {
        exprs,
        input: Box::new(input),
        rel_desc,
    })
}

fn transform_select_item(
    ecx: &ExprContext,
    item: &SelectItem,
) -> Result<Vec<CoercibleExpr>> {
    match item {
        SelectItem::UnnamedExpr(expr) => Ok(vec![transform_expr(ecx, expr)?]),
        SelectItem::Wildcard => Ok(wildcard_column_ref(&ecx.rel_desc)
            .into_iter()
            .map(|e| e.into())
            .collect::<Vec<CoercibleExpr>>()),
        _ => Err(FloppyError::NotImplemented(format!(
            "select item not supported: {}",
            item
        ))),
    }
}

pub fn transform_expr(ecx: &ExprContext, sql_expr: &AstExpr) -> Result<CoercibleExpr> {
    match sql_expr {
        AstExpr::Value(v) => transform_literal(ecx, v),
        AstExpr::Identifier(name) => transform_identifier(ecx, name),
        AstExpr::BinaryOp { left, op, right } => {
            transform_binary_op(ecx, left, op, right)
        }
        _ => Err(FloppyError::NotImplemented(format!(
            "Unsupported expression {:?}",
            sql_expr
        ))),
    }
}

fn transform_literal(ecx: &ExprContext, literal: &SqlValue) -> Result<CoercibleExpr> {
    match literal {
        SqlValue::Number(n, _) => expr::parse_sql_number(&n).map(|e| e.into()),
        SqlValue::SingleQuotedString(s) => {
            Ok(CoercibleExpr::LiteralString(s.to_string()))
        }
        SqlValue::DoubleQuotedString(s) => {
            Ok(CoercibleExpr::LiteralString(s.to_string()))
        }
        SqlValue::Boolean(b) => Ok(expr::literal_boolean(*b).into()),
        SqlValue::Null => Ok(CoercibleExpr::LiteralNull),
        SqlValue::Placeholder(p) => transform_parameter(ecx, p.to_string()),
        _ => Err(FloppyError::NotImplemented(format!(
            "literal not supported: {}",
            literal
        ))),
    }
}

fn transform_identifier(ecx: &ExprContext, name: &AstIdent) -> Result<CoercibleExpr> {
    let rel_desc = ecx.rel_desc.clone();
    let id = rel_desc.column_idx(&name.value)?;
    let name = rel_desc.column_name(id).to_string();
    Ok(Expr::Column(ColumnRef { id, name }).into())
}

fn transform_binary_op(
    ecx: &ExprContext,
    left: &AstExpr,
    op: &BinaryOperator,
    right: &AstExpr,
) -> Result<CoercibleExpr> {
    let rel_desc = ecx.rel_desc.clone();
    let left = transform_expr(ecx, left)?;
    let right = transform_expr(ecx, right)?;
    match op {
        BinaryOperator::Plus => transform_bop_plus(ecx, left, right),
        BinaryOperator::Minus => transform_bop_minus(ecx, left, right),
        BinaryOperator::Gt => transform_bop_gt(ecx, left, right),
        BinaryOperator::Lt => transform_bop_lt(ecx, left, right),
        BinaryOperator::GtEq => transform_bop_gte(ecx, left, right),
        BinaryOperator::LtEq => transform_bop_lte(ecx, left, right),
        BinaryOperator::Eq => transform_bop_eq(ecx, left, right),
        BinaryOperator::NotEq => transform_bop_neq(ecx, left, right),
        BinaryOperator::And => transform_bop_and(ecx, left, right),
        BinaryOperator::Or => transform_bop_or(ecx, left, right),
        _ => Err(FloppyError::NotImplemented(format!(
            "binary op not implemented: {:?}",
            op
        ))),
    }
}

fn transform_parameter(ecx: &ExprContext, p: String) -> Result<CoercibleExpr> {
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
        Ok(Expr::Parameter(n).into())
    } else {
        Ok(CoercibleExpr::Parameter(n))
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
fn transform_bop_plus(
    ecx: &ExprContext,
    cexpr1: CoercibleExpr,
    cexpr2: CoercibleExpr,
) -> Result<CoercibleExpr> {
    let expr1 = cexpr1.type_as_any(ecx)?;
    let expr2 = cexpr2.type_as_any(ecx)?;

    let (expr1, expr2) = numeric_op_cast(ecx, expr1, expr2)?;
    add(ecx, &expr1, &expr2).map(|e| e.into())
}

fn transform_bop_minus(
    ecx: &ExprContext,
    left: CoercibleExpr,
    right: CoercibleExpr,
) -> Result<CoercibleExpr> {
    unimplemented!()
}

fn transform_bop_gt(
    ecx: &ExprContext,
    left: CoercibleExpr,
    right: CoercibleExpr,
) -> Result<CoercibleExpr> {
    let expr1 = left.type_as_any(ecx)?;
    let expr2 = right.type_as_any(ecx)?;

    let (expr1, expr2) = numeric_op_cast(ecx, expr1, expr2)?;
    gt(ecx, &expr1, &expr2).map(|e| e.into())
}

fn transform_bop_lt(
    ecx: &ExprContext,
    left: CoercibleExpr,
    right: CoercibleExpr,
) -> Result<CoercibleExpr> {
    unimplemented!()
}

fn transform_bop_gte(
    ecx: &ExprContext,
    left: CoercibleExpr,
    right: CoercibleExpr,
) -> Result<CoercibleExpr> {
    unimplemented!()
}

fn transform_bop_lte(
    ecx: &ExprContext,
    left: CoercibleExpr,
    right: CoercibleExpr,
) -> Result<CoercibleExpr> {
    unimplemented!()
}

fn transform_bop_eq(
    ecx: &ExprContext,
    left: CoercibleExpr,
    right: CoercibleExpr,
) -> Result<CoercibleExpr> {
    unimplemented!()
}

fn transform_bop_neq(
    ecx: &ExprContext,
    left: CoercibleExpr,
    right: CoercibleExpr,
) -> Result<CoercibleExpr> {
    unimplemented!()
}

fn transform_bop_and(
    ecx: &ExprContext,
    left: CoercibleExpr,
    right: CoercibleExpr,
) -> Result<CoercibleExpr> {
    unimplemented!()
}

fn transform_bop_or(
    ecx: &ExprContext,
    left: CoercibleExpr,
    right: CoercibleExpr,
) -> Result<CoercibleExpr> {
    unimplemented!()
}

fn numeric_op_cast(ecx: &ExprContext, expr1: Expr, expr2: Expr) -> Result<(Expr, Expr)> {
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
    use std::sync::Arc;
    use test_util::seed;
    fn logical_plan(scx: &StatementContext, sql: &str) -> Result<LogicalPlan> {
        let dialect = PostgreSqlDialect {};
        let ast = &Parser::parse_sql(&dialect, sql)?[0];
        match ast {
            Statement::Query(q) => transform_query(scx, q),
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
            catalog: Arc::new(catalog::memory::MemCatalog::default()),
            param_types: RefCell::default(),
            param_values: RefCell::default(),
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
        let catalog = seed::seed_catalog();
        let scx = StatementContext::new(Arc::new(catalog));
        let err =
            logical_plan(&scx, "SELECT * FROM faketable").expect_err("query is invalid");
        assert!(matches!(
            err,
            FloppyError::Catalog(CatalogError::TableNotFound(_))
        ));
    }

    #[test]
    fn select_column_not_exists() {
        let catalog = seed::seed_catalog();
        let scx = StatementContext::new(Arc::new(catalog));

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
        let catalog = seed::seed_catalog();
        let scx = StatementContext::new(Arc::new(catalog));

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
        let catalog = seed::seed_catalog();
        let scx = StatementContext::new(Arc::new(catalog));

        quick_test_eq(
            &scx,
            "SELECT c1 FROM test WHERE c2 > 100",
            "Projection: c1\
                   \n  Filter: c2 > Int32(100)\
                   \n    Table: test",
        );
    }
}
