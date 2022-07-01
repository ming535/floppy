use crate::catalog::CatalogRef;
use crate::common::error::{FloppyError, Result};
use crate::common::schema::{Schema, SchemaRef};
use crate::logical_expr::column::Column;
use crate::logical_expr::expr::Expr;
use crate::logical_expr::literal::lit;
use crate::logical_expr::value::Value;
use crate::logical_plan::operator::Operator;
use crate::logical_plan::plan::{
    EmptyRelation, Filter, LogicalPlan, Projection, TableScan,
};
use sqlparser::ast::{
    BinaryOperator, Expr as SQLExpr, Ident, Query, Select, SelectItem, SetExpr,
    Statement, TableFactor, TableWithJoins, Value as SQLValue,
};
use std::sync::Arc;

pub struct LogicalPlanner {
    catalog: CatalogRef,
}

impl LogicalPlanner {
    pub fn new(catalog: CatalogRef) -> Self {
        LogicalPlanner { catalog }
    }

    pub fn statement_to_plan(&self, statement: Statement) -> Result<LogicalPlan> {
        match statement {
            Statement::Query(query) => self.query_to_plan(*query),
            _ => Err(FloppyError::NotImplemented(format!(
                "Unsupported SQL statement: {:?}",
                statement
            ))),
        }
    }

    pub fn query_to_plan(&self, query: Query) -> Result<LogicalPlan> {
        // SELECT or UNION / EXCEPT / INTERSECT
        let set_expr = query.body;
        self.set_expr_to_plan(set_expr)
    }

    pub fn set_expr_to_plan(&self, set_expr: SetExpr) -> Result<LogicalPlan> {
        match set_expr {
            SetExpr::Select(select) => self.select_to_plan(*select),
            _ => Err(FloppyError::NotImplemented(format!(
                "Query {} not implemented yet",
                set_expr
            ))),
        }
    }

    pub fn select_to_plan(&self, select: Select) -> Result<LogicalPlan> {
        // process `from` clause
        // todo! a vec of LogicalPlan ?
        let plan = self.plan_from_tables(select.from)?;

        // process `where` clause
        let plan = self.plan_filter(select.selection, plan)?;

        let plan = self.plan_projection(select.projection, plan)?;

        Ok(plan)
    }

    pub fn plan_from_tables(&self, from: Vec<TableWithJoins>) -> Result<LogicalPlan> {
        if from.is_empty() {
            return Ok(LogicalPlan::EmptyRelation(EmptyRelation {
                schema: SchemaRef::new(Schema::empty()),
            }));
        }

        let table = &from[0];
        let relation = &table.relation;
        match relation {
            TableFactor::Table {
                name: ref sql_object_name,
                alias,
                ..
            } => {
                let table_name = sql_object_name
                    .0
                    .iter()
                    .map(normalize_ident)
                    .collect::<Vec<String>>()
                    .join(".");
                let table_scan = LogicalPlan::TableScan(TableScan {
                    table_name: table_name.clone(),
                    schema: self.catalog.get_schema(&table_name)?,
                    filters: vec![],
                });
                Ok(table_scan)
            }
            _ => Err(FloppyError::NotImplemented(format!(
                "Relation {} not implemented yet",
                relation
            ))),
        }
    }

    pub fn plan_filter(
        &self,
        selection: Option<SQLExpr>,
        plan: LogicalPlan,
    ) -> Result<LogicalPlan> {
        match selection {
            Some(predicate_expr) => {
                let filter_expr = self.sql_expr_to_logical_expr(predicate_expr)?;
                Ok(LogicalPlan::Filter(Filter {
                    predicate: filter_expr,
                    input: Arc::new(plan),
                }))
            }
            None => Ok(plan),
        }
    }

    pub fn plan_projection(
        &self,
        projection: Vec<SelectItem>,
        plan: LogicalPlan,
    ) -> Result<LogicalPlan> {
        let input_is_empty = matches!(plan, LogicalPlan::EmptyRelation(_));
        let projection_exprs = projection
            .into_iter()
            .map(|expr| self.sql_project_to_logical_expr(expr, &plan, input_is_empty))
            .flat_map(|result| match result {
                Ok(vec) => vec.into_iter().map(Ok).collect(),
                Err(err) => vec![Err(err)],
            })
            .collect::<Result<Vec<Expr>>>()?;

        let schema = plan.schema().clone();
        Ok(LogicalPlan::Projection(Projection {
            expr: projection_exprs,
            input: Arc::new(plan.clone()),
            schema,
        }))
    }

    pub fn sql_expr_to_logical_expr(&self, sql: SQLExpr) -> Result<Expr> {
        match sql {
            SQLExpr::Value(SQLValue::Number(n, _)) => parse_sql_number(&n),
            SQLExpr::Value(SQLValue::SingleQuotedString(ref s)) => Ok(lit(s.clone())),
            SQLExpr::Value(SQLValue::Boolean(n)) => Ok(lit(n)),
            SQLExpr::Value(SQLValue::Null) => Ok(Expr::Literal(Value::Null)),
            SQLExpr::Identifier(identifier) => {
                if identifier.value.starts_with('@') {
                    return Err(FloppyError::NotImplemented(format!(
                        "Unsupported identifier starts with @"
                    )));
                }
                Ok(Expr::Column(Column {
                    relation: None,
                    name: normalize_ident(&identifier),
                }))
            }
            SQLExpr::BinaryOp { left, op, right } => {
                self.parse_sql_binary_op(*left, op, *right)
            }
            _ => Err(FloppyError::NotImplemented(format!(
                "Unsupported expression {:?}",
                sql
            ))),
        }
    }

    pub fn sql_project_to_logical_expr(
        &self,
        project: SelectItem,
        plan: &LogicalPlan,
        input_is_empty: bool,
    ) -> Result<Vec<Expr>> {
        match project {
            SelectItem::UnnamedExpr(expr) => {
                let expr = self.sql_expr_to_logical_expr(expr)?;
                // todo normalize column?
                Ok(vec![expr])
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                return Err(FloppyError::NotImplemented(format!(
                    "Alias is not supported"
                )));
            }
            SelectItem::Wildcard => {
                if input_is_empty {
                    return Err(FloppyError::Plan(format!(
                        "SELECT * with no tables specified is not valid"
                    )));
                }
                expand_wildcard(plan.schema(), plan)
            }
            SelectItem::QualifiedWildcard(ref object_name) => {
                return Err(FloppyError::NotImplemented(format!(
                    "alias.* or schema.table.* is not supported"
                )))
            }
        }
    }

    fn parse_sql_binary_op(
        &self,
        left: SQLExpr,
        op: BinaryOperator,
        right: SQLExpr,
    ) -> Result<Expr> {
        let operator = match op {
            BinaryOperator::Eq => Ok(Operator::Eq),
            _ => Err(FloppyError::NotImplemented(format!(
                "Unsupported binary operator {:?}",
                op
            ))),
        }?;

        Ok(Expr::BinaryExpr {
            left: Box::new(self.sql_expr_to_logical_expr(left)?),
            op: operator,
            right: Box::new(self.sql_expr_to_logical_expr(right)?),
        })
    }
}

pub fn normalize_ident(ident: &Ident) -> String {
    match ident.quote_style {
        Some(_) => ident.value.clone(),
        None => ident.value.to_ascii_lowercase(),
    }
}

// Parse number in sql string, convert it to Expr::Literal
fn parse_sql_number(n: &str) -> Result<Expr> {
    match n.parse::<i64>() {
        Ok(n) => Ok(lit(n)),
        Err(_) => Ok(lit(n.parse::<f64>().unwrap())),
    }
}

pub fn expand_wildcard(schema: &Schema, plan: &LogicalPlan) -> Result<Vec<Expr>> {
    Ok(schema
        .fields()
        .iter()
        .map(|f| {
            let col = f.qualified_column();
            Expr::Column(col)
        })
        .collect::<Vec<Expr>>())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::Catalog;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    fn logical_plan(sql: &str) -> Result<LogicalPlan> {
        let planner = LogicalPlanner::new(Arc::new(Catalog::empty()));
        let dialect = GenericDialect {};
        let ast = Parser::parse_sql(&dialect, sql);
        match ast {
            Ok(ast) => planner.statement_to_plan(ast[0].clone()),
            Err(e) => return Err(FloppyError::ParseError(e.to_string())),
        }
    }

    /// Create logical plan, write with formatter and compare the string
    fn quick_test(sql: &str, expected: &str) {
        let plan = logical_plan(sql).unwrap();
        assert_eq!(format!("{:?}", plan), expected)
    }

    #[test]
    fn select_no_relation_single_column() {
        quick_test(
            "SELECT 1",
            "Projection: Int64(1)\
                   \n  EmptyRelation",
        );
    }

    #[test]
    fn select_no_relation_multiple_column() {}

    #[test]
    fn select_table_that_exists() {}

    #[test]
    fn select_table_not_exists() {}

    #[test]
    fn select_filter() {}

    #[test]
    fn select_projection() {}

    #[test]
    fn select_filter_projection() {}

    #[test]
    fn inner_join_two_tables() {}
}
