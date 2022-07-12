use crate::catalog::{CatalogRef, SchemaProvider};
use crate::common::error::{field_not_found, FloppyError, Result};
use crate::common::operator::Operator;
use crate::common::schema::{Schema, SchemaRef};
use crate::common::value::Value;
use crate::logical_expr::column::Column;
use crate::logical_expr::expr::LogicalExpr;
use crate::logical_expr::expr_rewriter::normalize_col;
use crate::logical_expr::expr_visitor::{ExprVisitable, ExpressionVisitor, Recursion};
use crate::logical_expr::literal::lit;
use crate::logical_plan::plan::{
    EmptyRelation, Filter, LogicalPlan, Projection, TableScan,
};
use sqlparser::ast::{
    BinaryOperator, Expr as SQLExpr, Ident, Query, Select, SelectItem, SetExpr,
    Statement, TableFactor, TableWithJoins, Value as SQLValue,
};
use std::sync::Arc;

pub struct LogicalPlanner<'a, S: SchemaProvider> {
    schema_provider: &'a S,
}

impl<'a, S: SchemaProvider> LogicalPlanner<'a, S> {
    pub fn new(schema_provider: &'a S) -> Self {
        LogicalPlanner { schema_provider }
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
                    projected_schema: self.schema_provider.get_schema(&table_name)?,
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
                let filter_expr = self.sql_to_rex(predicate_expr, plan.schema())?;
                let filter_expr = normalize_col(filter_expr, &plan)?;
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
            .collect::<Result<Vec<LogicalExpr>>>()?;

        let schema = plan.schema().clone();
        Ok(LogicalPlan::Projection(Projection {
            expr: projection_exprs,
            input: Arc::new(plan.clone()),
            schema,
        }))
    }

    /// Validate the schema provides all of the columns referenced in the expressions.
    pub fn validate_schema_satisfies_exprs(
        &self,
        schema: &Schema,
        exprs: &[LogicalExpr],
    ) -> Result<()> {
        find_column_exprs(exprs)
            .iter()
            .try_for_each(|col| match col {
                LogicalExpr::Column(col) => match &col.relation {
                    Some(r) => {
                        schema.field_with_qualified_name(r, &col.name)?;
                        Ok(())
                    }
                    None => {
                        if !schema.fields_with_unqualified_name(&col.name).is_empty() {
                            Ok(())
                        } else {
                            Err(field_not_found(None, col.name.as_str(), schema))
                        }
                    }
                }
                .map_err(|_: FloppyError| {
                    field_not_found(
                        col.relation.as_ref().map(|s| s.to_owned()),
                        col.name.as_str(),
                        schema,
                    )
                }),
                _ => Err(FloppyError::Internal("Not a column".to_string())),
            })
    }

    pub fn sql_to_rex(&self, sql: SQLExpr, schema: &Schema) -> Result<LogicalExpr> {
        let mut expr = self.sql_expr_to_logical_expr(sql)?;
        self.validate_schema_satisfies_exprs(schema, &[expr.clone()])?;
        Ok(expr)
    }

    fn sql_expr_to_logical_expr(&self, sql: SQLExpr) -> Result<LogicalExpr> {
        match sql {
            SQLExpr::Value(SQLValue::Number(n, _)) => parse_sql_number(&n),
            SQLExpr::Value(SQLValue::SingleQuotedString(ref s)) => Ok(lit(s.clone())),
            SQLExpr::Value(SQLValue::Boolean(n)) => Ok(lit(n)),
            SQLExpr::Value(SQLValue::Null) => Ok(LogicalExpr::Literal(Value::Null)),
            SQLExpr::Identifier(identifier) => {
                if identifier.value.starts_with('@') {
                    return Err(FloppyError::NotImplemented(format!(
                        "Unsupported identifier starts with @"
                    )));
                }
                let col = Column {
                    relation: None,
                    name: normalize_ident(&identifier),
                };
                Ok(LogicalExpr::Column(col))
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
    ) -> Result<Vec<LogicalExpr>> {
        match project {
            SelectItem::UnnamedExpr(expr) => {
                let expr = self.sql_to_rex(expr, plan.schema())?;
                Ok(vec![normalize_col(expr, plan)?])
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
    ) -> Result<LogicalExpr> {
        let operator = match op {
            BinaryOperator::Plus => Ok(Operator::Plus),
            BinaryOperator::Minus => Ok(Operator::Minus),
            BinaryOperator::Eq => Ok(Operator::Eq),
            BinaryOperator::NotEq => Ok(Operator::NotEq),
            BinaryOperator::Lt => Ok(Operator::Lt),
            BinaryOperator::LtEq => Ok(Operator::LtEq),
            BinaryOperator::Gt => Ok(Operator::Gt),
            BinaryOperator::GtEq => Ok(Operator::GtEq),
            BinaryOperator::And => Ok(Operator::And),
            BinaryOperator::Or => Ok(Operator::Or),
            _ => Err(FloppyError::NotImplemented(format!(
                "Unsupported binary operator {:?}",
                op
            ))),
        }?;

        Ok(LogicalExpr::BinaryExpr {
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
fn parse_sql_number(n: &str) -> Result<LogicalExpr> {
    match n.parse::<i64>() {
        Ok(n) => Ok(lit(n)),
        _ => Err(FloppyError::Internal(format!(
            "unknown parser_sql_number error"
        ))),
    }
}

pub fn expand_wildcard(schema: &Schema, plan: &LogicalPlan) -> Result<Vec<LogicalExpr>> {
    Ok(schema
        .fields()
        .iter()
        .map(|f| {
            let col = f.qualified_column();
            LogicalExpr::Column(col)
        })
        .collect::<Vec<LogicalExpr>>())
}

/// Collect all deeply nested `Expr::Column`. They are returned
/// in the order of appearance (depth first), and my contain duplicates.
pub fn find_column_exprs(exprs: &[LogicalExpr]) -> Vec<LogicalExpr> {
    exprs
        .iter()
        .flat_map(find_columns_referenced_by_expr)
        .map(LogicalExpr::Column)
        .collect()
}

/// Recursively find all columns referenced by an expression
#[derive(Debug, Default)]
struct ColumnCollector {
    exprs: Vec<Column>,
}

impl ExpressionVisitor for ColumnCollector {
    fn pre_visit(mut self, expr: &LogicalExpr) -> Result<Recursion<Self>>
    where
        Self: ExpressionVisitor,
    {
        if let LogicalExpr::Column(c) = expr {
            self.exprs.push(c.clone())
        }
        Ok(Recursion::Continue(self))
    }
}

pub fn find_columns_referenced_by_expr(e: &LogicalExpr) -> Vec<Column> {
    let collector = e
        .accept(ColumnCollector::default())
        .expect("Unexpected error");
    collector.exprs
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::Catalog;
    use crate::common::error::SchemaError;
    use crate::common::schema::{DataType, Field};
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    struct MockSchemaProvider {}

    impl SchemaProvider for MockSchemaProvider {
        fn get_schema(&self, table_name: &str) -> Result<SchemaRef> {
            match table_name {
                "test" => Ok(Arc::new(Schema::new(vec![Field::new(
                    Some("test"),
                    "id",
                    DataType::Int32,
                    false,
                )]))),
                _ => Err(FloppyError::SchemaError(SchemaError::TableNotFound(
                    format!("table name not found {}", table_name),
                ))),
            }
        }
    }

    fn logical_plan(sql: &str) -> Result<LogicalPlan> {
        let mock_schema_provider = MockSchemaProvider {};
        let planner = LogicalPlanner::new(&mock_schema_provider);
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

        quick_test(
            "SELECT 1 + 1",
            "Projection: Int64(1) + Int64(1)\
                    \n  EmptyRelation",
        );

        quick_test(
            "SELECT 2, 3",
            "Projection: Int64(2), Int64(3)\
                    \n  EmptyRelation",
        );

        quick_test(
            "SELECT 2 + 4, 3",
            "Projection: Int64(2) + Int64(4), Int64(3)\
                    \n  EmptyRelation",
        )
    }

    #[test]
    fn select_table_not_exists() {
        let sql = "SELECT * FROM faketable";
        let err = logical_plan(sql).expect_err("query should have failed");
        match err {
            FloppyError::SchemaError(SchemaError::TableNotFound(_)) => (),
            _ => assert!(false, "err not match: {:?}", err),
        }
    }

    #[test]
    fn select_column_does_not_exist() {
        let sql = "SELECT fakecolumn FROM test";
        let err = logical_plan(sql).expect_err("query should have failed");
        match err {
            FloppyError::SchemaError(SchemaError::FieldNotFound {
                qualifier,
                name,
                valid_fields,
            }) => (),
            _ => assert!(false, "err not match: {:?}", err),
        }
    }

    #[test]
    fn select_table_that_exists() {
        let sql = "SELECT * FROM test";
        quick_test(
            sql,
            "Projection: #test.id\
                   \n  TableScan: test",
        );

        let sql = "SELECT id FROM test";
        quick_test(
            sql,
            "Projection: #test.id\
               \n  TableScan: test",
        )
    }

    #[test]
    fn select_filter() {
        let sql = "SELECT * FROM test WHERE id = 100";
        quick_test(
            sql,
            "Projection: #test.id\
                   \n  Filter: #test.id = Int64(100)\
                   \n    TableScan: test",
        )
    }

    #[test]
    fn select_projection() {}

    #[test]
    fn select_filter_projection() {}

    #[test]
    fn inner_join_two_tables() {}
}
