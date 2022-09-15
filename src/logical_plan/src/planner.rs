use crate::builder::LogicalPlanBuilder;
use crate::plan::LogicalPlan;
use common::error::{field_not_found, FloppyError, Result};
use common::operator::Operator;
use common::row::ColumnRef;
use common::scalar::Datum;
use common::schema::{RelationDesc, RelationDescRef};
use logical_expr::expr::LogicalExpr;
use logical_expr::expr_visitor::{
    ExprVisitable, ExpressionVisitor, Recursion,
};
use logical_expr::literal::lit;
use sqlparser::ast::{
    BinaryOperator, Expr as SQLExpr, Ident, Query, Select,
    SelectItem, SetExpr, Statement, TableFactor,
    TableWithJoins, Value as SQLValue,
};
use std::sync::Arc;
use storage::CatalogStore;

pub struct LogicalPlanner {
    catalog_store: Arc<dyn CatalogStore>,
    builder: LogicalPlanBuilder,
}

impl LogicalPlanner {
    pub fn new(
        catalog_store: Arc<dyn CatalogStore>,
    ) -> Self {
        LogicalPlanner {
            catalog_store,
            builder: LogicalPlanBuilder::default(),
        }
    }

    pub fn statement_to_plan(
        &self,
        statement: Statement,
    ) -> Result<LogicalPlan> {
        match statement {
            Statement::Query(query) => {
                self.query_to_plan(*query)
            }
            _ => Err(FloppyError::NotImplemented(format!(
                "Unsupported SQL statement: {:?}",
                statement
            ))),
        }
    }

    pub fn query_to_plan(
        &self,
        query: Query,
    ) -> Result<LogicalPlan> {
        // SELECT or UNION / EXCEPT / INTERSECT
        let set_expr = query.body;
        self.set_expr_to_plan(set_expr)
    }

    pub fn set_expr_to_plan(
        &self,
        set_expr: SetExpr,
    ) -> Result<LogicalPlan> {
        match set_expr {
            SetExpr::Select(select) => {
                self.select_to_plan(*select)
            }
            _ => Err(FloppyError::NotImplemented(format!(
                "Query {} not implemented yet",
                set_expr
            ))),
        }
    }

    pub fn select_to_plan(
        &self,
        select: Select,
    ) -> Result<LogicalPlan> {
        // process `from` clause
        // todo! a vec of LogicalPlan ?
        let builder = self.plan_from_tables(select.from)?;

        // process `where` clause
        let builder =
            self.plan_filter(select.selection, builder)?;

        let builder = self
            .plan_projection(select.projection, builder)?;

        builder.build()
    }

    pub fn plan_from_tables(
        &self,
        from: Vec<TableWithJoins>,
    ) -> Result<LogicalPlanBuilder> {
        if from.is_empty() {
            return Ok(LogicalPlanBuilder::empty_relation());
        }

        let table = &from[0];
        let relation = &table.relation;
        match relation {
            TableFactor::Table {
                name: ref sql_object_name,
                alias: _,
                ..
            } => {
                let table_name = sql_object_name
                    .0
                    .iter()
                    .map(normalize_ident)
                    .collect::<Vec<String>>()
                    .join(".");
                LogicalPlanBuilder::scan(
                    table_name.as_str(),
                    Arc::new(
                        self.catalog_store
                            .fetch_rel(&table_name)?,
                    ),
                    vec![],
                )
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
        builder: LogicalPlanBuilder,
    ) -> Result<LogicalPlanBuilder> {
        match selection {
            Some(predicate_expr) => {
                let filter_expr = self.sql_to_rex(
                    predicate_expr,
                    builder.plan()?.relation_desc(),
                )?;
                builder.filter(filter_expr)
            }
            None => Ok(builder),
        }
    }

    pub fn plan_projection(
        &self,
        projection: Vec<SelectItem>,
        builder: LogicalPlanBuilder,
    ) -> Result<LogicalPlanBuilder> {
        let input_is_empty = matches!(
            builder.plan()?,
            LogicalPlan::EmptyRelation(_)
        );
        let projection_exprs = projection
            .into_iter()
            .map(|expr| {
                self.sql_project_to_logical_expr(
                    expr,
                    builder.plan()?,
                    input_is_empty,
                )
            })
            .flat_map(|result| match result {
                Ok(vec) => {
                    vec.into_iter().map(Ok).collect()
                }
                Err(err) => vec![Err(err)],
            })
            .collect::<Result<Vec<LogicalExpr>>>()?;

        builder.project(projection_exprs)
    }

    //// Validate the schema provides all of the columns referenced in the expressions.
    // pub fn validate_schema_satisfies_exprs(
    //     &self,
    //     rel: &RelationDesc,
    //     exprs: &[LogicalExpr],
    // ) -> Result<()> {
    //     find_column_exprs(exprs).iter().try_for_each(
    //         |col| match col {
    //             LogicalExpr::Column(col) => match &col
    //                 .relation
    //             {
    //                 Some(r) => {
    //                     rel.field_with_qualified_name(
    //                         r, &col.name,
    //                     )?;
    //                     Ok(())
    //                 }
    //                 None => {
    //                     if !rel
    //                         .fields_with_unqualified_name(
    //                             &col.name,
    //                         )
    //                         .is_empty()
    //                     {
    //                         Ok(())
    //                     } else {
    //                         Err(field_not_found(
    //                             None,
    //                             col.name.as_str(),
    //                             rel,
    //                         ))
    //                     }
    //                 }
    //             }
    //             .map_err(|_: FloppyError| {
    //                 field_not_found(
    //                     col.relation
    //                         .as_ref()
    //                         .map(|s| s.to_owned()),
    //                     col.name.as_str(),
    //                     rel,
    //                 )
    //             }),
    //             _ => Err(FloppyError::Internal(
    //                 "Not a column".to_string(),
    //             )),
    //         },
    //     )
    // }

    pub fn sql_to_rex(
        &self,
        sql: SQLExpr,
        rel: &RelationDesc,
    ) -> Result<LogicalExpr> {
        let expr =
            self.sql_expr_to_logical_expr(sql, rel)?;
        // self.validate_schema_satisfies_exprs(
        //     schema,
        //     &[expr.clone()],
        // )?;
        Ok(expr)
    }

    fn sql_expr_to_logical_expr(
        &self,
        sql: SQLExpr,
        rel: &RelationDesc,
    ) -> Result<LogicalExpr> {
        match sql {
            SQLExpr::Value(SQLValue::Number(n, _)) => {
                parse_sql_number(&n)
            }
            SQLExpr::Value(
                SQLValue::SingleQuotedString(ref s),
            ) => Ok(lit(s.clone())),
            SQLExpr::Value(SQLValue::Boolean(n)) => {
                Ok(lit(n))
            }
            SQLExpr::Value(SQLValue::Null) => {
                Ok(LogicalExpr::Literal(Datum::Null))
            }
            SQLExpr::Identifier(identifier) => {
                if identifier.value.starts_with('@') {
                    return Err(FloppyError::NotImplemented("Unsupported identifier starts with @".to_string()));
                }
                let idx =
                    rel.column_idx(&identifier.value)?;
                Ok(LogicalExpr::Column(ColumnRef { idx }))
            }
            SQLExpr::BinaryOp { left, op, right } => self
                .parse_sql_binary_op(
                    *left, op, *right, rel,
                ),
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
                let expr = self.sql_to_rex(expr, plan.relation_desc())?;
                Ok(vec![expr])
            }
            SelectItem::ExprWithAlias { expr: _, alias: _ } => {
                Err(FloppyError::NotImplemented("Alias is not supported".to_string()))
            }
            SelectItem::Wildcard => {
                if input_is_empty {
                    return Err(FloppyError::Plan("SELECT * with no tables specified is not valid".to_string()));
                }
                expand_wildcard(plan.relation_desc(), plan)
            }
            SelectItem::QualifiedWildcard(ref _object_name) => {
                Err(FloppyError::NotImplemented("alias.* or schema.table.* is not supported".to_string()))
            }
        }
    }

    fn parse_sql_binary_op(
        &self,
        left: SQLExpr,
        op: BinaryOperator,
        right: SQLExpr,
        rel: &RelationDesc,
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
            left: Box::new(
                self.sql_expr_to_logical_expr(left, rel)?,
            ),
            op: operator,
            right: Box::new(
                self.sql_expr_to_logical_expr(right, rel)?,
            ),
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
        _ => Err(FloppyError::Internal(
            "unknown parser_sql_number error".to_string(),
        )),
    }
}

pub fn expand_wildcard(
    rel: &RelationDesc,
    _plan: &LogicalPlan,
) -> Result<Vec<LogicalExpr>> {
    Ok(rel
        .column_types()
        .iter()
        .enumerate()
        .map(|(idx, _)| {
            LogicalExpr::Column(ColumnRef { idx })
        })
        .collect::<Vec<LogicalExpr>>())
}

/// Collect all deeply nested `Expr::Column`. They are returned
/// in the order of appearance (depth first), and my contain duplicates.
pub fn find_column_exprs(
    exprs: &[LogicalExpr],
) -> Vec<LogicalExpr> {
    exprs
        .iter()
        .flat_map(find_columns_referenced_by_expr)
        .map(LogicalExpr::Column)
        .collect()
}

/// Recursively find all columns referenced by an expression
#[derive(Debug, Default)]
struct ColumnCollector {
    exprs: Vec<ColumnRef>,
}

impl ExpressionVisitor for ColumnCollector {
    fn pre_visit(
        mut self,
        expr: &LogicalExpr,
    ) -> Result<Recursion<Self>>
    where
        Self: ExpressionVisitor,
    {
        if let LogicalExpr::Column(c) = expr {
            self.exprs.push(c.clone())
        }
        Ok(Recursion::Continue(self))
    }
}

pub fn find_columns_referenced_by_expr(
    e: &LogicalExpr,
) -> Vec<ColumnRef> {
    let collector = e
        .accept(ColumnCollector::default())
        .expect("Unexpected error");
    collector.exprs
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::error::SchemaError;
    use common::scalar::ScalarType;
    use common::schema::ColumnType;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;
    use storage::memory::MemoryEngine;

    fn logical_plan(sql: &str) -> Result<LogicalPlan> {
        let mut mem_engine = MemoryEngine::default();
        let test_schema = RelationDesc::new(
            vec![ColumnType::new(ScalarType::Int32, false)],
            vec!["id".to_string()],
        );
        mem_engine.insert_rel("test", &test_schema)?;

        let planner =
            LogicalPlanner::new(Arc::new(mem_engine));
        let dialect = GenericDialect {};
        let ast = Parser::parse_sql(&dialect, sql);
        match ast {
            Ok(ast) => {
                planner.statement_to_plan(ast[0].clone())
            }
            Err(e) => Err(FloppyError::ParserError(e)),
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
        let err = logical_plan(sql)
            .expect_err("query should have failed");
        match err {
            FloppyError::SchemaError(
                SchemaError::TableNotFound(_),
            ) => (),
            _ => assert!(false, "err not match: {:?}", err),
        }
    }

    #[test]
    fn select_column_does_not_exist() {
        let sql = "SELECT fakecolumn FROM test";
        let err = logical_plan(sql)
            .expect_err("query should have failed");
        match err {
            FloppyError::SchemaError(
                SchemaError::FieldNotFound {
                    qualifier: _,
                    name: _,
                    valid_fields: _,
                },
            ) => (),
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
