use crate::empty::EmptyExec;
use crate::filter::FilterExec;
use crate::heap_scan::HeapScanExec;
use crate::plan::PhysicalPlan;
use crate::projection::ProjectionExec;
use common::error::Result;
use common::schema::RelationDesc;
use logical_expr::expr::LogicalExpr;
use logical_plan::plan::{
    Filter, LogicalPlan, Projection, TableScan,
};
use physical_expr::binary_expr::binary;
use physical_expr::column::Column;
use physical_expr::expr::PhysicalExpr;
use std::sync::Arc;
use storage::HeapStore;

pub struct PhysicalPlanner {
    heap_store: Arc<dyn HeapStore>,
}

impl PhysicalPlanner {
    pub fn new(heap_store: Arc<dyn HeapStore>) -> Self {
        Self {
            heap_store: heap_store.clone(),
        }
    }

    fn create_physical_expr(
        &self,
        expr: &LogicalExpr,
        rel: &RelationDesc,
    ) -> Result<Arc<PhysicalExpr>> {
        match expr {
            LogicalExpr::Column(c) => Ok(Arc::new(
                PhysicalExpr::Column(c.clone()),
            )),
            LogicalExpr::Literal(v) => Ok(Arc::new(
                PhysicalExpr::Literal(v.clone()),
            )),
            LogicalExpr::BinaryExpr { left, op, right } => {
                let lhs =
                    self.create_physical_expr(left, rel)?;
                let rhs =
                    self.create_physical_expr(right, rel)?;
                binary(lhs, *op, rhs, rel)
            }
        }
    }

    pub fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
    ) -> Result<PhysicalPlan> {
        match logical_plan {
            LogicalPlan::EmptyRelation(_empty) => Ok(
                PhysicalPlan::EmptyExec(EmptyExec::new()),
            ),
            LogicalPlan::TableScan(TableScan {
                table_name,
                projected_schema,
                filters,
            }) => {
                let physical_filters = filters
                    .iter()
                    .map(|e| {
                        self.create_physical_expr(
                            e,
                            projected_schema,
                        )
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(PhysicalPlan::HeapScanExec(
                    HeapScanExec::try_new(
                        self.heap_store.clone(),
                        table_name.clone(),
                        projected_schema.clone(),
                        physical_filters,
                    )?,
                ))
            }
            LogicalPlan::Projection(Projection {
                expr,
                input,
                schema,
            }) => {
                let exprs = expr
                    .iter()
                    .map(|e| {
                        self.create_physical_expr(e, schema)
                    })
                    .collect::<Result<Vec<_>>>()?;
                let input =
                    self.create_physical_plan(input)?;
                Ok(PhysicalPlan::ProjectionExec(
                    ProjectionExec {
                        expr: exprs,
                        input: Box::new(input),
                        rel: schema.clone(),
                    },
                ))
            }
            LogicalPlan::Filter(Filter {
                predicate,
                input,
            }) => {
                let expr = self.create_physical_expr(
                    predicate,
                    input.relation_desc(),
                )?;
                let input =
                    self.create_physical_plan(input)?;
                Ok(PhysicalPlan::FilterExec(FilterExec {
                    predicate: expr,
                    input: Box::new(input),
                }))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::operator::Operator;
    use common::row::ColumnRef;
    use common::row::Row;
    use common::scalar::{Datum, ScalarType};
    use common::schema::ColumnType;
    use logical_expr::literal::lit;
    use logical_plan::builder::LogicalPlanBuilder;
    use storage::memory::MemoryEngine;
    use storage::{CatalogStore, RowIter};

    fn seed_mem_engine(
        engine: &mut MemoryEngine,
        table_name: &str,
        schema: &RelationDesc,
        rows: &Vec<Row>,
    ) -> Result<()> {
        engine.insert_rel(table_name, schema)?;
        engine.seed(table_name, rows.iter())
    }

    #[tokio::test]
    async fn test_select_no_relation() -> Result<()> {
        let builder = LogicalPlanBuilder::empty_relation();
        let builder =
            builder.project(vec![lit(1), lit(2)])?;
        let logical_plan = builder.build()?;

        let mem_engine = MemoryEngine::default();
        let planner =
            PhysicalPlanner::new(Arc::new(mem_engine));
        let mut physical_plan =
            planner.create_physical_plan(&logical_plan)?;
        assert_eq!(format!("{}", physical_plan), 
                   "ProjectionExec: Literal(Int64(1)), Literal(Int64(2))\
                  \n  EmptyExec");
        let row = physical_plan.next()?;
        assert_eq!(row.is_some(), true);
        assert_eq!(
            row.unwrap(),
            Row::new(vec![
                Datum::Int64(Some(1)),
                Datum::Int64(Some(2))
            ])
        );

        let row = physical_plan.next()?;
        assert_eq!(row.is_none(), true);
        Ok(())
    }

    #[tokio::test]
    async fn test_simple_scan() -> Result<()> {
        let test_table_name = "test";
        let test_schema = RelationDesc::new(
            vec![ColumnType::new(ScalarType::Int32, false)],
            vec!["id".to_string()],
        );
        let data =
            vec![Row::new(vec![Datum::Int32(Some(1))])];

        let mut mem_engine = MemoryEngine::default();
        seed_mem_engine(
            &mut mem_engine,
            test_table_name,
            &test_schema,
            &data,
        );

        let logical_plan_builder =
            LogicalPlanBuilder::scan(
                test_table_name,
                Arc::new(
                    mem_engine
                        .fetch_rel(test_table_name)
                        .unwrap(),
                ),
                vec![],
            )?;

        let planner =
            PhysicalPlanner::new(Arc::new(mem_engine));
        let mut physical_plan = planner
            .create_physical_plan(
                &logical_plan_builder.build()?,
            )?;

        assert_eq!(
            format!("{}", physical_plan),
            "HeapScanExec: test"
        );

        let r = physical_plan.next()?;
        assert_eq!(r.is_some(), true);

        let r = r.unwrap();
        assert_eq!(
            r,
            Row::new(vec![Datum::Int32(Some(1))])
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_filter() -> Result<()> {
        let test_table_name = "test";
        let test_schema = RelationDesc::new(
            vec![ColumnType::new(ScalarType::Int32, false)],
            vec!["id".to_string()],
        );
        let data =
            vec![Row::new(vec![Datum::Int32(Some(1))])];

        let data: Vec<Row> = (0..100)
            .map(|n| Row::new(vec![Datum::Int32(Some(n))]))
            .collect();

        let mut mem_engine = MemoryEngine::default();
        seed_mem_engine(
            &mut mem_engine,
            test_table_name,
            &test_schema,
            &data,
        );

        let builder = LogicalPlanBuilder::scan(
            test_table_name,
            Arc::new(
                mem_engine
                    .fetch_rel(test_table_name)
                    .unwrap(),
            ),
            vec![],
        )?
        .filter(LogicalExpr::BinaryExpr {
            left: Box::new(LogicalExpr::Column(
                ColumnRef { idx: 0 },
            )),
            op: Operator::Eq,
            right: Box::new(lit(50)),
        })?;

        let planner =
            PhysicalPlanner::new(Arc::new(mem_engine));
        let mut physical_plan = planner
            .create_physical_plan(&builder.build()?)?;

        let r = physical_plan.next()?;
        assert_eq!(
            r,
            Some(Row::new(vec![Datum::Int32(Some(50))]))
        );

        let r = physical_plan.next()?;
        assert_eq!(r, None);
        Ok(())
    }
}
