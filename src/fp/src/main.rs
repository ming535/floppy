use common::error::Result;
use common::relation::Row;
use common::relation::{ColumnType, RelationDesc};
use common::scalar::{Datum, ScalarType};
use std::sync::Arc;
use storage::{memory::MemoryEngine, CatalogStore};
use tokio::net::TcpListener;
use tokio::signal;

pub mod session_ctx;

#[tokio::main]
async fn main() -> Result<()> {
    // enable logging
    tracing_subscriber::fmt::try_init()?;
    let table_name = "test";

    let mem_engine = Arc::new(MemoryEngine::default());
    let rel = RelationDesc::new(
        vec![
            ColumnType::new(ScalarType::Int32, false),
            ColumnType::new(ScalarType::Int32, false),
        ],
        vec!["c1".to_string(), "c2".to_string()],
    );

    let data: Vec<Row> = (0..100)
        .map(|n| Row::new(vec![Datum::Int32(Some(n)), Datum::Int32(Some(n * 2))]))
        .collect();
    mem_engine.insert_rel(table_name, &rel);
    mem_engine.seed(table_name, data.iter());

    let shutdown = signal::ctrl_c();
    let listener = TcpListener::bind("127.0.0.1:6432").await?;
    pgwire::server::run(listener, shutdown).await;
    Ok(())
}
