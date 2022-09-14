use common::error::Result;
use common::row::Row;
use common::scalar::{Datum, ScalarType};
use common::schema::{Field, Schema};
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
    let schema = Schema::new(vec![
        Field::new(
            Some(table_name),
            "c1",
            ScalarType::Int32,
            false,
        ),
        Field::new(
            Some(table_name),
            "c2",
            ScalarType::Int32,
            false,
        ),
    ]);

    let data: Vec<Row> = (0..100)
        .map(|n| {
            Row::new(vec![
                Datum::Int32(Some(n)),
                Datum::Int32(Some(n * 2)),
            ])
        })
        .collect();
    mem_engine.insert_schema(table_name, &schema);
    mem_engine.seed(table_name, data.iter());

    let shutdown = signal::ctrl_c();
    let listener =
        TcpListener::bind("127.0.0.1:6432").await?;
    pgwire::server::run(listener, shutdown).await;
    Ok(())
}
