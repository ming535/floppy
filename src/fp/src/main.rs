use common::error::Result;
use common::relation::Row;
use common::relation::{ColumnType, RelationDesc};
use common::scalar::{Datum, ScalarType};
use std::sync::Arc;
use storage::memory::MemoryEngine;
use test_util::seeder;
use tokio::net::TcpListener;
use tokio::signal;

#[tokio::main]
async fn main() -> Result<()> {
    // enable logging
    tracing_subscriber::fmt::try_init()?;

    let data: Vec<Row> = (0..100)
        .map(|n| Row::new(vec![Datum::Int32(n), Datum::Int32(n)]))
        .collect();
    let (catalog, table) = seeder::seed_catalog_and_table(&data)?;

    let shutdown = signal::ctrl_c();
    let listener = TcpListener::bind("127.0.0.1:6432").await?;
    pgwire::server::run(listener, shutdown).await;
    Ok(())
}
