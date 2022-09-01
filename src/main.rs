extern crate core;

mod common;
mod logical_expr;
mod logical_plan;
mod pgwire;
mod physical_expr;
mod physical_plan;
mod session_ctx;
mod storage;
mod store;

use crate::common::error::Result;
use crate::common::row::Row;
use crate::common::schema::{DataType, Field, Schema};
use crate::common::value::Value;
use crate::session_ctx::SessionContext;
use crate::storage::memory::MemoryEngine;
use crate::store::CatalogStore;
use postgres_protocol::message::backend;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::signal;

#[tokio::main]
async fn main() -> Result<()> {
    // enable logging
    tracing_subscriber::fmt::try_init()?;

    let table_name = "test";

    let mem_engine = Arc::new(MemoryEngine::default());
    let schema = Schema::new(vec![
        Field::new(
            Some(table_name),
            "a",
            DataType::Int32,
            false,
        ),
        Field::new(
            Some(table_name),
            "b",
            DataType::Int32,
            false,
        ),
    ]);

    let data: Vec<Row> = (0..100)
        .map(|n| {
            Row::new(vec![
                Value::Int32(Some(n)),
                Value::Int32(Some(n * 2)),
            ])
        })
        .collect();
    mem_engine.insert_schema(table_name, &schema);
    mem_engine.seed(table_name, data.iter());

    let session = SessionContext::new(
        mem_engine.clone(),
        mem_engine.clone(),
    );

    let shutdown = signal::ctrl_c();
    let listener =
        TcpListener::bind("127.0.0.1:6432").await?;
    pgwire::server::run(listener, shutdown).await;
    Ok(())
}
