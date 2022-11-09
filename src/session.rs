use crate::catalog::CatalogStore;
use crate::common::error::Result;
use crate::common::relation::{Params, StatementDesc};
use crate::common::scalar::ScalarType;
use crate::sql::analyzer;
use crate::sql::context::StatementContext;
use crate::storage::TableStore;
use sqlparser::ast::Statement;
use std::collections::HashMap;
use std::sync::Arc;

/// A session to the database state.
#[derive(Debug)]
pub struct Session {
    conn_id: u32,
    catalog_store: Arc<dyn CatalogStore>,
    table_store: Arc<dyn TableStore>,
    prepared_statements: HashMap<String, PreparedStatement>,
}

impl Session {
    pub fn open() -> Result<Self> {
        todo!()
    }

    pub fn prepare(sql: &str) -> Result<PreparedStatement> {
        todo!()
    }

    pub fn execute(sql: &str) -> Result<()> {
        Ok(())
    }
}

/// A prepared statement.
#[derive(Debug)]
pub struct PreparedStatement {
    stmt: Option<Statement>,
    desc: StatementDesc,
}

/// The transaction status of a session.
///
/// PostgreSQL's transaction states are in
/// backend/access/transam/xact.c.
#[derive(Debug)]
pub enum TransactionState {
    /// Idle. Matches `TBLOCK_DEFAULT`.
    Default,
    /// Running a possibly single-query transaction. Matches
    /// `TBLOCK_STARTED`. WARNING: This might not actually
    /// be a single statement due to the extended
    /// protocol. Thus, we should not perform
    /// optimizations based on this. See: <https://git.postgresql.org/gitweb/?p=postgresql.git&a=commitdiff&h=f92944137>.
    Started(Transaction),
    /// Currently in a transaction issued from a `BEGIN`.
    /// Matches `TBLOCK_INPROGRESS`.
    InTransaction(Transaction),
    /// Currently in an implicit transaction started from a
    /// multi-statement query with more than 1
    /// statements. Matches `TBLOCK_IMPLICIT_INPROGRESS`.
    InTransactionImplicit(Transaction),
    /// In a failed transaction that was started explicitly
    /// (i.e., previously InTransaction). We do not use
    /// Failed for implicit transactions because
    /// those cleanup after themselves. Matches
    /// `TBLOCK_ABORT`.
    Failed(Transaction),
}

impl TransactionState {
    /// Expresses whether or not the transaction was
    /// implicitly started. However, its negation does
    /// not imply explicitly started.
    pub fn is_implicit(&self) -> bool {
        match self {
            Self::Started(_) | Self::InTransactionImplicit(_) => true,
            Self::Default | Self::InTransaction(_) | Self::Failed(_) => false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Transaction {}
