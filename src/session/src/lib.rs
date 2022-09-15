use common::error::Result;
use common::scalar::ScalarType;
use common::schema::{Params, StatementDesc};
use pgrepr;
use sqlparser::ast::Statement;
use std::collections::HashMap;
use txn_mgr::Transaction;

/// A session holds per-connection state.
#[derive(Debug)]
pub struct Session {
    conn_id: u32,
    prepared_statements: HashMap<String, PreparedStatement>,
    portals: HashMap<String, Portal>,
    txn_state: TransactionState,
}

impl Session {
    pub fn new(conn_id: u32) -> Session {
        Session {
            conn_id,
            txn_state: TransactionState::Default,
            prepared_statements: HashMap::new(),
            portals: HashMap::new(),
        }
    }

    pub fn declare_portal(
        &mut self,
        name: String,
        stmt: Statement,
        param_types: Vec<Option<ScalarType>>,
    ) -> Result<()> {
        todo!()
    }

    pub fn get_portal(&self, portal_name: &str) -> Option<&Portal> {
        self.portals.get(portal_name)
    }

    pub fn txn(&self) -> &TransactionState {
        &self.txn_state
    }

    pub fn is_aborted_txn(&self) -> bool {
        matches!(self.txn_state, TransactionState::Failed(_))
    }

    pub async fn start_txn(&mut self, num_stmts: Option<usize>) {
        todo!()
    }

    pub async fn commit_txn(&mut self) -> Result<()> {
        todo!()
    }

    pub async fn rollback_txn(&mut self) -> Result<()> {
        todo!()
    }

    pub fn fail_txn(&mut self) {
        match &self.txn_state {
            TransactionState::Default => {
                assert!(false)
            }
            TransactionState::Started(txn)
            | TransactionState::InTransactionImplicit(txn)
            | TransactionState::InTransaction(txn) => {
                self.txn_state = TransactionState::Failed(txn.clone());
            }
            TransactionState::Failed(_) => {}
        };
    }
}

/// A prepared statement.
#[derive(Debug)]
pub struct PreparedStatement {
    stmt: Option<Statement>,
    desc: StatementDesc,
}

/// A portal represents the execution state of a running or runnable query.
#[derive(Debug)]
pub struct Portal {
    /// The statement that is bound to this portal.
    pub stmt: Option<Statement>,
    /// The statement description
    pub desc: StatementDesc,
    /// The bound values for the parameters in the prepared statement, if any.
    pub bound_params: Params,
    /// The desired output format for each column in the result set.
    pub result_formats: Vec<pgrepr::Format>,
    /// The execution state of the portal.
    pub state: PortalState,
}

/// Execution states of a portal.
/// todo!
#[derive(Debug)]
pub enum PortalState {
    NotStarted,
    InProgress,
    Completed,
}

/// The transaction status of a session.
///
/// PostgreSQL's transaction states are in backend/access/transam/xact.c.
#[derive(Debug)]
pub enum TransactionState {
    /// Idle. Matches `TBLOCK_DEFAULT`.
    Default,
    /// Running a possibly single-query transaction. Matches
    /// `TBLOCK_STARTED`. WARNING: This might not actually be
    /// a single statement due to the extended protocol. Thus,
    /// we should not perform optimizations based on this.
    /// See: <https://git.postgresql.org/gitweb/?p=postgresql.git&a=commitdiff&h=f92944137>.
    Started(Transaction),
    /// Currently in a transaction issued from a `BEGIN`. Matches `TBLOCK_INPROGRESS`.
    InTransaction(Transaction),
    /// Currently in an implicit transaction started from a multi-statement query
    /// with more than 1 statements. Matches `TBLOCK_IMPLICIT_INPROGRESS`.
    InTransactionImplicit(Transaction),
    /// In a failed transaction that was started explicitly (i.e., previously
    /// InTransaction). We do not use Failed for implicit transactions because
    /// those cleanup after themselves. Matches `TBLOCK_ABORT`.
    Failed(Transaction),
}

impl TransactionState {
    /// Expresses whether or not the transaction was implicitly started.
    /// However, its negation does not imply explicitly started.
    pub fn is_implicit(&self) -> bool {
        match self {
            Self::Started(_) | Self::InTransactionImplicit(_) => true,
            Self::Default | Self::InTransaction(_) | Self::Failed(_) => false,
        }
    }
}
