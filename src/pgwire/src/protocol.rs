use crate::codec::FramedConn;
use crate::message::{
    BackendMessage, ErrorResponse, FrontendMessage,
};
use common::error::Result;
use postgres::error::SqlState;
use session::{Session, TransactionState};
use sqlparser::ast::Statement;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tracing::{debug, error, info, instrument, warn};

pub async fn run<A>(
    conn_id: u32,
    conn: &mut FramedConn<A>,
) -> Result<()>
where
    A: AsyncRead + AsyncWrite + Send + Sync + Unpin,
{
    // Construct session
    let mut session = Session::new(conn_id);

    let mut buf = vec![BackendMessage::AuthenticationOk];
    buf.push(BackendMessage::ReadyForQuery(
        session.txn_status().into(),
    ));
    conn.send_all(buf).await?;
    conn.flush().await?;

    let machine = StateMachine { conn, session };
    machine.run().await?;
    Ok(())
}

enum State {
    Ready,
    Drain,
    Done,
}

struct StateMachine<'a, A> {
    conn: &'a mut FramedConn<A>,
    session: Session,
}

impl<'a, A> StateMachine<'a, A>
where
    A: AsyncRead + AsyncWrite + Send + Sync + Unpin + 'a,
{
    async fn run(mut self) -> Result<()> {
        let mut state = State::Ready;
        loop {
            state = match state {
                State::Ready => {
                    self.advance_ready().await?
                }
                State::Drain => {
                    self.advance_drain().await?
                }
                State::Done => return Ok(()),
            }
        }
    }

    async fn advance_ready(&mut self) -> Result<State> {
        let message = self.conn.recv().await?;
        match message {
            Some(FrontendMessage::Query { sql }) => {
                self.query(sql).await?;
            }
            _ => {
                warn!("unimplemented: {:?}", message)
            }
        }
        Ok(State::Ready)
    }

    async fn advance_drain(&mut self) -> Result<State> {
        todo!()
    }

    async fn flush(&mut self) -> Result<State> {
        self.conn.flush().await?;
        Ok(State::Ready)
    }

    /// Sends a backend message to the client, after applying a severity filter.
    ///
    /// The message is only sent if its severity is above the severity set
    /// in the session, with the default value being NOTICE.
    async fn send<M>(&mut self, message: M) -> Result<()>
    where
        M: Into<BackendMessage>,
    {
        let message: BackendMessage = message.into();
        match message {
            BackendMessage::ErrorResponse(ref err) => {
                if err.severity.should_output_to_client() {
                    self.conn.send(message).await?
                }
            }
            _ => self.conn.send(message).await?,
        }
        Ok(())
    }

    async fn ready(&mut self) -> Result<State> {
        let txn_state = self.session.txn_status().into();
        self.send(BackendMessage::ReadyForQuery(txn_state))
            .await?;
        self.flush().await
    }

    async fn query(
        &mut self,
        sql: String,
    ) -> Result<State> {
        let stmts = match parse_sql(&sql) {
            Ok(stmts) => stmts,
            Err(err) => {
                self.error(err).await?;
                return self.ready().await;
            }
        };

        let num_stmts = stmts.len();

        for stmt in stmts {
            // In an aborted transaction, reject all commands except COMMIT/ROLLBACK.
            if self.is_aborted_txn()
                && !is_txn_exit_stmt(Some(&stmt))
            {
                self.abort_txn_error().await?;
                break;
            }

            // Start an implicit transaction if we aren't in any transaction and there's
            // more than one statement. This mirrors the `use_implicit_block` variable in
            // postgres.
            //
            // This needs to be done in the loop instead of once at the top because
            // a COMMIT/ROLLBACK statement needs to start a new transaction on next
            // statement.
            self.start_txn(Some(num_stmts)).await;

            match self.one_query(stmt).await? {
                State::Ready => (),
                State::Drain => break,
                State::Done => return Ok(State::Done),
            }
        }

        // Implicit transactions are closed at the end of a Query message.
        if self.session.txn_status().is_implicit() {
            self.commit_txn().await?;
        }

        if num_stmts == 0 {
            self.send(BackendMessage::EmptyQueryResponse)
                .await?;
        }
        self.ready().await
    }

    async fn one_query(
        &mut self,
        stmt: Statement,
    ) -> Result<State> {
        todo!()
    }

    async fn start_txn(
        &mut self,
        num_stmts: Option<usize>,
    ) -> Result<()> {
        todo!()
    }

    fn is_aborted_txn(&self) -> bool {
        todo!()
    }

    async fn abort_txn_error(&mut self) -> Result<()> {
        todo!()
    }

    async fn commit_txn(&mut self) -> Result<()> {
        todo!()
    }

    async fn rollback_txn(&mut self) -> Result<()> {
        todo!()
    }

    async fn error(
        &mut self,
        err: ErrorResponse,
    ) -> Result<State> {
        assert!(err.severity.is_error());
        let is_fatal = err.severity.is_fatal();
        self.send(BackendMessage::ErrorResponse(err))
            .await?;
        let txn_status = self.session.txn_status();
        match txn_status {
            // Error can be called from describe and parse and so might not be in an active
            // transaction.
            TransactionState::Default
            | TransactionState::Failed(_) => {}
            // In Started (i.e., a single statement), cleanup ourselves.
            TransactionState::Started(_) => {
                self.rollback_txn().await?;
            }
            // Implicit transactions also clear themselves.
            TransactionState::InTransactionImplicit(_) => {
                self.rollback_txn().await?;
            }
            // Explicit transactions move to failed.
            TransactionState::InTransaction(_) => {
                self.session.fail_txn();
            }
        };

        if is_fatal {
            Ok(State::Done)
        } else {
            Ok(State::Drain)
        }
    }
}

fn parse_sql(
    sql: &str,
) -> std::result::Result<Vec<Statement>, ErrorResponse> {
    let dialect = PostgreSqlDialect {};
    Parser::parse_sql(&dialect, sql).map_err(|e| {
        ErrorResponse::error(
            SqlState::SYNTAX_ERROR,
            e.to_string(),
        )
    })
}

fn is_txn_exit_stmt(stmt: Option<&Statement>) -> bool {
    match stmt {
        Some(stmt) => matches!(
            stmt,
            Statement::Commit { chain: _ }
                | Statement::Rollback { chain: _ }
        ),
        _ => false,
    }
}
