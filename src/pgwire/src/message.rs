// Pgwire protocol versions are represented as 32-bit integers, where the
// high 16 bits represent the major version and the low 16 bits represent the
// minor version.
//
// There have only been three released protocol versions, v1.0, v2.0, and v3.0.
// The protocol changes very infrequently: the most recent protocol version,
// v3.0, was released with Postgres v7.4 in 2003.
//
// Somewhat unfortunately, the protocol overloads the version field to indicate
// special types of connections, namely, SSL connections and cancellation
// connections. These pseudo-versions were constructed to avoid ever matching
// a true protocol version.

use std::collections::HashMap;

pub const VERSION_1: i32 = 0x10000;
pub const VERSION_2: i32 = 0x20000;
pub const VERSION_3: i32 = 0x30000;
pub const VERSION_CANCEL: i32 = (1234 << 16) + 5678;
pub const VERSION_SSL: i32 = (1234 << 16) + 5679;
pub const VERSION_GSSENC: i32 = (1234 << 16) + 5680;

pub const VERSIONS: &[i32] = &[
    VERSION_1,
    VERSION_2,
    VERSION_3,
    VERSION_CANCEL,
    VERSION_SSL,
    VERSION_GSSENC,
];
use postgres::error::SqlState;
use session::TransactionState;

/// Like [`FrontendMessage`], but only the messages that can occur during
/// startup protocol negotiation.
#[derive(Debug)]
pub enum FrontendStartupMessage {
    Startup {
        version: i32,
        params: HashMap<String, String>,
    },

    /// Request SSL encryption for the connection.
    SslRequest,

    /// Request GSSAPI encryption for the connection.
    GssEncRequest,

    /// Cancel a query that is running on another connection.
    CancelRequest {
        /// The target connection ID.
        conn_id: u32,
        /// The secret key for the target connection.
        secret_key: u32,
    },
}

/// A decoded frontend pgwire [message], representing instructions for the
/// backend.
///
/// [message]: https://www.postgresql.org/docs/11/protocol-message-formats.html
#[derive(Debug)]
pub enum FrontendMessage {
    /// Execute the specified SQL.
    ///
    /// This is issued as part of the simple query flow.
    Query {
        /// The SQL to execute.
        sql: String,
    },
}

/// Internal representation of a backend [message]
///
/// [message]: https://www.postgresql.org/docs/11/protocol-message-formats.html
pub enum BackendMessage {
    AuthenticationOk,
    EmptyQueryResponse,
    ReadyForQuery(TransactionStatus),
    ErrorResponse(ErrorResponse),
}

pub enum TransactionStatus {
    /// Not currently in a transaction
    Idle,
    /// Currently in a transaction
    InTransaction,
    /// Currently in a transaction block which is failed
    Failed,
}

impl From<&TransactionState> for TransactionStatus {
    fn from(state: &TransactionState) -> Self {
        match state {
            TransactionState::Default => Self::Idle,
            TransactionState::Started(_) => {
                Self::InTransaction
            }
            TransactionState::InTransaction(_) => {
                Self::InTransaction
            }
            TransactionState::InTransactionImplicit(_) => {
                Self::InTransaction
            }
            TransactionState::Failed(_) => Self::Failed,
        }
    }
}

#[derive(Debug)]
pub struct ErrorResponse {
    pub severity: Severity,
    pub code: SqlState,
    pub message: String,
    pub detail: Option<String>,
    pub hint: Option<String>,
    pub position: Option<usize>,
}

impl ErrorResponse {
    pub fn error<S>(
        code: SqlState,
        message: S,
    ) -> ErrorResponse
    where
        S: Into<String>,
    {
        ErrorResponse::new(Severity::Error, code, message)
    }

    fn new<S>(
        severity: Severity,
        code: SqlState,
        message: S,
    ) -> ErrorResponse
    where
        S: Into<String>,
    {
        ErrorResponse {
            severity,
            code,
            message: message.into(),
            detail: None,
            hint: None,
            position: None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Severity {
    Panic,
    Fatal,
    Error,
    Warning,
    Notice,
    Debug,
    Info,
    Log,
}

impl Severity {
    pub fn is_error(&self) -> bool {
        matches!(
            self,
            Severity::Panic
                | Severity::Fatal
                | Severity::Error
        )
    }

    pub fn is_fatal(&self) -> bool {
        matches!(self, Severity::Fatal)
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Error => "ERROR",
            Self::Fatal => "FATAL",
            Self::Panic => "PANIC",
            Self::Warning => "WARNING",
            Self::Notice => "NOTICE",
            Self::Debug => "DEBUG",
            Self::Info => "INFO",
            Self::Log => "LOG",
        }
    }

    pub fn should_output_to_client(&self) -> bool {
        match self {
            Self::Debug => false,
            _ => true,
        }
    }
}
