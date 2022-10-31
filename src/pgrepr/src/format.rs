/// The encoding format for a [`Value`](crate::Value).
///
/// See the ["Formats and Format Codes"][pgdocs] section of
/// the PostgreSQL protocol documentation for details on the
/// available formats.
///
/// [pgdocs]:
/// https://www.postgresql.org/docs/current/protocol-overview.html#PROTOCOL-FORMAT-CODES
#[derive(Copy, Clone, Debug)]
pub enum Format {
    /// Text encoding.
    Text,
    /// Binary encoding.
    Binary,
}
