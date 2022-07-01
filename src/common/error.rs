use std::result;
pub type Result<T> = result::Result<T, FloppyError>;

#[derive(Debug)]
pub enum FloppyError {
    NotImplemented(String),
    ParseError(String),
    Plan(String),
}
