use std::fmt;
use std::fmt::Formatter;

/// Represents the column at a given column index in a Tuple
#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub index: usize,
}

impl fmt::Display for Column {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{}", self.name, self.index)
    }
}
