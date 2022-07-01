use std::fmt;
use std::fmt::Formatter;

/// A named reference to a qualified field in a schema.
#[derive(Clone)]
pub struct Column {
    /// relation/table name.
    pub relation: Option<String>,
    /// field/column name.
    pub name: String,
}

impl fmt::Display for Column {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self.relation {
            Some(ref r) => write!(f, "#{}.{}", r, self.name),
            None => write!(f, "#{}", self.name),
        }
    }
}
