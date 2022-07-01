use crate::common::datatype::DataType;
use crate::logical_expr::column::Column;

pub struct Field {
    /// Optional qualifier (usually a table or relation name)
    qualifier: Option<String>,
    /// Field's name
    name: String,
    data_type: DataType,
    nullable: bool,
}

impl Field {
    pub fn new(name: &str, data_type: DataType, nullable: bool) -> Self {
        Field {
            name: name.to_string(),
            data_type,
            nullable,
            qualifier: None,
        }
    }

    /// Builds a qualified column based on self
    pub fn qualified_column(&self) -> Column {
        Column {
            relation: self.qualifier.clone(),
            name: self.name.clone(),
        }
    }
}
