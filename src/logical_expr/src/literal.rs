//! Literal module contains foundational types that are used to represent literals.

use crate::expr::LogicalExpr;
use common::relation::ColumnType;
use common::scalar::{Datum, ScalarType};

/// Create a literal expression
pub fn lit<T: Literal>(n: T) -> LogicalExpr {
    n.lit()
}

pub trait Literal {
    /// Convert the value to a Literal expression
    fn lit(&self) -> LogicalExpr;
}

impl Literal for String {
    fn lit(&self) -> LogicalExpr {
        LogicalExpr::Literal(
            Datum::String(self.to_owned()),
            ColumnType {
                scalar_type: ScalarType::String,
                nullable: true,
            },
        )
    }
}

macro_rules! make_literal {
    ($TYPE:ty, $SCALAR:ident, $DOC: expr) => {
        #[doc = $DOC]
        impl Literal for $TYPE {
            fn lit(&self) -> LogicalExpr {
                LogicalExpr::Literal(Datum::$SCALAR(Some(self.clone())))
            }
        }
    };
}

make_literal!(bool, Boolean, "literal expression containing a bool");
make_literal!(i64, Int64, "literal expression containing an i64");
