//! Literal module contains foundational types that are used to represent literals.

use crate::logical_expr::expr::Expr;
use crate::logical_expr::value::Value;

/// Create a literal expression
pub fn lit<T: Literal>(n: T) -> Expr {
    n.lit()
}

pub trait Literal {
    /// Convert the value to a Literal expression
    fn lit(&self) -> Expr;
}

impl Literal for String {
    fn lit(&self) -> Expr {
        Expr::Literal(Value::Utf8(Some((*self).to_owned())))
    }
}

macro_rules! make_literal {
    ($TYPE:ty, $SCALAR:ident, $DOC: expr) => {
        #[doc = $DOC]
        impl Literal for $TYPE {
            fn lit(&self) -> Expr {
                Expr::Literal(Value::$SCALAR(Some(self.clone())))
            }
        }
    };
}

make_literal!(bool, Boolean, "literal expression containing a bool");
make_literal!(i64, Int64, "literal expression containing an i64");
make_literal!(f64, Float64, "literal expression containing an f64");
