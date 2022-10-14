use crate::context::ExprContext;
use crate::prim::expr::Expr;
use common::error::FloppyError;
use common::error::Result;
use common::relation::ColumnType;
use common::scalar::ScalarType;
use std::fmt;
use std::fmt::Formatter;
#[derive(Debug, Clone)]
pub struct BinaryExpr {
    pub func: BinaryFunc,
    pub expr1: Box<Expr>,
    pub expr2: Box<Expr>,
}

impl fmt::Display for BinaryExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{} {} {}", self.expr1, self.func, self.expr2)
    }
}

impl BinaryExpr {
    pub fn typ(&self) -> ColumnType {
        let scalar_type = match self.func {
            BinaryFunc::AddInt16 => ScalarType::Int16,
            BinaryFunc::AddInt32 => ScalarType::Int32,
            BinaryFunc::AddInt64 => ScalarType::Int64,
            BinaryFunc::SubInt16 => ScalarType::Int16,
            BinaryFunc::SubInt32 => ScalarType::Int32,
            BinaryFunc::SubInt64 => ScalarType::Int64,
            BinaryFunc::Eq => ScalarType::Boolean,
            BinaryFunc::NotEq => ScalarType::Boolean,
            BinaryFunc::Lt => ScalarType::Boolean,
            BinaryFunc::Lte => ScalarType::Boolean,
            BinaryFunc::Gt => ScalarType::Boolean,
            BinaryFunc::Gte => ScalarType::Boolean,
        };
        ColumnType {
            scalar_type,
            nullable: false,
        }
    }
}

#[derive(Debug, Clone)]
pub enum BinaryFunc {
    AddInt16,
    AddInt32,
    AddInt64,
    SubInt16,
    SubInt32,
    SubInt64,
    Eq,
    NotEq,
    Lt,
    Lte,
    Gt,
    Gte,
}

impl fmt::Display for BinaryFunc {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::AddInt16 | Self::AddInt32 | Self::AddInt64 => write!(f, "+"),
            Self::SubInt16 | Self::SubInt32 | Self::SubInt64 => write!(f, "-"),
            Self::Eq => write!(f, "="),
            Self::NotEq => write!(f, "!="),
            Self::Lt => write!(f, "<"),
            Self::Lte => write!(f, "<="),
            Self::Gt => write!(f, ">"),
            Self::Gte => write!(f, ">="),
        }
    }
}

pub fn add(ecx: &ExprContext, expr1: &Expr, expr2: &Expr) -> Result<Expr> {
    let ty1 = expr1.typ(ecx).scalar_type;
    let ty2 = expr2.typ(ecx).scalar_type;

    if ty1 != ty2 {
        return Err(FloppyError::Internal(format!(
            "add two different type, expr1: {}, expr2: {}",
            ty1, ty2
        )));
    }

    let f = match ty1 {
        ScalarType::Int16 => BinaryFunc::AddInt16,
        ScalarType::Int32 => BinaryFunc::AddInt32,
        ScalarType::Int64 => BinaryFunc::AddInt64,
        _ => {
            return Err(FloppyError::Internal(format!(
                "add only supports numeric types: {}",
                ty1
            )))
        }
    };

    Ok(Expr::CallBinary(BinaryExpr {
        func: f,
        expr1: Box::new(expr1.clone()),
        expr2: Box::new(expr2.clone()),
    }))
}

pub fn gt(ecx: &ExprContext, expr1: &Expr, expr2: &Expr) -> Result<Expr> {
    let ty1 = expr1.typ(ecx).scalar_type;
    let ty2 = expr2.typ(ecx).scalar_type;

    if ty1 != ty2 {
        return Err(FloppyError::Internal(format!(
            "compare two different type, expr1: {}, expr2: {}",
            ty1, ty2
        )));
    }

    Ok(Expr::CallBinary(BinaryExpr {
        func: BinaryFunc::Gt,
        expr1: Box::new(expr1.clone()),
        expr2: Box::new(expr2.clone()),
    }))
}

#[derive(Debug, Clone)]
pub struct VariadicExpr {
    func: VariadicFunc,
    exprs: Vec<Expr>,
}

impl fmt::Display for VariadicExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self.func {
            VariadicFunc::And | VariadicFunc::Or => {
                let exprs = self
                    .exprs
                    .iter()
                    .map(|e| format!("{}", e))
                    .collect::<Vec<String>>();
                write!(f, "{}", exprs.join(format!("{}", self.func).as_str()))
            }
        }
    }
}

impl VariadicExpr {
    pub fn typ(&self) -> ColumnType {
        // we only support `AND`, `OR` function right now.
        ColumnType {
            scalar_type: ScalarType::Boolean,
            nullable: false,
        }
    }
}

#[derive(Debug, Clone)]
pub enum VariadicFunc {
    And,
    Or,
}

impl fmt::Display for VariadicFunc {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::And => write!(f, "AND"),
            Self::Or => write!(f, "OR"),
        }
    }
}
