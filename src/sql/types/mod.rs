use std::cmp::Ordering;
use std::fmt::{Display, Formatter};
use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};
use crate::sql::parser::ast::{Consts, Expression};

#[derive(Serialize, Deserialize, Encode, Decode, Debug, PartialEq)]
pub enum DataType {
    Boolean,
    Integer,
    Float,
    String,
    Date,
    Time,
    DateTime,
    Binary,
    Array(Box<DataType>),
    Map(Box<DataType>, Box<DataType>),
    Union(Vec<DataType>),
    Null,
}

#[derive(Serialize, Deserialize, Encode, Decode,Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    String(String),
    // Date(String),
    // Time(String),
    // DateTime(String),
    // Binary(Vec<u8>),
    // Array(Vec<Value>),
    // Map(Vec<(Value, Value)>),
    // Union(Vec<Value>),
    // Json(String),
    // Jsonb(String),
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) { 
            (Value::Null, Value::Null) => Some(Ordering::Equal),
            (Value::Null, _) => Some(Ordering::Less),
            (_, Value::Null) => Some(Ordering::Greater),
            (Value::Boolean(a), Value::Boolean(b)) => a.partial_cmp(b),
            (Value::Integer(a), Value::Integer(b)) => a.partial_cmp(b),
            (Value::Integer(a), Value::Float(b)) => (*a as f64).partial_cmp(b),
            (Value::Float(a), Value::Float(b)) => a.partial_cmp(b),
            (Value::Float(a), Value::Integer(b)) => a.partial_cmp(&(*b as f64)),
            (Value::String(a), Value::String(b)) => a.partial_cmp(b),
            (_, _) => None,
        }
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Null => write!(f, "{}", "NULL"),
            Value::Boolean(b) if *b => write!(f, "{}", "TRUE"),
            Value::Boolean(_) => write!(f, "{}", "FALSE"),
            Value::Integer(v) => write!(f, "{}", v),
            Value::Float(v) => write!(f, "{}", v),
            Value::String(v) => write!(f, "{}", v),
        }
    }
}

impl Value {
    
    pub fn from_expression(expr: Expression) -> Self {
        match expr { 
            Expression::Consts(Consts::Null) => Self::Null,
            Expression::Consts(Consts::Boolean(b)) => Self::Boolean(b),
            Expression::Consts(Consts::Integer(i)) => Self::Integer(i),
            Expression::Consts(Consts::Float(f)) => Self::Float(f),
            Expression::Consts(Consts::String(s)) => Self::String(s),
            _ => unreachable!()
        }
    }
    
    // 获取数据类型
    pub fn get_type(&self) -> Option<DataType> {
        match self {
            Value::Null => None,
            Value::Boolean(_) => Some(DataType::Boolean),
            Value::Integer(_) => Some(DataType::Integer),
            Value::Float(_) => Some(DataType::Float),
            Value::String(_) => Some(DataType::String),
            // Value::Date(_) => Some(DataType::Date),
            // Value::Time(_) => Some(DataType::Time),
            // Value::DateTime(_) => Some(DataType::DateTime),
            // Value::Binary(_) => Some(DataType::Binary),
            // Value::Json(_) => Some(DataType::String),
            // Value::Jsonb(_) => Some(DataType::String),
            _ => None
        }
    }
    
}

pub type Row = Vec<Value>;