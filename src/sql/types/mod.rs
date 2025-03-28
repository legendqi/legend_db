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
    Date(String),
    Time(String),
    DateTime(String),
    Binary(Vec<u8>),
    Array(Vec<Value>),
    Map(Vec<(Value, Value)>),
    Union(Vec<Value>),
    Json(String),
    Jsonb(String),
}

impl Value {
    
    pub fn from_expression(expr: Expression) -> Self {
        match expr { 
            Expression::Consts(Consts::Null) => Self::Null,
            Expression::Consts(Consts::Boolean(b)) => Self::Boolean(b),
            Expression::Consts(Consts::Integer(i)) => Self::Integer(i),
            Expression::Consts(Consts::Float(f)) => Self::Float(f),
            Expression::Consts(Consts::String(s)) => Self::String(s),
        }
    }
    
    // 获取数据类型
    pub fn get_type(&self) -> DataType {
        match self {
            Value::Null => DataType::Null,
            Value::Boolean(_) => DataType::Boolean,
            Value::Integer(_) => DataType::Integer,
            Value::Float(_) => DataType::Float,
            Value::String(_) => DataType::String,
            Value::Date(_) => DataType::Date,
            Value::Time(_) => DataType::Time,
            Value::DateTime(_) => DataType::DateTime,
            Value::Binary(_) => DataType::Binary,
            Value::Json(_) => DataType::String,
            Value::Jsonb(_) => DataType::String,
            _ => panic!("Invalid value type"),
        }
    }
    
}

pub type Row = Vec<Value>;