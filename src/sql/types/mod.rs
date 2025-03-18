#[derive(Debug, Clone, PartialEq)]
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

#[derive(Debug)]
pub enum  Value {
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