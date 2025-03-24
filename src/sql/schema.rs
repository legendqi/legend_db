use rkyv::{Archive, Deserialize, Serialize};
use crate::sql::types::{DataType, Value};

#[derive(Debug, PartialEq, Archive, Serialize, Deserialize)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
}

#[derive(Debug, PartialEq, Archive, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub default_value: Option<Value>,
}