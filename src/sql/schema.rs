use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};
use crate::sql::types::{DataType, Value};

#[derive(Serialize, Deserialize, Encode, Decode, Debug, PartialEq)]

pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
}

#[derive(Serialize, Deserialize, Encode, Decode, Debug, PartialEq)]

pub struct Column {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub default_value: Option<Value>,
}