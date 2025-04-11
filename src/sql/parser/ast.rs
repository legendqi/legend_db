use std::collections::BTreeMap;
use crate::sql::types::DataType;

#[derive(Debug, PartialEq)]
pub enum Statement {
    CreateTable { name: String, columns: Vec<Column> },
    CreateDatabase { database_name: String },
    Insert { table_name: String, columns: Option<Vec<String>>, values: Vec<Vec<Expression>> },
    Update { table_name: String, columns: BTreeMap<String, Expression>, where_clause: Option<BTreeMap<String, Expression>> },
    Delete { table_name: String, where_clause: Option<BTreeMap<String, Expression>> },
    // Select { table_name: String, column: Column, order_by: Vec<(String, OrderDirection)> },
    Select { table_name: String, order_by: Vec<(String, OrderDirection)> },
    DropTable { table_name: String },
    DropDatabase { database_name: String },
}

#[derive(Debug, PartialEq)]
pub enum OrderDirection {
    Asc,
    Desc,
}

#[derive(Debug, PartialEq)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
    pub nullable: Option<bool>,
    pub default: Option<Expression>,
    pub is_primary_key: bool,
    pub auto_increment: bool,
    pub unique: bool,
}

#[derive(Debug, PartialEq, Clone)]
pub enum Expression {
    Consts(Consts),
}

impl From<Consts> for Expression {
    fn from(consts: Consts) -> Self {
        Self::Consts(consts)
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum Consts {
    Null,
    String(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
}