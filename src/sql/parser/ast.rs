use crate::sql::types::DataType;

#[derive(Debug, PartialEq)]
pub enum Statement {
    CreateTable { name: String, columns: Vec<Column> },
    Insert { table_name: String, columns: Vec<String>, values: Vec<Vec<Expression>> },
    Select { table_name: String, columns: Vec<String>, where_clause: Option<Expression>}
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

#[derive(Debug, PartialEq)]
pub enum Expression {
    Consts(Consts),
}

impl From<Consts> for Expression {
    fn from(consts: Consts) -> Self {
        Self::Consts(consts)
    }
}

#[derive(Debug, PartialEq)]
pub enum Consts {
    Null,
    String(String),
    Integer(i32),
    Float(f32),
    Boolean(bool),
}