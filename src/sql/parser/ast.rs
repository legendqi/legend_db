use std::collections::BTreeMap;
use crate::sql::types::DataType;

#[derive(Debug, PartialEq)]
pub enum Statement {
    CreateTable { name: String, columns: Vec<Column> },
    CreateDatabase { database_name: String },
    Insert { table_name: String, columns: Option<Vec<String>>, values: Vec<Vec<Expression>> },
    Update { table_name: String, columns: BTreeMap<String, Expression>, where_clause: Option<BTreeMap<String, Expression>> },
    Delete { table_name: String, where_clause: Option<BTreeMap<String, Expression>> },
    // 别名可有可无
    Select { 
        columns: Vec<(Expression, Option<String>)>,
        from: FromItem,
        group_by: Option<Expression>,
        order_by: Vec<(String, OrderDirection)>,
        limit: Option<Expression>,
        offset: Option<Expression>
    },
    DropTable { table_name: String },
    DropDatabase { database_name: String },
    UseDatabase { database_name: String },
}

#[derive(Debug, PartialEq, Clone)]
pub enum FromItem {
    Table { name: String, alias: Option<String> },
    // SubQuery { query: Box<Statement> },
    Join {
        left: Box<FromItem>,
        right: Box<FromItem>,
        join_type: JoinType,
        predicate: Option<Expression>,
    },
}

#[derive(Debug, PartialEq, Clone)]
pub enum JoinType {
    Cross,
    Inner,
    Left,
    Right,
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

// join 的表达式，只有一种等于的情况
#[derive(Debug, PartialEq, Clone)]
pub enum Operation {
    Equal(Box<Expression>, Box<Expression>)
}

// 表达式
#[derive(Debug, PartialEq, Clone)]
pub enum Expression {
    Field(String),
    Consts(Consts),
    Operation(Operation),
    Function(String, String)
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