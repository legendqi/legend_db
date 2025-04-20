use std::collections::BTreeMap;
use crate::custom_error::{LegendDBError, LegendDBResult};
use crate::sql::types::{DataType, Value};

#[derive(Debug, PartialEq)]
pub enum Statement {
    CreateTable { name: String, columns: Vec<Column> },
    CreateDatabase { database_name: String },
    Insert { table_name: String, columns: Option<Vec<String>>, values: Vec<Vec<Expression>> },
    Update { table_name: String, columns: BTreeMap<String, Expression>, where_clause: Option<Vec<Expression>> },
    Delete { table_name: String, where_clause: Option<Vec<Expression>> },
    // 别名可有可无
    Select { 
        columns: Vec<(Expression, Option<String>)>,
        from: FromItem,
        where_clause: Option<Vec<Expression>>,
        group_by: Option<Expression>,
        having: Option<Expression>,
        order_by: Vec<(String, OrderDirection)>,
        limit: Option<Expression>,
        offset: Option<Expression>
    },
    DropTable { table_name: String },
    DropDatabase { database_name: String },
    UseDatabase { database_name: String },
    // ShowDatabases {},
    // ShowTables { },
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
    Equal(Box<Expression>, Box<Expression>),
    NotEqual(Box<Expression>, Box<Expression>),
    GreaterThan(Box<Expression>, Box<Expression>),
    LessThan(Box<Expression>, Box<Expression>),
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

pub fn evaluate_expr(expression: &Expression, left_col: &Vec<String>, left_row: &Vec<Value>, right_col: &Vec<String>, right_row: &Vec<Value>) -> LegendDBResult<Value> {
    match expression {
        // 查询哪些列
        Expression::Field(col_name) => {
            let pos = left_col.iter().position(|x| *x == *col_name).ok_or(LegendDBError::Internal(format!("Column {} not found", col_name)));
            Ok(left_row[pos?].clone())
        },
        // 常量
        Expression::Consts(consts) => Ok(match consts {
            Consts::Null => Value::Null,
            Consts::String(s) => Value::String(s.clone()),
            Consts::Integer(i) => Value::Integer(*i),
            Consts::Float(f) => Value::Float(*f),
            Consts::Boolean(b) => Value::Boolean(*b),
        }),
        // 操作符
        Expression::Operation(Operation::Equal(left, right)) => {
            let left_val = evaluate_expr(left, left_col, left_row, right_col, right_row)?;
            let right_val = evaluate_expr(right, right_col, right_row, left_col, left_row)?;
            match (left_val, right_val) {
                (Value::Integer(left_val), Value::Integer(right_val)) => Ok(Value::Boolean(left_val == right_val)),
                (Value::Boolean(left_val), Value::Boolean(right_val)) => Ok(Value::Boolean(left_val == right_val)),
                (Value::Float(left_val), Value::Float(right_val)) => Ok(Value::Boolean(left_val == right_val)),
                (Value::Integer(left_val), Value::Float(right_val)) => Ok(Value::Boolean(left_val as f64 == right_val)),
                (Value::Float(left_val), Value::Integer(right_val)) => Ok(Value::Boolean(left_val == right_val as f64)),
                (Value::String(left_val), Value::String(right_val)) => Ok(Value::Boolean(left_val == right_val)),
                (Value::Null, _) => Ok(Value::Null),
                (_, Value::Null) => Ok(Value::Null),
                (left, right) => Err(LegendDBError::Internal(format!("can not compare expression {:?} and {:?}", left, right))),
            }
        },
        Expression::Operation(Operation::NotEqual(left, right)) => {
            let left_val = evaluate_expr(left, left_col, left_row, right_col, right_row)?;
            let right_val = evaluate_expr(right, right_col, right_row, left_col, left_row)?;
            match (left_val, right_val) {
                (Value::Integer(left_val), Value::Integer(right_val)) => Ok(Value::Boolean(left_val != right_val)),
                (Value::Boolean(left_val), Value::Boolean(right_val)) => Ok(Value::Boolean(left_val != right_val)),
                (Value::Float(left_val), Value::Float(right_val)) => Ok(Value::Boolean(left_val != right_val)),
                (Value::Integer(left_val), Value::Float(right_val)) => Ok(Value::Boolean(left_val as f64 != right_val)),
                (Value::Float(left_val), Value::Integer(right_val)) => Ok(Value::Boolean(left_val != right_val as f64)),
                (Value::String(left_val), Value::String(right_val)) => Ok(Value::Boolean(left_val != right_val)),
                (Value::Null, _) => Ok(Value::Null),
                (_, Value::Null) => Ok(Value::Null),
                (left, right) => Err(LegendDBError::Internal(format!("can not compare expression {:?} and {:?}", left, right))),
            }
        },
        Expression::Operation(Operation::GreaterThan(left, right)) => {
            let left_val = evaluate_expr(left, left_col, left_row, right_col, right_row)?;
            let right_val = evaluate_expr(right, right_col, right_row, left_col, left_row)?;
            match (left_val, right_val) {
                (Value::Integer(left_val), Value::Integer(right_val)) => Ok(Value::Boolean(left_val > right_val)),
                (Value::Boolean(left_val), Value::Boolean(right_val)) => Ok(Value::Boolean(left_val > right_val)),
                (Value::Float(left_val), Value::Float(right_val)) => Ok(Value::Boolean(left_val > right_val)),
                (Value::Integer(left_val), Value::Float(right_val)) => Ok(Value::Boolean((left_val as f64) > right_val)),
                (Value::Float(left_val), Value::Integer(right_val)) => Ok(Value::Boolean(left_val > right_val as f64)),
                (Value::String(left_val), Value::String(right_val)) => Ok(Value::Boolean(left_val > right_val)),
                (Value::Null, _) => Ok(Value::Null),
                (_, Value::Null) => Ok(Value::Null),
                (left, right) => Err(LegendDBError::Internal(format!("can not compare expression {:?} and {:?}", left, right))),
            }
        },
        Expression::Operation(Operation::LessThan(left, right)) => {
            let left_val = evaluate_expr(left, left_col, left_row, right_col, right_row)?;
            let right_val = evaluate_expr(right, right_col, right_row, left_col, left_row)?;
            match (left_val, right_val) {
                (Value::Integer(left_val), Value::Integer(right_val)) => Ok(Value::Boolean(left_val < right_val)),
                (Value::Boolean(left_val), Value::Boolean(right_val)) => Ok(Value::Boolean(left_val < right_val)),
                (Value::Float(left_val), Value::Float(right_val)) => Ok(Value::Boolean(left_val < right_val)),
                (Value::Integer(left_val), Value::Float(right_val)) => Ok(Value::Boolean((left_val as f64) < right_val)),
                (Value::Float(left_val), Value::Integer(right_val)) => Ok(Value::Boolean(left_val < right_val as f64)),
                (Value::String(left_val), Value::String(right_val)) => Ok(Value::Boolean(left_val < right_val)),
                (Value::Null, _) => Ok(Value::Null),
                (_, Value::Null) => Ok(Value::Null),
                (left, right) => Err(LegendDBError::Internal(format!("can not compare expression {:?} and {:?}", left, right))),
            }
        },
        _ => Err(LegendDBError::Internal("Unexpected expression".into()))
    }
}