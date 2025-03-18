mod planner;

use crate::sql::parser::ast::Statement;
use crate::sql::plan::planner::Planner;
use crate::sql::schema::Table;
use crate::sql::types::Value;

pub enum Node {
    CreateTable {
        schema: Table
    },
    Insert {
        table_name: String,
        columns: Vec<String>,
        values: Vec<Vec<Value>>
    },

    Scan {
        table_name: String,
    }
}

//执行计划定义，底层是不同类型的节点
pub struct Plan(pub Node);


impl Plan {
    pub fn build(stmt: Statement) -> Self {
        Planner::new().build(stmt)
    }
}