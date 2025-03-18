use crate::sql::executor::{Executor, ResultSet};
use crate::sql::parser::ast::Expression;
use crate::utils::custom_error::LegendDBResult;

pub struct Insert {
    table_name: String,
    columns: Vec<String>,
    values: Vec<Vec<Expression>>,
}

impl Insert {
    pub fn new(table_name: String, columns: Vec<String>, values: Vec<Vec<Expression>>) -> Box<Self> {
        Box::new(Insert {
            table_name,
            columns,
            values,
        })
    }
}

impl Executor for Insert {
    fn execute(&self) -> LegendDBResult<ResultSet> {
        todo!()
    }
}