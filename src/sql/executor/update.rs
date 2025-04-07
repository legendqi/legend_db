use std::collections::BTreeMap;
use crate::sql::engine::Transaction;
use crate::sql::executor::executor::{Executor, ResultSet};
use crate::sql::parser::ast::Expression;
use crate::utils::custom_error::LegendDBResult;

pub struct Update<T: Transaction> {
    table_name: String,
    source: Box<dyn Executor<T>>,
    columns: BTreeMap<String, Expression>,
}

impl<T: Transaction> Update<T> {
    pub(crate) fn new(table_name: String, source: Box<dyn Executor<T>>, columns: BTreeMap<String, Expression>) -> Box<Self> {
        Box::new(Update {
            table_name,
            source,
            columns,
        })
    }
}

impl<T: Transaction> Executor<T> for Update<T> {
    fn execute(self: Box<Self>, txn: &mut T) -> LegendDBResult<ResultSet> {
        todo!()
    }
}
