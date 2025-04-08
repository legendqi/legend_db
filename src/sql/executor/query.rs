use std::collections::BTreeMap;
use crate::sql::engine::Transaction;
use crate::sql::executor::executor::{Executor, ResultSet};
use crate::sql::parser::ast::Expression;
use crate::utils::custom_error::LegendDBResult;

pub struct Scan {
    table_name: String,
    filter: Option<BTreeMap<String, Expression>>
}

impl Scan {
    pub fn new(table_name: String, filter: Option<BTreeMap<String, Expression>>) -> Box<Self> {
        Box::new(Scan {
            table_name,
            filter
        })
    }
}

impl<T: Transaction> Executor<T> for Scan {
    fn execute(self: Box<Self>, txn: &mut T) -> LegendDBResult<ResultSet> {
        let table = txn.get_table_must(self.table_name.clone())?;
        let rows = txn.scan_table(self.table_name.clone(), self.filter)?;
        Ok(ResultSet::Scan { 
            columns: table.columns.into_iter().map(|c| c.name).collect(), 
            rows
        }
        )
    }
}
