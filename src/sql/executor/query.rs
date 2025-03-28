use crate::sql::engine::Transaction;
use crate::sql::executor::{Executor, ResultSet};
use crate::utils::custom_error::LegendDBResult;

pub struct Scan {
    table_name: String,
}

impl Scan {
    pub fn new(table_name: String) -> Box<Self> {
        Box::new(Scan {
            table_name,
        })
    }
}

impl<T: Transaction> Executor<T> for Scan {
    fn execute(self: Box<Self>, txn: &mut T) -> LegendDBResult<ResultSet> {
        let table = txn.get_table_must(self.table_name.clone())?;
        let rows = txn.scan_table(self.table_name.clone())?;
        Ok(ResultSet::Scan { 
            columns: table.columns.into_iter().map(|c| c.name).collect(), 
            row: rows}
        )
    }
}
