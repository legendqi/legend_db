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
    fn execute(&self, txn: &mut T) -> LegendDBResult<ResultSet> {
       todo!()
    }
}
