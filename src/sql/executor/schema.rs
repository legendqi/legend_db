use crate::sql::engine::Transaction;
use crate::sql::executor::{Executor, ResultSet};
use crate::sql::schema::Table;
use crate::utils::custom_error::LegendDBResult;

pub struct CreateTable {
    schema: Table,
}

// 
impl CreateTable {
    pub fn new(schema: Table) -> Box<Self> {
        Box::new(CreateTable {
            schema,
        })
    }
}

impl<T: Transaction> Executor<T> for CreateTable {
    fn execute(&self, txn: &mut T) -> LegendDBResult<ResultSet> {
        todo!()
    }
}