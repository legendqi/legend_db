use crate::sql::engine::Transaction;
use crate::sql::executor::executor::{Executor, ResultSet};
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
    fn execute(self: Box<Self>, txn: &mut T) -> LegendDBResult<ResultSet> {
        let table_name = self.schema.name.clone();
        txn.create_table(self.schema)?;
        Ok(ResultSet::CreateTable {table_name})
    }
}