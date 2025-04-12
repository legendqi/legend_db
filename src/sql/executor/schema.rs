use crate::sql::engine::engine::Transaction;
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

pub struct DropTable {
    table_name: String,
}

impl DropTable {
    pub fn new(table_name: String) -> Box<Self> {
        Box::new(DropTable {
            table_name,
        })
    }
}

impl<T: Transaction> Executor<T> for DropTable {
    fn execute(self: Box<Self>, txn: &mut T) -> LegendDBResult<ResultSet> {
        txn.drop_table(&self.table_name)?;
        Ok(ResultSet::DropTable {
            table_name: self.table_name,
        })
    }
}