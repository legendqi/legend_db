use crate::sql::engine::engine::Transaction;
use crate::sql::executor::executor::{Executor, ResultSet};
use crate::sql::schema::Table;
use crate::custom_error::LegendDBResult;

pub struct CreateTableExecutor {
    schema: Table,
}

// 
impl CreateTableExecutor {
    pub fn new(schema: Table) -> Box<Self> {
        Box::new(CreateTableExecutor {
            schema,
        })
    }
}

impl<T: Transaction> Executor<T> for CreateTableExecutor {
    fn execute(self: Box<Self>, txn: &mut T) -> LegendDBResult<ResultSet> {
        let table_name = self.schema.name.clone();
        txn.create_table(self.schema)?;
        Ok(ResultSet::CreateTable {table_name})
    }
}

pub struct DropTableExecutor {
    table_name: String,
}

impl DropTableExecutor {
    pub fn new(table_name: String) -> Box<Self> {
        Box::new(Self {
            table_name,
        })
    }
}

impl<T: Transaction> Executor<T> for DropTableExecutor {
    fn execute(self: Box<Self>, txn: &mut T) -> LegendDBResult<ResultSet> {
        txn.drop_table(&self.table_name)?;
        Ok(ResultSet::DropTable {
            table_name: self.table_name,
        })
    }
}