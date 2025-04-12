use crate::sql::engine::engine::Transaction;
use crate::sql::executor::executor::{Executor, ResultSet};
use crate::utils::custom_error::LegendDBResult;

pub struct CreateDataBase {
    pub database_name: String,
}

impl CreateDataBase {
    pub fn new(database_name: String) -> Box<Self> {
        Box::new(CreateDataBase {
            database_name
        }
    )
    }
}

impl<T: Transaction> Executor<T> for CreateDataBase {
    fn execute(self: Box<Self>, txn: &mut T) -> LegendDBResult<ResultSet> {
        txn.create_database(&*self.database_name.clone())?;
        Ok(ResultSet::CreateDatabase {
            database_name: self.database_name.clone(),
        })
    }
}

pub struct DropDataBase {
    pub database_name: String,
}

impl DropDataBase {
    pub fn new(database_name: String) -> Box<Self> {
        Box::new(DropDataBase {
            database_name
        }
    )
    }
}

impl<T: Transaction> Executor<T> for DropDataBase {
    fn execute(self: Box<Self>, txn: &mut T) -> LegendDBResult<ResultSet> {
        txn.drop_database(&*self.database_name.clone())?;
        Ok(ResultSet::DropDatabase {
            database_name: self.database_name.clone(),
        })
    }
}