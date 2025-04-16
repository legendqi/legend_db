use crate::sql::engine::engine::Transaction;
use crate::sql::executor::executor::{Executor, ResultSet};
use crate::custom_error::LegendDBResult;

pub struct CreateDataBaseExecutor {
    pub database_name: String,
}

impl CreateDataBaseExecutor {
    pub fn new(database_name: String) -> Box<Self> {
        Box::new(Self {
            database_name
        }
    )
    }
}

impl<T: Transaction> Executor<T> for CreateDataBaseExecutor {
    fn execute(self: Box<Self>, txn: &mut T) -> LegendDBResult<ResultSet> {
        txn.create_database(&*self.database_name.clone())?;
        Ok(ResultSet::CreateDatabase {
            database_name: self.database_name.clone(),
        })
    }
}

pub struct DropDataBaseExecutor {
    pub database_name: String,
}

impl DropDataBaseExecutor {
    pub fn new(database_name: String) -> Box<Self> {
        Box::new(Self {
            database_name
        }
    )
    }
}

impl<T: Transaction> Executor<T> for DropDataBaseExecutor {
    fn execute(self: Box<Self>, txn: &mut T) -> LegendDBResult<ResultSet> {
        txn.drop_database(&*self.database_name.clone())?;
        Ok(ResultSet::DropDatabase {
            database_name: self.database_name.clone(),
        })
    }
}

pub struct UseDatabaseExecutor {
    pub database_name: String,
}

impl UseDatabaseExecutor {
    pub fn new(database_name: String) -> Box<Self> {
        Box::new(Self {
            database_name
        }
    )
    }
}

impl<T: Transaction> Executor<T> for UseDatabaseExecutor {
    fn execute(self: Box<Self>, txn: &mut T) -> LegendDBResult<ResultSet> {
        txn.use_database(&*self.database_name.clone())?;
        Ok(ResultSet::UseDatabase {
            database_name: self.database_name.clone(),
        })
    }
}