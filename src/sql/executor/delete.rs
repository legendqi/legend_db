use crate::sql::engine::Transaction;
use crate::sql::executor::executor::{Executor, ResultSet};
use crate::sql::types::Value;
use crate::utils::custom_error::{LegendDBError, LegendDBResult};

pub struct Delete<T: Transaction> {
    table_name: String,
    source: Box<dyn Executor<T>>,
}

impl<T: Transaction> Delete<T>  {
    pub fn new(table_name: String, source: Box<dyn Executor<T>>) -> Box<Self> {
        Box::new(Delete {
            table_name,
            source,
        })
    }
}

impl<T: Transaction>  Executor<T> for Delete<T> {
    fn execute(self: Box<Self>, txn: &mut T) -> LegendDBResult<ResultSet> {
        let mut count = 0;
        match self.source.execute(txn)? { 
            ResultSet::Scan { columns: _, rows} => {
                // 表名加主键定位数据
                let table = txn.get_table_must(self.table_name)?;
                // 遍历所有要更新的行
                for row in rows {
                    let pk = table.get_primary_key(&row)?;
                    txn.delete_row(&table, &pk)?;
                    count += 1;
                }
            },
            _ => {return Err(LegendDBError::Internal("Unexpected result set".into()))}
        }
        Ok(ResultSet::Delete { count })
    }
}