use crate::sql::engine::engine::Transaction;
use crate::sql::executor::executor::{Executor, ResultSet};
use crate::utils::custom_error::{LegendDBError, LegendDBResult};

pub struct NestLoopJoinExecutor<T: Transaction> {
    left: Box<dyn Executor<T>>,
    right: Box<dyn Executor<T>>,
}

impl<T: Transaction>  NestLoopJoinExecutor<T> {
    pub fn new(left: Box<dyn Executor<T>>, right: Box<dyn Executor<T>>) -> Box<Self> {
        Box::new(
            Self {
                left,
                right,
        }
    )
    }
}

impl<T: Transaction> Executor<T> for NestLoopJoinExecutor<T> {
    fn execute(self: Box<NestLoopJoinExecutor<T>>, txn: &mut T) -> LegendDBResult<ResultSet> {
        // 先执行左边的查询
        if let ResultSet::Scan { columns, rows } = self.left.execute(txn)? {
            // 获取右边的查询
            if let ResultSet::Scan { columns: right_columns, rows: right_rows } = self.right.execute(txn)? {
                let mut new_rows = Vec::with_capacity(rows.len() * right_rows.len());
                let mut new_columns = columns.clone();
                new_columns.extend(right_columns);
               for row in rows {
                   let mut row = row.clone();
                   for right_row in &right_rows {
                       // 遍历左右两边的行，进行连接
                       // 构建连接后的行
                       row.extend(right_row.clone());
                       new_rows.push(row.clone());
                   }
               }
                return Ok(ResultSet::Scan {
                    columns: new_columns,
                    rows: new_rows,
                })
            }
        }
        Err(LegendDBError::Internal("Unexpected result set".into()))
    }
}