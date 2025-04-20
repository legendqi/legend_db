use crate::sql::engine::engine::Transaction;
use crate::sql::executor::executor::{Executor, ResultSet};
use crate::sql::parser::ast::{evaluate_expr, Expression};
use crate::sql::types::Value;
use crate::custom_error::{LegendDBError, LegendDBResult};

pub struct NestLoopJoinExecutor<T: Transaction> {
    left: Box<dyn Executor<T>>,
    right: Box<dyn Executor<T>>,
    predicate: Option<Expression>,
    router: bool
}

impl<T: Transaction>  NestLoopJoinExecutor<T> {
    pub fn new(left: Box<dyn Executor<T>>, right: Box<dyn Executor<T>>, predicate: Option<Expression>, router: bool) -> Box<Self> {
        Box::new(
            Self {
                left,
                right,
                predicate,
                router,
        }
    )
    }
}

impl<T: Transaction> Executor<T> for NestLoopJoinExecutor<T> {
    fn execute(self: Box<NestLoopJoinExecutor<T>>, txn: &mut T) -> LegendDBResult<ResultSet> {
        // 先执行左边的查询
        if let ResultSet::Scan { columns: lcols, rows: lrows } = self.left.execute(txn)? {
            let mut new_rows = Vec::new();
            let mut new_columns = lcols.clone();
            // 获取右边的查询
            if let ResultSet::Scan { columns: rcols, rows: rrows } = self.right.execute(txn)? {
                new_columns.extend(rcols.clone());
               for lrow in &lrows {
                   let mut matched = false;
                   for rrow in &rrows {
                       let mut row = lrow.clone();
                       // 如果有条件，则进行条件判断，如果满足条件，则加入到结果集中
                       if let Some(predicate) = &self.predicate {
                           // 左表中的一列与右表的一列进行比较
                           match evaluate_expr(predicate, &lcols, lrow, &rcols, rrow)? {
                               Value::Boolean(true) => {
                                   // 满足条件，则加入到结果集中
                                   row.extend(rrow.clone());
                                   new_rows.push(row);
                                   matched = true;
                               },
                               Value::Boolean(false) => {},
                               Value::Null => {},
                               _ => {
                                   return Err(LegendDBError::Internal("Unexpected Expression".into()));
                               }
                           }
                       } else {
                           row.extend(rrow.clone());
                           new_rows.push(row.clone());
                       }
                   }
                   if self.router && !matched {
                       // 如果是outer模式，则只返回一条记录， 其他的需要填充空
                       let mut row = lrow.clone();
                       for _ in 0..rrows[0].len() {
                           row.push(Value::Null);
                       }
                       new_rows.push(row);
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
