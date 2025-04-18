use crate::sql::engine::engine::Transaction;
use crate::sql::executor::executor::{Executor, ResultSet};
use crate::sql::parser::ast::{Expression, Operation};
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

fn evaluate_expr(expression: &Expression, left_col: &Vec<String>, left_row: &Vec<Value>, right_col: &Vec<String>, right_row: &Vec<Value>) -> LegendDBResult<Value> {
    match expression {
        Expression::Field(col_name) => {
            let pos = left_col.iter().position(|x| *x == *col_name).ok_or(LegendDBError::Internal(format!("Column {} not found", col_name)));
            Ok(left_row[pos?].clone())
        },
        Expression::Operation(Operation::Equal(left, right)) => {
            let left_val = evaluate_expr(left, left_col, left_row, right_col, right_row)?;
            let right_val = evaluate_expr(right, right_col, right_row, left_col, left_row)?;
            match (left_val, right_val) {
                (Value::Integer(left_val), Value::Integer(right_val)) => Ok(Value::Boolean(left_val == right_val)),
                (Value::Boolean(left_val), Value::Boolean(right_val)) => Ok(Value::Boolean(left_val == right_val)),
                (Value::Float(left_val), Value::Float(right_val)) => Ok(Value::Boolean(left_val == right_val)),
                (Value::Integer(left_val), Value::Float(right_val)) => Ok(Value::Boolean(left_val as f64 == right_val)),
                (Value::Float(left_val), Value::Integer(right_val)) => Ok(Value::Boolean(left_val == right_val as f64)),
                (Value::String(left_val), Value::String(right_val)) => Ok(Value::Boolean(left_val == right_val)),
                (Value::Null, _) => Ok(Value::Null),
                (_, Value::Null) => Ok(Value::Null),
                (left, right) => Err(LegendDBError::Internal(format!("can not compare expression {:?} and {:?}", left, right))),
            }
        },
        Expression::Operation(Operation::NotEqual(left, right)) => todo!(),
        Expression::Operation(Operation::GreaterThan(left, right)) => todo!(),
        Expression::Operation(Operation::LessThan(left, right)) => todo!(),
        _ => Err(LegendDBError::Internal("Unexpected expression".into()))
    }
}