use std::cmp::Ordering;
use std::collections::HashMap;
use crate::sql::engine::engine::Transaction;
use crate::sql::executor::executor::{Executor, ResultSet};
use crate::sql::parser::ast::{evaluate_expr, Expression, OrderDirection};
use crate::custom_error::{LegendDBError, LegendDBResult};
use crate::sql::types::Value;

pub struct ScanExecutor {
    table_name: String,
    filter: Option<Vec<Expression>>
}

impl ScanExecutor {
    pub fn new(table_name: String, filter: Option<Vec<Expression>>) -> Box<Self> {
        Box::new(Self {
            table_name,
            filter
        })
    }
}

impl<T: Transaction> Executor<T> for ScanExecutor {
    fn execute(self: Box<Self>, txn: &mut T) -> LegendDBResult<ResultSet> {
        let table = txn.get_table_must(self.table_name.clone())?;
        let rows = txn.scan_table(self.table_name.clone(), self.filter)?;
        Ok(ResultSet::Scan { 
            columns: table.columns.into_iter().map(|c| c.name).collect(), 
            rows
        }
        )
    }
}


// 排序
pub struct OrderExecutor<T: Transaction> {
    source: Box<dyn Executor<T>>,
    order_by: Vec<(String, OrderDirection)>,
}

impl<T: Transaction> OrderExecutor<T> {
    pub(crate) fn new(source: Box<dyn Executor<T>>, order_by: Vec<(String, OrderDirection)>) -> Box<Self> {
        Box::new(
            Self {
                source,
                order_by,
            }
        )
    }
}

impl<T: Transaction> Executor<T> for OrderExecutor<T> {
    fn execute(self: Box<Self>, txn: &mut T) -> LegendDBResult<ResultSet> {
        match self.source.execute(txn)? { 
            ResultSet::Scan { columns, mut rows} => {
                // order by 后面的顺序可能跟 columns顺序不一致，所以需要找到列表中的列对应的位置
                let mut order_col_index = HashMap::new();
                for (i, (col_name, _)) in self.order_by.iter().enumerate() {
                    // 从columns中找到对应的position
                    match columns.iter().position(|c| c == col_name) {
                        Some(pos) => {
                            order_col_index.insert(i, pos);
                        },
                        None => return Err(LegendDBError::Internal(format!("Column {} not found in table", col_name)))
                    }
                }
                rows.sort_by(|col1, col2| {
                    for (i, (_, direction)) in self.order_by.iter().enumerate() {
                        let col_index = order_col_index.get(&i).unwrap();
                        let x = &col1[*col_index];
                        let y = &col2[*col_index];
                        match x.partial_cmp(y) {
                            Some(Ordering::Equal) => {},
                            Some(o) => return if *direction == OrderDirection::Asc { o } else { o.reverse() },
                            None => {}
                        }
                    }
                    Ordering::Equal
                });
                Ok(ResultSet::Scan { columns, rows })
            },
            _ => Err(LegendDBError::Internal("Unexpected result set".into()))
        }
    }
}


// Limit
pub struct LimitExecutor<T: Transaction> {
    source: Box<dyn Executor<T>>,
    limit: usize,
}

impl<T: Transaction> LimitExecutor<T> {
    pub(crate) fn new(source: Box<dyn Executor<T>>, limit: usize) -> Box<Self> {
        Box::new(
            Self {
                source,
                limit,
            }
        )
    }
}

impl<T: Transaction> Executor<T> for LimitExecutor<T> {
    fn execute(self: Box<Self>, txn: &mut T) -> LegendDBResult<ResultSet> {
        match self.source.execute(txn)? {
            ResultSet::Scan { columns, mut rows} => {
                // truncate 方法会将向量的长度截断到指定的长度。
                // 如果指定的长度小于当前向量的长度，向量将被截断，超出部分将被丢弃。
                // 如果指定的长度大于或等于当前向量的长度，向量保持不变。
                rows.truncate(self.limit); // 性能相比下面更高
                // 等效于
                // let new_row = rows.iter().take(self.limit).collect();
                Ok(ResultSet::Scan { columns, rows })
            },
            _ => Err(LegendDBError::Internal("Unexpected result set".into()))
        }
    }
}

pub struct OffsetExecutor<T: Transaction> {
    source: Box<dyn Executor<T>>,
    offset: usize,
}

impl<T: Transaction> OffsetExecutor<T> {
    pub(crate) fn new(source: Box<dyn Executor<T>>, offset: usize) -> Box<Self> {
        Box::new(
            Self {
                source,
                offset,
            }
        )
    }
}

impl<T: Transaction> Executor<T> for OffsetExecutor<T> {
    fn execute(self: Box<Self>, txn: &mut T) -> LegendDBResult<ResultSet> {
        match self.source.execute(txn)? {
            ResultSet::Scan { columns, mut rows} => {
                // 移除元素：
                // drain 方法会从集合中移除指定范围内的元素，并将这些元素从集合中删除。
                // 移除的元素可以通过返回的迭代器进行访问。
                // 范围参数：
                // drain 方法接受一个范围参数（如 0..3），表示要移除的元素范围。
                // 范围是左闭右开的，即包含起始索引，但不包含结束索引。
                // 返回迭代器：
                // drain 方法返回一个 Drain 迭代器，允许你遍历被移除的元素。
                // 直接修改 rows 向量，移除元素后，rows 的长度会减少， 由于是原地操作，性能较高，不需要额外的内存分配。
                rows.drain(..self.offset);
                // 等效于 rows.iter().skip(self.offset).collect(); 但是不会改变原始向量， 而是返回一个新的向量。
                // 需要额外的内存分配来存储结果， 性能相对 drain 较低
                Ok(ResultSet::Scan { columns, rows })
            },
            _ => Err(LegendDBError::Internal("Unexpected result set".into()))
        }
    }
}


pub struct ProjectionExecutor<T: Transaction> {
    source: Box<dyn Executor<T>>,
    columns: Vec<(Expression, Option<String>)>,
}

impl<T: Transaction> ProjectionExecutor<T> {
    pub fn new(source: Box<dyn Executor<T>>, columns: Vec<(Expression, Option<String>)>) -> Box<Self> {
        Box::new(
            Self {
                source,
                columns,
            }
        )
    }
}

impl<T: Transaction> Executor<T> for ProjectionExecutor<T> {
    fn execute(self: Box<Self>, txn: &mut T) -> LegendDBResult<ResultSet> {
        match self.source.execute(txn)? {
            ResultSet::Scan { columns, rows} => {
                let mut selected_columns = Vec::new();
                let mut new_columns = Vec::new();
                for (col, alias) in self.columns {
                    if let Expression::Field(col_name) = col {
                        let pos = match columns.iter().position(|c| *c == col_name) {
                            Some(pos) => pos,
                            None => return Err(LegendDBError::Internal(format!("Column {} not found in table", col_name)))
                        };
                        selected_columns.push(pos);
                        new_columns.push(if alias.is_some() { alias.clone().unwrap() } else { col_name });
                    }
                }
                let mut new_row = Vec::new();
                for row in rows.into_iter() {
                    let mut new_columns = Vec::new();
                    for i in selected_columns.iter() {
                        new_columns.push(row[*i].clone())
                    }
                    new_row.push(new_columns);
                }
                Ok(ResultSet::Scan { columns: new_columns, rows: new_row })
            },
            _ => Err(LegendDBError::Internal("Unexpected result set".into()))
        }
    }
}

pub struct FilterExecutor<T: Transaction> {
    source: Box<dyn Executor<T>>,
    predicate: Expression,
}

impl<T: Transaction> FilterExecutor<T> {
    pub fn new(source: Box<dyn Executor<T>>, predicate: Expression) -> Box<Self> {
        Box::new(
            Self {
                source,
                predicate,
            }
        )
    }
}

impl<T: Transaction> Executor<T> for FilterExecutor<T> {
    fn execute(self: Box<Self>, txn: &mut T) -> LegendDBResult<ResultSet> {
        match self.source.execute(txn)? { 
            ResultSet::Scan {columns, rows} => {
                let mut new_rows = Vec::new();
                for row in rows {
                    match evaluate_expr(&self.predicate, &columns, &row, &columns, &row)? { 
                        Value::Null => {},
                        Value::Boolean(true) => {
                            new_rows.push(row);
                        },
                        Value::Boolean(false) => {}
                        _ => {
                            return Err(LegendDBError::Internal("Unexpected result set".into()))
                        }
                    }
                }
                Ok(ResultSet::Scan { columns, rows: new_rows })
            },
            _ => {
                Err(LegendDBError::Internal("Unexpected result set".into()))
            }
        }
    }
}