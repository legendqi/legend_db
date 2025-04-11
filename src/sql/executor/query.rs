use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};
use crate::sql::engine::Transaction;
use crate::sql::executor::executor::{Executor, ResultSet};
use crate::sql::parser::ast::{Expression, OrderDirection};
use crate::utils::custom_error::{LegendDBError, LegendDBResult};

pub struct Scan {
    table_name: String,
    filter: Option<BTreeMap<String, Expression>>
}

impl Scan {
    pub fn new(table_name: String, filter: Option<BTreeMap<String, Expression>>) -> Box<Self> {
        Box::new(Scan {
            table_name,
            filter
        })
    }
}

impl<T: Transaction> Executor<T> for Scan {
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
pub struct Order<T: Transaction> {
    source: Box<dyn Executor<T>>,
    order_by: Vec<(String, OrderDirection)>,
}

impl<T: Transaction> Order<T> {
    pub(crate) fn new(source: Box<dyn Executor<T>>, order_by: Vec<(String, OrderDirection)>) -> Box<Self> {
        Box::new(
            Self {
                source,
                order_by,
            }
        )
    }
}

impl<T: Transaction> Executor<T> for Order<T> {
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
