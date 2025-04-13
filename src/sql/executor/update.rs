use std::collections::BTreeMap;
use crate::sql::engine::engine::Transaction;
use crate::sql::executor::executor::{Executor, ResultSet};
use crate::sql::parser::ast::Expression;
use crate::sql::types::Value;
use crate::utils::custom_error::{LegendDBError, LegendDBResult};

pub struct UpdateExecutor<T: Transaction> {
    table_name: String,
    source: Box<dyn Executor<T>>,
    columns: BTreeMap<String, Expression>,
}

impl<T: Transaction> UpdateExecutor<T> {
    pub(crate) fn new(table_name: String, source: Box<dyn Executor<T>>, columns: BTreeMap<String, Expression>) -> Box<Self> {
        Box::new(Self {
            table_name,
            source,
            columns,
        })
    }
}

impl<T: Transaction> Executor<T> for UpdateExecutor<T> {
    fn execute(self: Box<Self>, txn: &mut T) -> LegendDBResult<ResultSet> {
        // 执行扫描操作， 获取到扫描的结果
        let mut count = 0;
        match self.source.execute(txn)? { 
            ResultSet::Scan { columns, rows } => {
                let table = txn.get_table_must(self.table_name)?;
                // 遍历所有要更新的行
                for row in rows {
                    let mut new_row = row.clone();
                    let pk = table.get_primary_key(&row)?;
                    for (index, col) in columns.iter().enumerate() {
                        if let Some(expr) = self.columns.get(col) {
                            // 更新列的值
                            new_row[index] = Value::from_expression(expr.clone());
                        }
                    }
                    // 执行更新操作
                    // 如果有主键更新，则删除原来的数据，新增一条新的数据
                    // 否则就根据table_name + primary key ==>更新数据
                    txn.update_row(&table, &pk, new_row)?;
                    count += 1;
                }
            },
            _ => {return Err(LegendDBError::Internal("Unexpected result set".into()))}
        }
        Ok(ResultSet::Update { count })
    }
}
