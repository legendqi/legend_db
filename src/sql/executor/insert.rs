use std::collections::HashMap;
use crate::sql::engine::engine::Transaction;
use crate::sql::executor::executor::{Executor, ResultSet};
use crate::sql::parser::ast::Expression;
use crate::sql::schema::Table;
use crate::sql::types::{Row, Value};
use crate::sql::types::DataType::Null;
use crate::utils::custom_error::{LegendDBError, LegendDBResult};

pub struct InsertExecutor {
    table_name: String,
    columns: Vec<String>,
    values: Vec<Vec<Expression>>,
}

impl InsertExecutor {
    pub fn new(table_name: String, columns: Vec<String>, values: Vec<Vec<Expression>>) -> Box<Self> {
        Box::new(Self {
            table_name,
            columns,
            values,
        })
    }
}

// 列对齐，也就是补全未指定的列
// table
// insert into table values(1,2,3)
// a   b   c   d
// 1   2   3   default 填充
// 如果没有默认值，则报错
fn pad_row(table: &Table, row: &Row) -> LegendDBResult<Row> {
    // skip 跳过前面的n个元素，就是跳过values的长度
    let mut results = row.clone();
    for column in table.columns.iter().skip(row.len()) {
        if let Some(default_value) = &column.default_value {
            results.push(default_value.clone());
        } else {
            return Err(LegendDBError::Internal("Missing default value".to_string()));
        }
    }
    Ok(results)
}

// insert into table(d, c) values(1,2)
//   a           b       c   d
// default     default   2   1

fn make_row(table: &Table, columns: &Vec<String>, values: &Row) -> LegendDBResult<Row> {
    // 判断columns和values的长度是否一致
    if columns.len() != values.len() {
        return Err(LegendDBError::Internal("Column and value length mismatch".to_string()))
    }
    // 创建一个HashMap，用于存储指定的列名和值
    let mut inputs = HashMap::new();
    for (index, col_name) in columns.iter().enumerate() {
        inputs.insert(col_name, values[index].clone());
    }
    for col in table.columns.iter() {
        if !columns.contains(&col.name) {
            if let Some(default_value) = &col.default_value {
                inputs.insert(&col.name, default_value.clone());
            } else {
                return Err(LegendDBError::Internal(format!("Missing default value for column {}", col.name)));
            }
        }
    }
    Ok(inputs.values().cloned().collect::<Vec<_>>())
}

impl<T: Transaction> Executor<T> for InsertExecutor {

    fn execute(self: Box<Self>, txn: &mut T) -> LegendDBResult<ResultSet> {
        let mut count = 0;
        //先取出表中的信息
        let table = txn.get_table_must(self.table_name.clone())?;
        // 将表达式转换为值
        for exprs in self.values {
            let row = exprs.into_iter().map(|expr| {Value::from_expression(expr)}).collect::<Vec<_>>();
            // 如果没有指定插入的列
            let insert_row = if self.columns.is_empty() {
                pad_row(&table, &row)?
            } else {
                // 指定了插入的列，需要对value信息进行整理
                make_row(&table, &self.columns, &row)?
            };
            // 检查列类型是否匹配
            for (index, col) in table.columns.iter().enumerate() {
                // 如果列允许为空，则跳过
                if col.nullable {
                    continue;
                }
                let row_data_type = insert_row[index].get_type().unwrap_or_else(|| Null);
                // 如果列不允许为空，则检查值是否为空
                if !col.nullable && row_data_type == Null {
                    return Err(LegendDBError::Internal(format!("Column {} cannot be null", col.name)));
                }
                // 类型不匹配则报错
                if col.data_type != row_data_type {
                    return Err(LegendDBError::Internal(format!("Column type mismatch: {}", col.name)));
                }
            }
            // 将整理后的值插入到表中
            txn.create_row(self.table_name.clone(), insert_row)?;
            count += 1;
        }
        Ok(ResultSet::Insert { count})
    }
}