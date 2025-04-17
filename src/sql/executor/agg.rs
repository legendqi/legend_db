use std::collections::HashMap;
use crate::custom_error::{LegendDBError, LegendDBResult};
use crate::sql::engine::engine::Transaction;
use crate::sql::executor::executor::{Executor, ResultSet};
use crate::sql::parser::ast::Expression;
use crate::sql::types::Value;
use crate::sql::types::Value::Null;

pub struct AggregateExecutor<T: Transaction> {
    source: Box<dyn Executor<T>>,
    expressions: Vec<(Expression, Option<String>)>,
    group_by: Option<Expression>
}

impl<T: Transaction> AggregateExecutor<T> {
    pub fn new(source: Box<dyn Executor<T>>, expressions: Vec<(Expression, Option<String>)>, group_by: Option<Expression>) -> Box<Self> {
        Box::new(
            Self {
                source,
                expressions,
                group_by,
            }
        )
    }
}

impl<T: Transaction> Executor<T> for AggregateExecutor<T> {
    fn execute(self: Box<Self>, txn: &mut T) -> LegendDBResult<ResultSet> {
        if let ResultSet::Scan { columns, rows } = self.source.execute(txn)? {
            let mut new_row = Vec::new();
            let mut new_col = Vec::new();
            // 计算聚合函数 如果是分组的计算，
            let mut agg_calculation = |col_val: Option<&Value>, row: &Vec<Vec<Value>>| -> LegendDBResult<(Vec<Value>)> {
                let mut new_row = Vec::new();
                // 此处也需要使用借用类型
                for (expr, alias) in &self.expressions {
                    match expr {
                        Expression::Function(func_name, col_name) => {
                            let calculator = <dyn Calculator>::build(&func_name)?;
                            let value = calculator.calculate(&col_name, &columns, row)?;
                            new_row.push(value);
                            // min(a)            -> min
                            // min(a) as min_val -> min_val
                            // 有别名就取别名，没有别名就取函数名
                            if new_col.len() < self.expressions.len() {
                                new_col.push(if let Some(alias) = alias { alias.clone() } else { func_name.clone() })
                            }
                        },
                        // group by的列
                        Expression::Field(col) => {
                            if let Some(Expression::Field(group_col)) = &self.group_by {
                                if *col != *group_col {
                                    return Err(LegendDBError::Internal(format!("{} must appear in the GROUP BY clause or aggregate function", col)))
                                }
                            }
                            if new_col.len() < self.expressions.len() {
                                new_col.push(if let Some(alias) = alias { alias.clone() } else { col.clone() });
                            }
                            // 此处col_val在Expression::Field(col)的match情况中，使用了就回收了，而前面是有所有权的，所以这儿可以使用借用类型
                            new_row.push(col_val.unwrap().clone());
                        }
                        _ => return Err(LegendDBError::Internal("Unexpected expression".to_string()))
                    }
                }
                return Ok((new_row))
            };
            // 判断是否有group_by
            // select c2, min(c1), max(c3) from t group by c2;
            // c1 c2 c3
            // 1 aa 4.6
            // 3 cc 3.4
            // 2 bb 5.2
            // 4 cc 6.1
            // 5 aa 8.3
            // ----|------
            // ----|------
            // ----v------
            // 1 aa 4.6
            // 5 aa 8.3
            //
            // 2 bb 5.2
            //
            // 3 cc 3.4
            // 4 cc 6.1
            if let Some(Expression::Field(group_col)) = &self.group_by {
                // 获取分组列的位置
                let position = get_position(&columns, &group_col)?;
                // 针对Group by 的列进行分组
                let mut agg_map = HashMap::new();
                for row in rows.iter() {
                    let key = &row[position];
                    // Value作为hashmap的key，需要实现Hash的trait
                    let value = agg_map.entry(key).or_insert(Vec::new());
                    value.push(row.to_owned())
                }
                for (key, value) in agg_map {
                    let row = agg_calculation(Some(key), &value)?;
                    new_row.push(row);
                }
             } else {
                let row = agg_calculation(None, &rows)?;
                new_row.push(row);
            }

            return Ok(ResultSet::Scan {
                columns: new_col,
                rows: new_row,
            })
        }
        Err(LegendDBError::Internal("Unexpected result set".to_string()))
    }
}

pub trait Calculator {
    fn calculate(&self, col_name: &str, col: &Vec<String>, row: &Vec<Vec<Value>>) -> LegendDBResult<Value>;
}

impl dyn Calculator {
    pub fn build(func_name: &str) -> LegendDBResult<Box<dyn Calculator>> {
        Ok(match func_name.to_uppercase().as_ref() {
            "COUNT" => Count::new(),
            "SUM" => Sum::new(),
            "AVG" => Avg::new(),
            "MIN" => Min::new(),
            "MAX" => Max::new(),
            _ => return Err(LegendDBError::Internal(format!("This function {} is not currently supported", func_name)))
        })
    }
}

pub struct Count;
pub struct Sum;
pub struct Avg;
pub struct Min;
pub struct Max;

fn get_position(col: &Vec<String>, col_name: &str) -> LegendDBResult<usize> {
    Ok(match col.iter().position(|x| x == col_name) {
        Some(pos) => pos,
        None => {
            return Err(LegendDBError::Internal(format!("Column {} not found", col_name)))
        }
    })
}

impl Count {
    fn new() -> Box<Self> {
        Box::new(Self {})
    }
}
impl Calculator for Count {
    fn calculate(&self, col_name: &str, col: &Vec<String>, row: &Vec<Vec<Value>>) -> LegendDBResult<Value> {
        let position = get_position(col, col_name)?;
        // a  b     c
        // 1  X     3.1
        // 2  NULL  6.4
        // 3  Z     1.5
        let mut count = 0;
        for row in row.iter() {
            if row[position] != Value::Null {
                count += 1;
            }
        }
        Ok(Value::Integer(count))
    }
}

impl Min {
    fn new() -> Box<Self> {
        Box::new(Self {})
    }
}
impl Calculator for Min {
    fn calculate(&self, col_name: &str, col: &Vec<String>, row: &Vec<Vec<Value>>) -> LegendDBResult<Value> {
        let position = get_position(col, col_name)?;
        // NULL的时候跳过，如果全为NULL，返回NULL
        let mut values = Vec::new();
        for row in row.iter() {
            if row[position] != Value::Null {
                values.push(&row[position]);
            }
        }
        let mut min = Value::Null;
        // Value 实现了 PartialOrd
        if !values.is_empty() {
            // NULL 值是跳过的，这儿可以直接unwrap()
            values.sort_by(|a, b| a.partial_cmp(b).unwrap());
            min = values[0].clone();
        }
        Ok(min)
    }
}

impl Max {
    fn new() -> Box<Self> {
        Box::new(Self {})
    }
}
impl Calculator for Max {
    fn calculate(&self, col_name: &str, col: &Vec<String>, row: &Vec<Vec<Value>>) -> LegendDBResult<Value> {
        let position = get_position(col, col_name)?;
        let mut values = Vec::new();
        for row in row.iter() {
            if row[position] != Null {
                values.push(&row[position]);
            }
        }
        let mut max = Null;
        if !values.is_empty() {
            values.sort_by(|a, b| b.partial_cmp(a).unwrap());
            max = values[0].clone();
        }
        Ok(max)
    }
}

impl Sum {
    fn new() -> Box<Self> {
        Box::new(Self {})
    }
}
impl Calculator for Sum {
    fn calculate(&self, col_name: &str, col: &Vec<String>, row: &Vec<Vec<Value>>) -> LegendDBResult<Value> {
        let position = get_position(col, col_name)?;
        let mut sum = None;
        for row in row.iter() {
            match row[position] {
                Value::Integer(i) => {
                    if sum.is_none() {
                        sum = Some(0.0);
                    } else {
                        sum = Some(sum.unwrap() + i as f64);
                    }
                },
                Value::Float(f) => {
                    if sum.is_none() {
                        sum = Some(0.0);
                    } else {
                        sum = Some(sum.unwrap() + f);
                    }
                },
                Null => {},
                _ => {
                    return Err(LegendDBError::Internal(format!("Column {} is not number", col_name)))
                }
            }
        }
        match sum {
            Some(s) => Ok(Value::Float(s)),
            None => Ok(Null),
        }
    }
}

impl Avg {
    fn new() -> Box<Self> {
        Box::new(Self {})
    }
}
impl Calculator for Avg {
    fn calculate(&self, col_name: &str, col: &Vec<String>, row: &Vec<Vec<Value>>) -> LegendDBResult<Value> {
        let sum = Sum::new().calculate(col_name, col, row)?;
        let count = Count::new().calculate(col_name, col, row)?;
        match (sum, count) { 
            (Value::Float(sum), Value::Integer(count)) => {
                Ok(Value::Float(sum / count as f64))
            },
            _ => {
                Ok(Null)
            }
        }
    }
}