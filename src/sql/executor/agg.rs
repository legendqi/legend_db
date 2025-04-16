use crate::custom_error::{LegendDBError, LegendDBResult};
use crate::sql::engine::engine::Transaction;
use crate::sql::executor::executor::{Executor, ResultSet};
use crate::sql::parser::ast::Expression;
use crate::sql::types::Value;
use crate::sql::types::Value::Null;

pub struct AggregateExecutor<T: Transaction> {
    source: Box<dyn Executor<T>>,
    expressions: Vec<(Expression, Option<String>)>,
}

impl<T: Transaction> AggregateExecutor<T> {
    pub fn new(source: Box<dyn Executor<T>>, expressions: Vec<(Expression, Option<String>)>) -> Box<Self> {
        Box::new(
            Self {
                source,
                expressions,
            }
        )
    }
}

impl<T: Transaction> Executor<T> for AggregateExecutor<T> {
    fn execute(self: Box<Self>, txn: &mut T) -> LegendDBResult<ResultSet> {
        if let ResultSet::Scan { columns, rows } = self.source.execute(txn)? {
            let mut new_row = Vec::new();
            let mut new_col = Vec::new();
            for (expr, alias) in self.expressions {
                if let Expression::Function(func_name, col_name) = expr {
                    let calculator = <dyn Calculator>::build(&func_name)?;
                    let value = calculator.calculate(&col_name, &columns, &rows)?;
                    new_row.push(value);
                    // 有别名就取别名，没有别名就取函数名
                    new_col.push(if let Some(alias) = alias { alias } else { func_name })
                }
            }
            return Ok(ResultSet::Scan {
                columns: new_col,
                rows: vec![new_row],
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