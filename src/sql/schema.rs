use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};
use crate::sql::types::{DataType, Row, Value};
use crate::custom_error::{LegendDBError, LegendDBResult};

#[derive(Serialize, Deserialize, Encode, Decode, Debug, PartialEq)]

pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
}

impl Table {
    // 校验表的有效性
    pub(crate) fn validate(&self) -> LegendDBResult<()> {
        // 校验是否有列信息
        if self.columns.is_empty() {
            return Err(LegendDBError::Internal(format!("table {} has no columns", self.name)));
        }
        // 校验是否有主键
        match self.columns.iter().filter(|c| c.is_primary_key).count() {
            1 => {},
            0 => return Err(LegendDBError::Internal(format!("table {} has no primary key", self.name))),
            _ => return Err(LegendDBError::Internal(format!("table {} has more than one primary key", self.name))),
        }
        // 检查列信息
        for column in &self.columns {
            // 检查列名
            if column.name.is_empty() {
                return Err(LegendDBError::Internal(format!("table {} has empty column name", self.name)));
            }
            // 主键不能为空
            if column.nullable && column.default_value.is_none() {
                return Err(LegendDBError::Internal(format!("table {} has nullable column {} without default value", self.name, column.name)));
            }
            // 检查列类型
            if let Some(default_value) = &column.default_value {
                match default_value.get_type() { 
                    Some(dt) => {
                        if dt != column.data_type {
                            return Err(LegendDBError::Internal(format!("table {} has column {} with invalid default value type", self.name, column.name)));
                        }
                    },
                    None => {}
                }
            }
        }
        Ok(())
    }
    
    // 获取主键值
    pub fn get_primary_key(&self, row: &Row) -> LegendDBResult<Value> {
        let position = self.columns.iter().position(|c| c.is_primary_key).expect("table has no primary key");
        Ok(row[position].clone())
    }
    
    // 获取列索引
    pub fn get_column_index(&self, name: &str) -> LegendDBResult<usize> {
        // 采用下面更优写法
        // match self.columns.iter().position(|c| c.name == name) {
        //     Some(index) => Ok(index),
        //     None => Err(LegendDBError::Internal(format!("table {} has no column {}", self.name, name))),
        // }
        self.columns.iter().position(|c| c.name == name).ok_or(LegendDBError::Internal(format!("table {} has no column {}", self.name, name)))
    }
}

#[derive(Serialize, Deserialize, Encode, Decode, Debug, PartialEq)]

pub struct Column {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub default_value: Option<Value>,
    pub is_primary_key: bool,
}