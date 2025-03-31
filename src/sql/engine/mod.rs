mod kv;

use crate::sql::executor::ResultSet;
use crate::sql::parser::Parser;
use crate::sql::plan::Plan;
use crate::sql::schema::Table;
use crate::sql::types::Row;
use crate::utils::custom_error::{LegendDBError, LegendDBResult};

// 抽象的SQL引擎层定义，目前只有一个KVEngine
pub trait Engine: Clone{
    type Transaction: Transaction;

    fn begin(&self) -> LegendDBResult<Self::Transaction>;

    fn session(&self) -> LegendDBResult<Session<Self>> {
        Ok(Session {
            engine: self.clone(),
            transaction: self.begin()?,
        })
    }
}

// 抽象的事务信息，包含DDL和DML操作
// 底层可以接入普通的KV存储殷勤，也可以接入分布式存储引擎
pub trait Transaction {
    // 提交事务
    fn commit(&self) -> LegendDBResult<()>;

    // 回滚事务
    fn rollback(&self) -> LegendDBResult<()>;

    // 创建数据库
    fn create_database(&self, name: &str) -> LegendDBResult<()>;

    // 删除数据库
    fn drop_database(&self, name: &str) -> LegendDBResult<()>;

    // 创建表
    fn create_table(&mut self, table: Table) -> LegendDBResult<()>;

    // 删除表
    fn drop_table(&self, name: &str) -> LegendDBResult<()>;

    //创建行
    fn create_row(&mut self, table: String, row: Row) -> LegendDBResult<()>;

    // 扫描表
    fn scan_table(&mut self, table: String) -> LegendDBResult<Vec<Row>>;

    //获取表信息
    fn get_table(&self, table: String) -> LegendDBResult<Option<Table>>;
    // 获取表信息，不存在则报错
    fn get_table_must(&self, table: String) -> LegendDBResult<Table> {
        self.get_table(table.clone())?
            .ok_or(LegendDBError::TableNotFound(format!("Table {} not found", table)))
    }
}

// 客户端Session定义
pub struct Session<E: Engine> {
    engine: E,
    transaction: E::Transaction,
}

impl<E: Engine> Session<E>  {
    // 执行客户端SQL语句
    pub fn execute(&mut self, sql: &str) -> LegendDBResult<ResultSet> {
        match Parser::new(sql).parse()? {
            stmt => {
                let mut txn = self.engine.begin()?;
                // 构建执行计划Plan，执行sql
                match Plan::build(stmt).execute(&mut txn) {
                    Ok(result) => {
                        txn.commit()?;
                        Ok(result)
                    }
                    Err(err) => {
                        txn.rollback()?;
                        Err(err)
                    }
                }
            }
        }
    }
}

fn fibonacci(term: u32) -> u32 {
    match term {
        0 => 0,
        1 => 1,
        _ => fibonacci(term - 1) + fibonacci(term - 2),
    }
}

fn fibonacci_func(term: u32) -> u32 {
    match term {
        0 => 0,
        1 => 1,
        2 => (0..term).fold((0, 1), |(a, b), _| (b, a + b)).1,
        _ => (0..term).fold((0, 1), |(a, b), _| (b, a + b)).1,
    }
}

#[test]
fn test_fibonacci() {
    println!("fibonacci(10) = {}", fibonacci(10))
}