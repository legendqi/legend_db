use crate::sql::executor::executor::ResultSet;
use crate::sql::parser::ast::Expression;
use crate::sql::parser::parser::Parser;
use crate::sql::plan::node::Plan;
use crate::sql::schema::Table;
use crate::sql::types::{Row, Value};
use crate::custom_error::{LegendDBError, LegendDBResult};

// 抽象的SQL引擎层定义，目前只有一个KVEngine
pub trait Engine: Clone{
    type Transaction: Transaction;

    fn begin(&self) -> LegendDBResult<Self::Transaction>;

    fn session(&self) -> LegendDBResult<Session<Self>> {
        Ok(Session {
            engine: self.clone(),
            transaction: None,
        })
    }
}


#[allow(unused)]
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

    // 切换数据库
    fn use_database(&self, database_name: &str) -> LegendDBResult<()>;

    // 创建表
    fn create_table(&mut self, table: Table) -> LegendDBResult<()>;

    // 删除表
    fn drop_table(&self, name: &str) -> LegendDBResult<()>;

    //创建行
    fn create_row(&mut self, table: String, row: Row) -> LegendDBResult<()>;

    // 更新行
    fn update_row(&mut self, table: &Table, id: &Value, row: Row) -> LegendDBResult<()>;

    // 删除行
    fn delete_row(&mut self, table: &Table, id: &Value) -> LegendDBResult<()>;

    // 扫描表
    fn scan_table(&mut self, table_name: String, filter: Option<Vec<Expression>>) -> LegendDBResult<Vec<Row>>;

    //获取表信息
    fn get_table(&self, table: String) -> LegendDBResult<Option<Table>>;

    // 获取所有的表名
    fn get_table_names(&mut self) -> LegendDBResult<Vec<String>>;
    // 获取表信息，不存在则报错
    fn get_table_must(&self, table: String) -> LegendDBResult<Table> {
        self.get_table(table.clone())?
            .ok_or(LegendDBError::TableNotFound(format!("Table {} not found", table)))
    }
}

#[allow(unused)]
// 客户端Session定义
pub struct Session<E: Engine> {
    pub engine: E,
    pub transaction: Option<E::Transaction>,
}

#[allow(unused)]
impl<E: Engine + 'static> Session<E>  {
    // 执行客户端SQL语句
    pub fn execute(&mut self, sql: &str) -> LegendDBResult<ResultSet> {
        match Parser::new(sql).parse()? {
            stmt => {
                let mut txn = self.engine.begin()?;
                // 构建执行计划Plan，执行sql
                match Plan::build(stmt)?.execute(&mut txn) {
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
    
    // 获取表信息
    pub fn get_table(&self, table_name: String) -> LegendDBResult<String> {
        let txn = self.engine.begin()?;
        let table = txn.get_table_must(table_name)?;
        txn.commit()?;
        Ok(table.to_string())
    }
    
    pub fn get_table_names(&self) -> LegendDBResult<String> {
        let mut txn = self.engine.begin()?;
        let table_names = txn.get_table_names()?;
        txn.commit()?;
        Ok(table_names.join(",\n"))
    }

}