use std::collections::BTreeMap;
use std::fs;
use std::fs::File;
use bincode::{config, Decode, Encode};
use serde::{Deserialize, Serialize};
use crate::sql::engine::engine::{Engine, Session, Transaction};
use crate::sql::parser::ast::{evaluate_expr, Expression, Operation};
use crate::sql::schema::Table;
use crate::storage;
use crate::storage::engine::Engine as StorageEngine;
use crate::storage::keycode::{deserializer, serializer};
use crate::storage::mvcc::{MvccTransaction};
use crate::sql::types::{Row, Value};
use crate::custom_error::{LegendDBError, LegendDBResult, CURRENT_DB_FILE, DEFAULT_DB_FOLDER};
// KV引擎定义
#[derive(Debug)]
pub struct KVEngine<E: StorageEngine> {
    // 底层存储引擎
    pub kv: storage::mvcc::Mvcc<E>,
}

impl<E: StorageEngine> Clone for KVEngine<E>  {
    fn clone(&self) -> Self {
        Self {
            kv: self.kv.clone(),
        }
    }
}

impl<E: StorageEngine> KVEngine<E> {
    pub fn new(engine: E) -> Self {
        Self {
            kv: storage::mvcc::Mvcc::new(engine),
        }
    }
}


impl<E: StorageEngine> Engine for KVEngine<E> {
    type Transaction = KVTransaction<E>;

    fn begin(&self) -> LegendDBResult<Self::Transaction> {
        Ok(Self::Transaction::new(self.kv.begin()?))
    }

    fn session(&self) -> LegendDBResult<Session<Self>> {
        Ok(Session {
            engine: self.clone(),
            transaction: None,
        })
    }

}

// kv transaction 定义， 实际就是存储引擎中MvccTransaction的封装
#[derive(Debug, Clone)]
pub struct KVTransaction<E: StorageEngine> {
    pub txn: MvccTransaction<E>,
}

impl<E: StorageEngine> KVTransaction<E> {
    pub fn new(txn: MvccTransaction<E>) -> Self {
        KVTransaction { txn }
    }
}

impl<E: StorageEngine> Transaction for KVTransaction<E> {
    fn commit(&self) -> LegendDBResult<()> {
        Ok(self.txn.commit()?)
    }

    fn rollback(&self) -> LegendDBResult<()> {
        Ok(self.txn.rollback()?)
    }

    fn create_database(&self, name: &str) -> LegendDBResult<()> {
        // 判断数据库是否存在
        if fs::metadata(format!("{}{}.db", DEFAULT_DB_FOLDER, name)).is_ok() {
            return Err(LegendDBError::Internal(format!("database {} already exists", name)));
        } else {
            File::create(format!("{}{}.db", DEFAULT_DB_FOLDER, name))?;
        }
        Ok(())
    }
    #[allow(unused)]
    fn drop_database(&self, name: &str) -> LegendDBResult<()> {
        // 判断数据库是否存在
        if !fs::metadata(format!("{}/{}.db", DEFAULT_DB_FOLDER, name)).is_ok() {
            return Err(LegendDBError::Internal(format!("database {} not already exists", name)));
        } else {
            fs::remove_file(format!("{}/{}.db", DEFAULT_DB_FOLDER, name))?;
        }
        Ok(())
    }

    fn use_database(&self, database_name: &str) -> LegendDBResult<()> {
        // 判断数据库是否存在
        if !fs::metadata(format!("{}/{}", DEFAULT_DB_FOLDER, database_name)).is_ok() {
            return Err(LegendDBError::Internal(format!("database {} not already exists", database_name)));
        }
        // 没有文件会创建文件，并将内容写到文件中
        fs::write(CURRENT_DB_FILE, database_name)?;

        Ok(())
    }

    fn create_table(&mut self, table: Table) -> LegendDBResult<()> {
        // 判断表table否存在
        if self.get_table(table.name.clone())?.is_some() {
            return Err(LegendDBError::TableExist(table.name));
        }
        // 判断表的有效性
        table.validate()?;
        let key = TransactionKey::TableName(table.name.clone()).encode()?;
        // 简单序列化
        // let key_bytes: Vec<u8> = to_bytes::<RancorError>(&key)?.into_vec();
        // 高性能序列化
        // let mut arena = Arena::new();
        // let key_result = to_bytes_with_alloc::<_, RancorError>(&key, arena.acquire())?.into_vec();
        // let table_result = to_bytes_with_alloc::<_, RancorError>(&table, arena.acquire())?.into_vec();
        let config = config::standard();
        let table_result = bincode::encode_to_vec(table, config)?;
        self.txn.set(key, table_result)?;
        Ok(())
    }

    #[allow(unused)]
    fn drop_table(&self, name: &str) -> LegendDBResult<()> {
        todo!()
    }

    fn create_row(&mut self, table_name: String, row: Row) -> LegendDBResult<()> {
        let table = self.get_table_must(table_name.clone())?;
        // 校验行的有效性
        for (index, column) in table.columns.iter().enumerate() {
            match row[index].get_type() { 
                None if column.nullable => {},
                None => {
                    return Err(LegendDBError::Internal(format!("column {} is null", column.name)));
                },
                Some(dt) if dt != column.data_type => {
                    return Err(LegendDBError::Internal(format!("column {} type is not match", column.name)));
                },
                _ => {}
            }
        }
        // 存放数据
        // 找到表中的主键作为一行数据的唯一标识
        let primary_key = table.get_primary_key(&row)?;
        // 查看主键对应的数据是否已经存在
        let id = TransactionKey::RowKey(table_name.clone(), primary_key.clone()).encode()?;
        if self.txn.get(id.clone())?.is_some() {
            return Err(LegendDBError::Internal(format!("Duplicte data for primary key {:?} in table {}", primary_key.clone(), table_name.clone())));
        }
        let config = config::standard();
        let value = bincode::encode_to_vec(row, config)?;
        self.txn.set(id, value)?;
        Ok(())
    }

    fn update_row(&mut self, table: &Table, id: &Value, row: Row) -> LegendDBResult<()> {
        let new_pk = table.get_primary_key(&row)?;
        // 如果更新了主键，则删除旧的数据
        if new_pk != *id {
            let key = TransactionKey::RowKey(table.name.clone(), id.clone()).encode()?;
            self.txn.delete(key)?;
            // return Err(LegendDBError::Internal(format!("primary key is not match")));
        }
        let key = TransactionKey::RowKey(table.name.clone(), new_pk).encode()?;
        let value = bincode::encode_to_vec(row, config::standard())?;
        self.txn.set(key, value)?;
        Ok(())
    }

    fn delete_row(&mut self, table: &Table, id: &Value) -> LegendDBResult<()> {
        let key = TransactionKey::RowKey(table.name.clone(), id.clone()).encode()?;
        self.txn.delete(key)?;
        Ok(())
    }


    fn get_table_names(&mut self) -> LegendDBResult<Vec<String>> {
        let prefix = KeyPrefix::Table.encode()?;
        let results = self.txn.scan_prefix(prefix)?;
        let mut names = Vec::new();
        for result in results {
            let (table, _): (Table, usize) = bincode::decode_from_slice(&result.value, config::standard())?;
            names.push(table.name);
        }
        Ok(names)
    }

    fn scan_table(&mut self, table_name: String, filter: Option<Vec<Expression>>) -> LegendDBResult<Vec<Row>> {
        let table = self.get_table_must(table_name.clone())?;
        let prefix = KeyPrefix::Row(table_name.clone()).encode()?;
        let config = config::standard();
        let results = self.txn.scan_prefix(prefix)?;
        let mut rows = Vec::new();
        for result in results {
            let (row, _) = bincode::decode_from_slice(&result.value, config)?;
            // 根据filter进行过滤
            match filter {
                None => {
                    rows.push(row);
                },
                Some(ref filters) => {
                    let table_cols = table.columns.iter().map(|c| c.name.clone()).collect::<Vec<_>>();
                    for filter in filters {
                        match evaluate_expr(filter, &table_cols, &row, &table_cols, &row)? {
                            Value::Boolean(true) => {
                                rows.push(row.clone());
                            },
                            Value::Null => {}
                            Value::Boolean(false) => {}
                            _ => {
                                return Err(LegendDBError::Internal("filter is not match".to_string()));
                            }
                        }
                    }
                }
            }
        }
        Ok(rows)
    }


    fn get_table(&self, table: String) -> LegendDBResult<Option<Table>> {
        // let bytes = to_bytes::<Error>(&value).unwrap();
        // let deserialized = from_bytes::<Example, Error>(&bytes).unwrap()
        let key = TransactionKey::TableName(table).encode()?;
        let config = config::standard();
        // let mut arena = Arena::new();
        // let key_bytes = to_bytes_with_alloc::<_, RancorError>(&key, arena.acquire())?.into_vec();
        let value = self.txn.get(key)?;
        Ok(value.map(|v| {
            //Result<&ArchivedTable, RancorError>
            // let table_archived: &ArchivedTable = access::<ArchivedTable, RancorError>(&v)?;
            // deserialize::<Table, RancorError>(table_archived)
            bincode::decode_from_slice(&v, config).map(|(table, _)| table)
        }).transpose()?)
    }
}

#[derive(Debug, Clone, Encode, Decode, Serialize, Deserialize)]
pub enum TransactionKey {
    TableName(String),
    RowKey(String, Value),
}

impl TransactionKey {
    pub fn encode(&self) -> LegendDBResult<Vec<u8>> {
        serializer(self)
    }
}

#[derive(Debug, Clone, Encode, Decode, Serialize, Deserialize)]
pub enum KeyPrefix {
    Table,
    Row(String)
}

impl KeyPrefix {
    pub fn encode(&self) -> LegendDBResult<Vec<u8>> {
        serializer(self)
    }
    
    pub fn decode(input: &[u8]) -> LegendDBResult<Self> {
        deserializer(input)
    }
}

#[cfg(test)]
mod tests {
    use crate::sql::engine::engine::Engine;
    use crate::sql::executor::executor::ResultSet;
    use crate::storage::disk::DiskEngine;
    use super::KVEngine;
    use crate::storage::memory::MemoryEngine;
    use crate::custom_error::LegendDBResult;

    #[test]
    fn test_create_table() -> LegendDBResult<()> {
        let kv_engine = KVEngine::new(MemoryEngine::new());
        let mut s = kv_engine.session()?;
        s.execute("create table t1 (a int primary key, b text default 'vv', c integer default 100);")?;
        // s.execute("insert into t1 values(1, 'a', 1);")?;
        // s.execute("insert into t1 values(2, 'b');")?;
        s.execute("insert into t1(c, a) values(200, 3);")?;
        s.execute("select * from t1;")?;
        Ok(())
    }
    
    #[test]
    fn test_update() -> LegendDBResult<()> {
        let kv_engine = KVEngine::new(MemoryEngine::new());
        let mut s = kv_engine.session()?;
        s.execute("create table t1 (a int primary key, b text default 'vv', c integer default 100);")?;
        s.execute("insert into t1 values(1, 'a', 1);")?;
        s.execute("insert into t1 values(2, 'b', 2);")?;
        s.execute("update t1 set b = 'aa', c = 200  where a = 1;")?;
        Ok(())
    }

    #[test]
    fn test_delete() -> LegendDBResult<()> {
        let kv_engine = KVEngine::new(MemoryEngine::new());
        let mut s = kv_engine.session()?;
        // let config = bincode::config::standard();
        // let encode_str = bincode::encode_to_vec("1", config)?;
        // println!("{:?}", encode_str);
        // let decoded_str = bincode::decode_from_slice::<String, _>(&encode_str, config)?;
        // print!("{:?}", decoded_str);
        // assert_eq!(decoded_str.0, "1".to_string());
        s.execute("create table t1 (a int primary key, b text default 'vv', c integer default 100);")?;
        s.execute("insert into t1 values(1, 'a', 1);")?;
        s.execute("insert into t1 values(2, 'b', 2);")?;
        s.execute("insert into t1 values(3, 'b', 3);")?;
        s.execute("delete from t1 where a = 1;")?;
        match s.execute("select * from t1;")? { 
            ResultSet::Scan { columns, rows} => {
                for row in rows {
                    println!("{:?}", row);
                }
            }
            _ => unreachable!()
        }
        Ok(())
    }
    
    #[test]
    fn test_select() -> LegendDBResult<()> {
        let p = tempfile::tempdir()?.into_path().join("test.db");
        let kv_engine = KVEngine::new(DiskEngine::new(p.clone())?);
        let mut s = kv_engine.session()?;
        s.execute(
            "create table t3 (
                     a int primary key,
                     b int default 12 null,
                     c integer default 0 NULL,
                     d float not NULL
                 );",
        )?;
        s.execute("insert into t3 values (1, 34, 22, 1.22);")?;
        s.execute("insert into t3 values (4, 23, 65, 4.23);")?;
        s.execute("insert into t3 values (3, 56, 22, 2.88);")?;
        s.execute("insert into t3 values (2, 87, 57, 6.78);")?;
        s.execute("insert into t3 values (5, 87, 14, 3.28);")?;
        s.execute("insert into t3 values (7, 87, 82, 9.52);")?;

        match s.execute("select a, b as col2 from t3 order by c, a desc limit 100;")? {
            ResultSet::Scan { columns, rows } => {
                assert_eq!(2, columns.len());
                assert_eq!(6, rows.len());
            }
            _ => unreachable!(),
        }

        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }

    #[test]
    fn test_cross_join() -> LegendDBResult<()> {
        let p = tempfile::tempdir()?.into_path().join("sqldb-log");
        let kvengine = KVEngine::new(DiskEngine::new(p.clone())?);
        let mut s = kvengine.session()?;
        s.execute("create table t1 (a int primary key);")?;
        s.execute("create table t2 (b int primary key);")?;
        s.execute("create table t3 (c int primary key);")?;

        s.execute("insert into t1 values (1), (2), (3);")?;
        s.execute("insert into t2 values (2), (3), (4);")?;
        // s.execute("insert into t3 values (7), (8), (9);")?;

        match s.execute("select * from t1 left join t2 on a = b;")? {
            ResultSet::Scan { columns, rows } => {
                // assert_eq!(3, columns.len());
                // assert_eq!(27, rows.len());
                for row in rows {
                    println!("{:?}", row);
                }
            }
            _ => unreachable!(),
        }

        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }

    #[test]
    fn test_create_database() -> LegendDBResult<()> {
        let p = tempfile::tempdir()?.into_path().join("sqldb-log");
        let kvengine = KVEngine::new(DiskEngine::new(p.clone())?);
        let mut s = kvengine.session()?;
        s.execute("create database test;")?;
        Ok(())
    }

    #[test]
    fn test_drop_database() -> LegendDBResult<()> {
        let p = tempfile::tempdir()?.into_path().join("sqldb-log");
        let kvengine = KVEngine::new(DiskEngine::new(p.clone())?);
        let mut s = kvengine.session()?;
        s.execute("drop database test;")?;
        Ok(())
    }
    
    #[test]
    fn test_agg() -> LegendDBResult<()> {
        let p = tempfile::tempdir()?.into_path().join("sqldb-log");
        let kvengine = KVEngine::new(DiskEngine::new(p.clone())?);
        let mut s = kvengine.session()?;
        s.execute("create table t1 (a int primary key, b text, c float);")?;
        s.execute("insert into t1 values (1, 'a', 1.1);")?;
        s.execute("insert into t1 values (2, 'b', 2.2);")?;
        s.execute("insert into t1 values (3, 'c', 3.3);")?;
        match s.execute("select min(c) as ffffff from t1;")? {
            ResultSet::Scan { columns, rows } => {
                print!("{:?}", columns);
                print!("{:?}", rows);
            }
            _ => unreachable!(),
        }
        Ok(())
    }

    #[test]
    fn test_agg_group() -> LegendDBResult<()> {
        let p = tempfile::tempdir()?.into_path().join("sqldb-log");
        let kvengine = KVEngine::new(DiskEngine::new(p.clone())?);
        let mut s = kvengine.session()?;
        s.execute("create table t1 (a int primary key, b text, c float);")?;
        s.execute("insert into t1 values (1, 'a', 1.1);")?;
        s.execute("insert into t1 values (2, 'c', 2.2);")?;
        s.execute("insert into t1 values (3, 'a', 3.3);")?;
        s.execute("insert into t1 values (4, 'c', 3.3);")?;
        match s.execute("select b, min(c) from t1 group by b;")? {
            ResultSet::Scan { columns, rows } => {
                print!("{:?}", columns);
                print!("{:?}", rows);
            }
            _ => unreachable!(),
        }
        Ok(())
    }

    #[test]
    fn test_where_great_group() -> LegendDBResult<()> {
        let p = tempfile::tempdir()?.into_path().join("sqldb-log");
        let kvengine = KVEngine::new(DiskEngine::new(p.clone())?);
        let mut s = kvengine.session()?;
        s.execute("create table t1 (a int primary key, b text, c float);")?;
        s.execute("insert into t1 values (1, 'a', 1.1);")?;
        s.execute("insert into t1 values (2, 'c', 2.2);")?;
        s.execute("insert into t1 values (3, 'a', 3.3);")?;
        s.execute("insert into t1 values (4, 'c', 3.3);")?;
        // match s.execute("select * from t1 where a > 2;")? {
        //     ResultSet::Scan { columns, rows } => {
        //         println!("{:?}", columns);
        //         println!("{:?}", rows);
        //     }
        //     _ => unreachable!(),
        // }

        match s.execute("select b, sum(c) from t1 having sum > 5; ")? {
            ResultSet::Scan { columns, rows } => {
                println!("{:?}", columns);
                println!("{:?}", rows);
            }
            _ => unreachable!(),
        }
        Ok(())
    }
}