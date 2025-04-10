use std::collections::BTreeMap;
use bincode::{config, Decode, Encode};
use serde::{Deserialize, Serialize};
use crate::sql::engine::{Engine, Session, Transaction};
use crate::sql::parser::ast::Expression;
use crate::sql::parser::lexer::Keyword::Key;
use crate::sql::schema::Table;
use crate::sql::storage;
use crate::sql::storage::engine::Engine as StorageEngine;
use crate::sql::storage::keycode::{deserializer, serializer};
use crate::sql::storage::mvcc::{MvccTransaction};
use crate::sql::types::{Row, Value};
use crate::utils::custom_error::{LegendDBError, LegendDBResult};
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

fn check_row_conditions(row: &Vec<Value>, conditions: Vec<(usize, Value)>) -> bool {
    for (index, expected_value) in conditions {
        if row.get(index) != Some(&expected_value) {
            return false;
        }
    }
    true
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
        todo!()
    }

    fn drop_database(&self, name: &str) -> LegendDBResult<()> {
        todo!()
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


    fn scan_table(&mut self, table_name: String, filter: Option<BTreeMap<String, Expression>>) -> LegendDBResult<Vec<Row>> {
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
                    let mut conditions = Vec::new();
                    for (key, value) in filters.iter() {
                        let col_index = table.get_column_index(key.as_str())?;
                        let value = Value::from_expression(value.to_owned());
                        conditions.push((col_index, value));
                    }
                    if check_row_conditions(&row, conditions) {
                        rows.push(row);
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
    use crate::sql::engine::Engine;
    use crate::sql::executor::executor::ResultSet;
    use super::KVEngine;
    use crate::sql::storage::memory::MemoryEngine;
    use crate::utils::custom_error::LegendDBResult;

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
}