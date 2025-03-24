use std::fmt::Error;
use std::io::Error;
use rkyv::{to_bytes, Archive, Archived, Deserialize, Serialize, SerializeUnsized};
use rkyv::api::test::to_archived;
use rkyv::util::AlignedVec;
use crate::sql::engine::{Engine, Session, Transaction};
use crate::sql::schema::Table;
use crate::sql::storage;
use crate::sql::storage::engine::Engine as StorageEngine;
use crate::sql::storage::mvcc::MvccTransaction;
use crate::sql::types::Row;
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


impl<E: StorageEngine> Engine for KVEngine<E> {
    type Transaction = KVTransaction<E>;

    fn begin(&self) -> LegendDBResult<Self::Transaction> {
        Ok(Self::Transaction::new(self.kv.begin()?))
    }

    fn session(&self) -> LegendDBResult<Session<Self>> {
        todo!()
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
        todo!()
    }

    fn rollback(&self) -> LegendDBResult<()> {
        todo!()
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
        if table.columns.is_empty() {
            return Err(LegendDBError::Internal(format!("table {} has no columns", table.name)));
        }
        let key = TransactionKey::TableName(table.name.clone());
        
        self.txn.set(to_bytes(&key)?.into_vec(), to_bytes(&table)?.into_vec())?;
        Ok(())
    }

    fn drop_table(&self, name: &str) -> LegendDBResult<()> {
        todo!()
    }

    fn create_row(&mut self, table: String, row: Row) -> LegendDBResult<()> {
        todo!()
    }

    fn scan_table(&self, table: String) -> LegendDBResult<Vec<Row>> {
        todo!()
    }

    fn get_table(&self, table: String) -> LegendDBResult<Option<Table>> {
        let key = TransactionKey::TableName(table);
        if let Some(value) = self.txn.get(to_bytes(&key)?.into_vec())? {
            let table:&Archived<Table>  = to_archived::<Table>(&value);
            Ok(Some(table))
        }
        todo!()
    }
}

#[derive(Debug, Archive, Serialize, Deserialize)]
pub enum TransactionKey {
    TableName(String),
    RowKey(String, String),
}