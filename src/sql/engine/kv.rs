use crate::sql::engine::{Engine, Session, Transaction};
use crate::sql::schema::Table;
use crate::sql::storage;
use crate::sql::storage::MvccTraction;
use crate::sql::types::Row;
use crate::utils::custom_error::LegendDBResult;

// KV引擎定义
#[derive(Debug, Clone)]
pub struct KVEngine {
    // 底层存储引擎
    pub kv: storage::Mvcc,
}


impl Engine for KVEngine {
    type Transaction = KVTransaction;

    fn begin(&self) -> LegendDBResult<Self::Transaction> {
        Ok(Self::Transaction::new(self.kv.begin()?))
    }

    fn session(&self) -> LegendDBResult<Session<Self>> {
        todo!()
    }

}

// kv transaction 定义， 实际就是存储引擎中MvccTransaction的封装
#[derive(Debug, Clone)]
pub struct KVTransaction {
    pub txn: MvccTraction,
}

impl KVTransaction {
    pub fn new(txn: MvccTraction) -> Self {
        KVTransaction { txn }
    }
}

impl Transaction for KVTransaction {
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

    fn create_table(&mut self, table: String, row: Row) -> LegendDBResult<()> {
        todo!()
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
        todo!()
    }
}