use std::sync::{Arc, Mutex};
use crate::sql::storage::engine::Engine;
use crate::utils::custom_error::LegendDBResult;

#[derive(Debug)]
pub struct Mvcc<E: Engine> {
    // 多线程运行，所以用Arc对象
    engine: Arc<Mutex<E>>,
}

impl<E: Engine> Clone for Mvcc<E>  {
    fn clone(&self) -> Self {
        Self {
            engine: self.engine.clone(),
        }
    }
}

impl<E: Engine> Mvcc<E>  {
    pub fn new(engine: E) -> Self {
        Self {
            engine: Arc::new(Mutex::new(engine))
        }
    }

    pub fn begin(&self) -> LegendDBResult<MvccTransaction<E>> {
        Ok(MvccTransaction::begin(self.engine.clone()))
    }
}

#[derive(Debug, Clone)]
pub struct MvccTransaction<E: Engine> {
    engine: Arc<Mutex<E>>,
}

impl<E: Engine> MvccTransaction<E> {
    pub fn begin(engine: Arc<Mutex<E>>) -> Self {
        Self {
            engine,
        }
    }
    
    pub fn commit(&self) -> LegendDBResult<()> {
        Ok(())
    }
    
    pub fn rollback(&self) -> LegendDBResult<()> {
        Ok(())
    }
    
    pub fn set(&self, key: Vec<u8>, value: Vec<u8>) -> LegendDBResult<()> {
        let mut engine = self.engine.lock()?;
        engine.set(key, value)
    }
    
    pub(crate) fn get(&self, key: Vec<u8>) -> LegendDBResult<Option<Vec<u8>>> {
        let engine = self.engine.lock()?;
        engine.get(key)
    }
    
    pub fn scan_prefix(&self, prefix: Vec<u8>) -> LegendDBResult<Vec<ScanResult>> {
        let engine = self.engine.lock()?;
        let mut iter = engine.scan_prefix(prefix);
        let mut results = Vec::new();
        while let Some((key, value)) = iter.next().transpose()? {
            results.push(ScanResult {
                key,
                value,
            });
        }
        Ok(results)
    }
}

pub struct ScanResult {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}