use std::collections::HashSet;
use std::sync::{Arc, Mutex, MutexGuard};
use bincode::{config, Decode, Encode};
use serde::{Deserialize, Serialize};
use crate::sql::storage::engine::Engine;
use crate::utils::custom_error::{LegendDBError, LegendDBResult};

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
        Ok(MvccTransaction::begin(self.engine.clone())?)
    }
}

#[derive(Debug, Clone)]
pub struct MvccTransaction<E: Engine> {
    engine: Arc<Mutex<E>>,
    state: MvccTransactionStat,
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct MvccTransactionStat {
    // 当前事务版本号
    version: Version,
    // 当前事务活跃的事务列表
    active_versions: HashSet<Version>,
}

impl MvccTransactionStat {
    pub fn is_visible(&self, version: Version) -> bool {
        if self.active_versions.contains(&version) {
            true
        } else {
            version <= self.version
        }
    }
}

pub type Version = u64;

// 事务号枚举
#[derive(Debug, Encode, Decode, Serialize, Deserialize)]
pub enum MvccKey {
    NextVersion,
    TxnActive(Version),
    TxnWrite(Version, #[serde(with = "serde_bytes")] Vec<u8>),
    Version(#[serde(with = "serde_bytes")]Vec<u8>, Version)
}

impl Clone for MvccKey {
    fn clone(&self) -> Self {
        match self {
            MvccKey::NextVersion => MvccKey::NextVersion,
            MvccKey::TxnActive(version) => MvccKey::TxnActive(version.clone()),
            MvccKey::TxnWrite(key, version) => MvccKey::TxnWrite(key.clone(), version.clone()),
            MvccKey::Version(key, version) => MvccKey::Version(key.clone(), version.clone()),
        }
    }
}

// NextVersion 0
// TxnActive 1-100 1-101 1-102
// Version Key1-100 Key2-100
//

impl MvccKey {
    pub fn encode(&self) -> LegendDBResult<Vec<u8>> {
        Ok(bincode::encode_to_vec(&MvccKey::NextVersion, config::standard())?)
    }

    pub fn decode(data: &[u8]) -> LegendDBResult<Self> {
        bincode::decode_from_slice(data, config::standard())
            .map(|(key, _)| key)
            .map_err(|e| e.into())
    }
}
// 事务号前缀枚举
#[derive(Debug, Clone, Encode, Decode)]
pub enum MvccKeyPrefix {
    NextVersion,
    TxnActive
}

impl MvccKeyPrefix {
    pub fn encode(&self) -> LegendDBResult<Vec<u8>> {
        Ok(bincode::encode_to_vec(&MvccKey::NextVersion, config::standard())?)
    }

    pub fn decode(data: &[u8]) -> LegendDBResult<Self> {
        bincode::decode_from_slice(data, config::standard())
            .map(|(key, _)| key)
            .map_err(|e| e.into())
    }
}

impl<E: Engine> MvccTransaction<E> {

    // 开启事务
    pub fn begin(eng: Arc<Mutex<E>>) -> LegendDBResult<Self> {
        // 获取存储引擎
        let mut engine = eng.lock()?;
        // 获取最新的事务号
        let next_version = match engine.get(MvccKey::NextVersion.encode()?) {
            Ok(Some(data)) => {
                let next_version = bincode::decode_from_slice::<u64, _>(&data, config::standard())
                    .map(|(version, _)| version)
                    .map_err(|e| LegendDBError::EncodeError(e.to_string()))?;
                next_version + 1
            },
            Ok(None) => 1,
            Err(e) => return Err(e.into()),
        };
        // 保存下一个事务号
        engine.set(MvccKey::NextVersion.encode()?, bincode::encode_to_vec(&(next_version + 1), config::standard())?)?;
        // 获取当前活跃的事务列表
        let active_versions = Self::get_active_txns(&mut engine)?;
        // 当前事务加入到活跃事务列表中
        engine.set(MvccKey::TxnActive(next_version).encode()?, vec![])?;
        Ok(Self {
            engine: eng.clone(),
            state: MvccTransactionStat {
                version: next_version,
                active_versions,
            }
        })
    }
    
    pub fn commit(&self) -> LegendDBResult<()> {
        Ok(())
    }
    
    pub fn rollback(&self) -> LegendDBResult<()> {
        Ok(())
    }
    
    pub fn set(&self, key: Vec<u8>, value: Vec<u8>) -> LegendDBResult<()> {
        self.write_inner(key, Some(value))
    }

    pub fn delete(&self, key: Vec<u8>) -> LegendDBResult<()> {
        self.write_inner(key, None)
    }

    // 更新/删除数据
    fn write_inner(&self, key: Vec<u8>, value: Option<Vec<u8>>) -> LegendDBResult<()> {
        let mut engine = self.engine.lock()?;
        // 检测冲突， 扫描活跃的事务列表
        // 3 4 5
        // key1-3 key2-4 key3-5
        // 当前写入的事务号为6
        // 扫描从3开始扫描，扫描到最大的事务号，最大的事务号不一定是6，因为可能此时有新的事务7 8 9等，已经对key做过修改了
        // 没有活跃的事务，那么最大的事务号就是当前事务号 + 1
        let from = MvccKey::Version(
            key.clone(),
            self.state.active_versions
                .iter()
                .min()
                .copied()
                .unwrap_or(self.state.version + 1))
            .encode()?;
        let to = MvccKey::Version(key.clone(), u64::MAX).encode()?;
        //只需要判断最后一个版本号
        // 因为
        // 1， key是按顺序排列的， 扫描出来的结果是从小到大的
        // 2， 假如有的事务修改了这个key，比如 10 那么当前事务号6 再修改就是冲突的
        // 3， 如果是当前活跃事务修改了这个key, 比如4修改了这个key，那么5也会进行同样判断，那么5不可能修改
        if let Some((k, _)) = engine.scan(from..=to).last().transpose()? {
            match MvccKey::decode(&k)? {
                MvccKey::Version(_, version) => {
                    // 检测这个 version 是否是可见的
                    if !self.state.is_visible(version) {
                        return Err(LegendDBError::WriteMvccConflict);
                    }
                }
                _ => {
                    return Err(LegendDBError::Internal(format!(
                        "unexpected key: {:?}",
                        String::from_utf8(k)
                    )))
                }
            }
        }
        // 记录这个version写入了哪些key， 用于回滚事务
        engine.set(
            MvccKey::TxnWrite(self.state.version, key.clone()).encode()?,
            vec![],
        )?;
        // 写入实际的 key value数据
        engine.set(MvccKey::Version(key.clone(), self.state.version).encode()?,
                   bincode::encode_to_vec(&value, config::standard())?)?;
        Ok(())
    }
    
    pub(crate) fn get(&self, key: Vec<u8>) -> LegendDBResult<Option<Vec<u8>>> {
        let mut engine = self.engine.lock()?;
        engine.get(key)
    }
    
    pub fn scan_prefix(&mut self, prefix: Vec<u8>) -> LegendDBResult<Vec<ScanResult>> {
        let mut engine = self.engine.lock()?;
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

    // 获取当前活跃事务列表
    pub fn get_active_txns(engine: &mut MutexGuard<E>) -> LegendDBResult<HashSet<Version>> {
        let mut active_txns = HashSet::new();
        let mut txn_iter = engine.scan_prefix(MvccKeyPrefix::TxnActive.encode()?);
        while let Some((key, _)) = txn_iter.next().transpose()? {
            match MvccKey::decode(&key) {
                Ok(version) => {
                    match version {
                        MvccKey::TxnActive(version) => {
                            active_txns.insert(version);
                        },
                        _ => {
                            return Err(LegendDBError::Internal(format!("unexpected key: {:?}", version)));
                        }
                    }
                }
                _ => {
                    return Err(LegendDBError::Internal("no exists active transaction set".to_string()));
                }
            }
        }
        Ok(active_txns)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ScanResult {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}