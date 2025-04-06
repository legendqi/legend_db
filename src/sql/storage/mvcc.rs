use std::collections::{BTreeMap, HashSet};
use std::sync::{Arc, Mutex, MutexGuard};
use bincode::{config, Decode, Encode};
use serde::{Deserialize, Serialize};
use crate::sql::storage::engine::Engine;
use crate::sql::storage::keycode::{deserializer, serializer};
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
        Ok(serializer(&self)?)
    }

    pub fn decode(data: &[u8]) -> LegendDBResult<Self> {
        Ok(deserializer(data)?)
    }
}
// 事务号前缀枚举
#[derive(Debug, Clone, Encode, Decode, Serialize, Deserialize)]
pub enum MvccKeyPrefix {
    NextVersion,
    TxnActive,
    TxnWrite(Version),
    Version(#[serde(with = "serde_bytes")] Vec<u8>),
}

#[allow(unused)]
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
        let mut engine = self.engine.lock()?;
        // vec![]和 Vec::new()在创建空数组时几乎没有区别，但宏的方式会可能会有一些编译时开销
        // let mut delete_keys = vec![];
        let mut delete_keys = Vec::new();
        // 找到这个当前事务的Txn Write 的信息
        let mut txns = engine.scan_prefix(MvccKeyPrefix::TxnWrite(self.state.version).encode()?);
        while let Some((key, _)) = txns.next().transpose()?{
            delete_keys.push(key)
        }
        // 在扫描的时候，engine生命周期并未结束，导致下面使用 engine删除的时候会报错，所以需要手动结束Iterator的生命周期
        drop(txns);
        for key in delete_keys.into_iter() {
            engine.delete(key)?;
        }
        // 从活跃事务列表中删除当前事务
        engine.delete(MvccKey::TxnActive(self.state.version).encode()?)
    }
    // 回滚事务基本上跟提交事务差不多，还会多一步，将事务存储的数据删除
    pub fn rollback(&self) -> LegendDBResult<()> {
        let mut engine = self.engine.lock()?;
        // vec![]和 Vec::new()在创建空数组时几乎没有区别，但宏的方式会可能会有一些编译时开销
        // let mut delete_keys = vec![];
        let mut delete_keys = Vec::new();
        // 找到这个当前事务的Txn Write 的信息
        let mut txns = engine.scan_prefix(MvccKeyPrefix::TxnWrite(self.state.version).encode()?);
        while let Some((key, _)) = txns.next().transpose()?{
            match MvccKey::decode(&key)? {
                // 原始的key
                MvccKey::TxnWrite(_, key) => {
                    // 拿到原始的key之后要构造MvccKey::Version的key, 通过这个key就能拿到实际用户存储的数据
                    delete_keys.push(MvccKey::Version(key, self.state.version).encode()?)
                },
                _ => {
                    return Err(LegendDBError::Internal(format!("unexpected key {:?}", String::from_utf8(key))))
                }
            }
            delete_keys.push(key)
        }
        // 在扫描的时候，engine生命周期并未结束，导致下面使用 engine删除的时候会报错，所以需要手动结束Iterator的生命周期
        drop(txns);
        for key in delete_keys.into_iter() {
            engine.delete(key)?;
        }
        // 从活跃事务列表中删除当前事务
        engine.delete(MvccKey::TxnActive(self.state.version).encode()?)
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
        // 假如当前的version是9
        // 可见版本就小于等于9，就需要扫描0到9的数据
        let from = MvccKey::Version(key.clone(), 0).encode()?;
        let to = MvccKey::Version(key.clone(), self.state.version).encode()?;
        // rev反转，肯定是从最新事务号开始找
        let mut iter = engine.scan(from..=to).rev();
        // 从最新的版本开始读取，找到一个最新可见的版本
        while let Some((k, v)) = iter.next().transpose()? {
            match MvccKey::decode(&k)? {
                MvccKey::Version(_, version) => {
                    // 检测这个 version 是否是可见的
                    if self.state.is_visible(version) {
                        // 如果是可见的，那么就返回这个值
                        return Ok(bincode::decode_from_slice(&v, config::standard())?.0);
                    }
                }
                _=> {
                    return Err(LegendDBError::Internal(format!("unexpected key {:?}", String::from_utf8(key))))
                }
            }
        }
        Ok(None)
    }
    
    pub fn scan_prefix(&mut self, prefix: Vec<u8>) -> LegendDBResult<Vec<ScanResult>> {
        let mut engine = self.engine.lock()?;
        let mut enc_prefix = MvccKeyPrefix::Version(prefix.clone()).encode()?;
        // 原始值           编码后
        // 97 98 99     -> 97 98 99 0 0
        // 前缀原始值        前缀编码后
        // 97 98        -> 97 98 0 0         -> 97 98
        // 去掉最后的 [0, 0] 后缀
        enc_prefix.truncate(enc_prefix.len() - 2);
        let mut iter = engine.scan_prefix(enc_prefix);
        let mut results = BTreeMap::new();
        while let Some((key, value)) = iter.next().transpose()? {
            match MvccKey::decode(&key)? {
                MvccKey::Version(raw_key, version) => {
                    if self.state.is_visible(version) {
                        match bincode::decode_from_slice(&value, config::standard())? {
                            (Some(raw_value), _) => {
                                results.insert(raw_key, raw_value);
                            },
                            (None, _) => {
                                return Err(LegendDBError::Internal(format!(
                                    "Unexepected value {:?}",
                                    String::from_utf8(value)
                                )))
                            },
                        };
                    }
                }
                _ => {
                    return Err(LegendDBError::Internal(format!(
                        "Unexepected key {:?}",
                        String::from_utf8(key)
                    )))
                }
            }
        }

        Ok(results
            .into_iter()
            .map(|(key, value)| ScanResult { key, value })
            .collect())
    }

    // 获取当前活跃事务列表
    pub fn get_active_txns(engine: &mut MutexGuard<E>) -> LegendDBResult<HashSet<Version>> {
        let mut active_txns = HashSet::new();
        let mut txn_iter = engine.scan_prefix(MvccKeyPrefix::TxnActive.encode()?);
        while let Some((key, _)) = txn_iter.next().transpose()? {
            match MvccKey::decode(&key)? {
                MvccKey::TxnActive(version) => {
                    active_txns.insert(version);
                },
                _ => {
                    return Err(LegendDBError::Internal(format!("unexpected key: {:?}", String::from_utf8(key))));
                }
                }

            }
        Ok(active_txns)
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct ScanResult {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

#[cfg(test)]
mod tests {
    use crate::sql::storage::disk::DiskEngine;
    use crate::sql::storage::engine::Engine;
    use crate::sql::storage::memory::MemoryEngine;
    use crate::sql::storage::mvcc::Mvcc;
    use crate::utils::custom_error::{LegendDBResult};

    // 1. Get
    fn get(eng: impl Engine) -> LegendDBResult<()> {
        let mvcc = Mvcc::new(eng);
        let tx = mvcc.begin()?;
        tx.set(b"key1".to_vec(), b"val1".to_vec())?;
        tx.set(b"key2".to_vec(), b"val2".to_vec())?;
        tx.set(b"key2".to_vec(), b"val3".to_vec())?;
        tx.set(b"key3".to_vec(), b"val4".to_vec())?;
        tx.delete(b"key3".to_vec())?;
        tx.commit()?;

        let tx1 = mvcc.begin()?;
        assert_eq!(tx1.get(b"key1".to_vec())?, Some(b"val1".to_vec()));
        assert_eq!(tx1.get(b"key2".to_vec())?, Some(b"val3".to_vec()));
        assert_eq!(tx1.get(b"key3".to_vec())?, None);

        Ok(())
    }

    #[test]
    fn test_get() -> LegendDBResult<()> {
        println!("test get");
        get(MemoryEngine::new())?;
        let p = tempfile::tempdir()?.into_path().join("sqldb-log");
        get(DiskEngine::new(p.clone())?)?;
        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }

    // 2. Get Isolation
    fn get_isolation(eng: impl Engine) -> LegendDBResult<()> {
        let mvcc = Mvcc::new(eng);
        let tx = mvcc.begin()?;
        tx.set(b"key1".to_vec(), b"val1".to_vec())?;
        tx.set(b"key2".to_vec(), b"val2".to_vec())?;
        tx.set(b"key2".to_vec(), b"val3".to_vec())?;
        tx.set(b"key3".to_vec(), b"val4".to_vec())?;
        tx.commit()?;

        let tx1 = mvcc.begin()?;
        tx1.set(b"key1".to_vec(), b"val2".to_vec())?;

        let tx2 = mvcc.begin()?;

        let tx3 = mvcc.begin()?;
        tx3.set(b"key2".to_vec(), b"val4".to_vec())?;
        tx3.delete(b"key3".to_vec())?;
        tx3.commit()?;

        assert_eq!(tx2.get(b"key1".to_vec())?, Some(b"val1".to_vec()));
        assert_eq!(tx2.get(b"key2".to_vec())?, Some(b"val3".to_vec()));
        assert_eq!(tx2.get(b"key3".to_vec())?, Some(b"val4".to_vec()));

        Ok(())
    }
    #[test]
    fn test_get_isolation() -> LegendDBResult<()> {
        get_isolation(MemoryEngine::new())?;

        let p = tempfile::tempdir()?.into_path().join("sqldb-log");
        get_isolation(DiskEngine::new(p.clone())?)?;
        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }

    // 3. scan prefix
    fn scan_prefix(eng: impl Engine) -> LegendDBResult<()> {
        let mvcc = Mvcc::new(eng);
        let tx = mvcc.begin()?;
        tx.set(b"aabb".to_vec(), b"val1".to_vec())?;
        tx.set(b"abcc".to_vec(), b"val2".to_vec())?;
        tx.set(b"bbaa".to_vec(), b"val3".to_vec())?;
        tx.set(b"acca".to_vec(), b"val4".to_vec())?;
        tx.set(b"aaca".to_vec(), b"val5".to_vec())?;
        tx.set(b"bcca".to_vec(), b"val6".to_vec())?;
        tx.commit()?;

        let mut tx1 = mvcc.begin()?;
        let iter1 = tx1.scan_prefix(b"aa".to_vec())?;
        assert_eq!(
            iter1,
            vec![
                super::ScanResult {
                    key: b"aabb".to_vec(),
                    value: b"val1".to_vec()
                },
                super::ScanResult {
                    key: b"aaca".to_vec(),
                    value: b"val5".to_vec()
                },
            ]
        );

        let iter2 = tx1.scan_prefix(b"a".to_vec())?;
        assert_eq!(
            iter2,
            vec![
                super::ScanResult {
                    key: b"aabb".to_vec(),
                    value: b"val1".to_vec()
                },
                super::ScanResult {
                    key: b"aaca".to_vec(),
                    value: b"val5".to_vec()
                },
                super::ScanResult {
                    key: b"abcc".to_vec(),
                    value: b"val2".to_vec()
                },
                super::ScanResult {
                    key: b"acca".to_vec(),
                    value: b"val4".to_vec()
                },
            ]
        );

        let iter3 = tx1.scan_prefix(b"bcca".to_vec())?;
        assert_eq!(
            iter3,
            vec![super::ScanResult {
                key: b"bcca".to_vec(),
                value: b"val6".to_vec()
            },]
        );

        Ok(())
    }

    #[test]
    fn test_scan_prefix() -> LegendDBResult<()> {
        scan_prefix(MemoryEngine::new())?;
        let p = tempfile::tempdir()?.into_path().join("sqldb-log");
        scan_prefix(DiskEngine::new(p.clone())?)?;
        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }

    // 4. scan isolation
    fn scan_isolation(eng: impl Engine) -> LegendDBResult<()> {
        let mvcc = Mvcc::new(eng);
        let tx = mvcc.begin()?;
        tx.set(b"aabb".to_vec(), b"val1".to_vec())?;
        tx.set(b"abcc".to_vec(), b"val2".to_vec())?;
        tx.set(b"bbaa".to_vec(), b"val3".to_vec())?;
        tx.set(b"acca".to_vec(), b"val4".to_vec())?;
        tx.set(b"aaca".to_vec(), b"val5".to_vec())?;
        tx.set(b"bcca".to_vec(), b"val6".to_vec())?;
        tx.commit()?;

        let mut tx1 = mvcc.begin()?;
        let tx2 = mvcc.begin()?;
        tx2.set(b"acca".to_vec(), b"val4-1".to_vec())?;
        tx2.set(b"aabb".to_vec(), b"val1-1".to_vec())?;

        let tx3 = mvcc.begin()?;
        tx3.set(b"bbaa".to_vec(), b"val3-1".to_vec())?;
        tx3.delete(b"bcca".to_vec())?;
        tx3.commit()?;

        let iter1 = tx1.scan_prefix(b"aa".to_vec())?;
        assert_eq!(
            iter1,
            vec![
                super::ScanResult {
                    key: b"aabb".to_vec(),
                    value: b"val1".to_vec()
                },
                super::ScanResult {
                    key: b"aaca".to_vec(),
                    value: b"val5".to_vec()
                },
            ]
        );

        let iter2 = tx1.scan_prefix(b"a".to_vec())?;
        assert_eq!(
            iter2,
            vec![
                super::ScanResult {
                    key: b"aabb".to_vec(),
                    value: b"val1".to_vec()
                },
                super::ScanResult {
                    key: b"aaca".to_vec(),
                    value: b"val5".to_vec()
                },
                super::ScanResult {
                    key: b"abcc".to_vec(),
                    value: b"val2".to_vec()
                },
                super::ScanResult {
                    key: b"acca".to_vec(),
                    value: b"val4".to_vec()
                },
            ]
        );

        let iter3 = tx1.scan_prefix(b"bcca".to_vec())?;
        assert_eq!(
            iter3,
            vec![super::ScanResult {
                key: b"bcca".to_vec(),
                value: b"val6".to_vec()
            },]
        );

        Ok(())
    }

    #[test]
    fn test_scan_isolation() -> LegendDBResult<()> {
        scan_isolation(MemoryEngine::new())?;
        let p = tempfile::tempdir()?.into_path().join("sqldb-log");
        scan_isolation(DiskEngine::new(p.clone())?)?;
        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }

    // 5. set
    fn set(eng: impl Engine) -> LegendDBResult<()> {
        let mvcc = Mvcc::new(eng);
        let tx = mvcc.begin()?;
        tx.set(b"key1".to_vec(), b"val1".to_vec())?;
        tx.set(b"key2".to_vec(), b"val2".to_vec())?;
        tx.set(b"key2".to_vec(), b"val3".to_vec())?;
        tx.set(b"key3".to_vec(), b"val4".to_vec())?;
        tx.set(b"key4".to_vec(), b"val5".to_vec())?;
        tx.commit()?;

        let tx1 = mvcc.begin()?;
        let tx2 = mvcc.begin()?;

        tx1.set(b"key1".to_vec(), b"val1-1".to_vec())?;
        tx1.set(b"key2".to_vec(), b"val3-1".to_vec())?;
        tx1.set(b"key2".to_vec(), b"val3-2".to_vec())?;

        tx2.set(b"key3".to_vec(), b"val4-1".to_vec())?;
        tx2.set(b"key4".to_vec(), b"val5-1".to_vec())?;

        tx1.commit()?;
        tx2.commit()?;

        let tx = mvcc.begin()?;
        assert_eq!(tx.get(b"key1".to_vec())?, Some(b"val1-1".to_vec()));
        assert_eq!(tx.get(b"key2".to_vec())?, Some(b"val3-2".to_vec()));
        assert_eq!(tx.get(b"key3".to_vec())?, Some(b"val4-1".to_vec()));
        assert_eq!(tx.get(b"key4".to_vec())?, Some(b"val5-1".to_vec()));
        Ok(())
    }

    #[test]
    fn test_set() -> LegendDBResult<()> {
        set(MemoryEngine::new())?;
        let p = tempfile::tempdir()?.into_path().join("sqldb-log");
        set(DiskEngine::new(p.clone())?)?;
        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }

    // 6. set conflict
    fn set_conflict(eng: impl Engine) -> LegendDBResult<()> {
        let mvcc = Mvcc::new(eng);
        let tx = mvcc.begin()?;
        tx.set(b"key1".to_vec(), b"val1".to_vec())?;
        tx.set(b"key2".to_vec(), b"val2".to_vec())?;
        tx.set(b"key2".to_vec(), b"val3".to_vec())?;
        tx.set(b"key3".to_vec(), b"val4".to_vec())?;
        tx.set(b"key4".to_vec(), b"val5".to_vec())?;
        tx.commit()?;

        let tx1 = mvcc.begin()?;
        // let tx2 = mvcc.begin()?;

        tx1.set(b"key1".to_vec(), b"val1-1".to_vec())?;
        tx1.set(b"key1".to_vec(), b"val1-2".to_vec())?;

        tx1.commit()?;
        Ok(())
    }

    #[test]
    fn test_set_conflict() -> LegendDBResult<()> {
        set_conflict(MemoryEngine::new())?;
        let p = tempfile::tempdir()?.into_path().join("sqldb-log");
        set_conflict(DiskEngine::new(p.clone())?)?;
        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }

    // 7. delete
    fn delete(eng: impl Engine) -> LegendDBResult<()> {
        let mvcc = Mvcc::new(eng);
        let tx = mvcc.begin()?;
        tx.set(b"key1".to_vec(), b"val1".to_vec())?;
        tx.set(b"key2".to_vec(), b"val2".to_vec())?;
        tx.set(b"key3".to_vec(), b"val3".to_vec())?;
        tx.delete(b"key2".to_vec())?;
        tx.delete(b"key3".to_vec())?;
        tx.set(b"key3".to_vec(), b"val3-1".to_vec())?;
        tx.commit()?;

        let mut tx1 = mvcc.begin()?;
        assert_eq!(tx1.get(b"key2".to_vec())?, None);

        let iter = tx1.scan_prefix(b"ke".to_vec())?;
        assert_eq!(
            iter,
            vec![
                super::ScanResult {
                    key: b"key1".to_vec(),
                    value: b"val1".to_vec()
                },
                super::ScanResult {
                    key: b"key3".to_vec(),
                    value: b"val3-1".to_vec()
                },
            ]
        );
        Ok(())
    }

    #[test]
    fn test_delete() -> LegendDBResult<()> {
        delete(MemoryEngine::new())?;
        let p = tempfile::tempdir()?.into_path().join("sqldb-log");
        delete(DiskEngine::new(p.clone())?)?;
        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }

    // 8. delete conflict
    fn delete_conflict(eng: impl Engine) -> LegendDBResult<()> {
        let mvcc = Mvcc::new(eng);
        let tx = mvcc.begin()?;
        tx.set(b"key1".to_vec(), b"val1".to_vec())?;
        tx.set(b"key2".to_vec(), b"val2".to_vec())?;
        tx.commit()?;

        let tx1 = mvcc.begin()?;
        // let tx2 = mvcc.begin()?;
        tx1.delete(b"key1".to_vec())?;
        tx1.set(b"key2".to_vec(), b"val2-1".to_vec())?;

        Ok(())
    }

    #[test]
    fn test_delete_conflict() -> LegendDBResult<()> {
        delete_conflict(MemoryEngine::new())?;
        let p = tempfile::tempdir()?.into_path().join("sqldb-log");
        delete_conflict(DiskEngine::new(p.clone())?)?;
        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }

    // 9. dirty read
    fn dirty_read(eng: impl Engine) -> LegendDBResult<()> {
        let mvcc = Mvcc::new(eng);
        let tx = mvcc.begin()?;
        tx.set(b"key1".to_vec(), b"val1".to_vec())?;
        tx.set(b"key2".to_vec(), b"val2".to_vec())?;
        tx.set(b"key3".to_vec(), b"val3".to_vec())?;
        tx.commit()?;

        let tx1 = mvcc.begin()?;
        let tx2 = mvcc.begin()?;

        tx2.set(b"key1".to_vec(), b"val1-1".to_vec())?;
        assert_eq!(tx1.get(b"key1".to_vec())?, Some(b"val1".to_vec()));

        Ok(())
    }

    #[test]
    fn test_dirty_read() -> LegendDBResult<()> {
        dirty_read(MemoryEngine::new())?;
        let p = tempfile::tempdir()?.into_path().join("sqldb-log");
        dirty_read(DiskEngine::new(p.clone())?)?;
        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }

    // 10. unrepeatable read
    fn unrepeatable_read(eng: impl Engine) -> LegendDBResult<()> {
        let mvcc = Mvcc::new(eng);
        let tx = mvcc.begin()?;
        tx.set(b"key1".to_vec(), b"val1".to_vec())?;
        tx.set(b"key2".to_vec(), b"val2".to_vec())?;
        tx.set(b"key3".to_vec(), b"val3".to_vec())?;
        tx.commit()?;

        let tx1 = mvcc.begin()?;
        let tx2 = mvcc.begin()?;

        tx2.set(b"key1".to_vec(), b"val1-1".to_vec())?;
        assert_eq!(tx1.get(b"key1".to_vec())?, Some(b"val1".to_vec()));
        tx2.commit()?;
        assert_eq!(tx1.get(b"key1".to_vec())?, Some(b"val1".to_vec()));

        Ok(())
    }

    #[test]
    fn test_unrepeatable_read() -> LegendDBResult<()> {
        unrepeatable_read(MemoryEngine::new())?;
        let p = tempfile::tempdir()?.into_path().join("sqldb-log");
        unrepeatable_read(DiskEngine::new(p.clone())?)?;
        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }

    // 11. phantom read
    fn phantom_read(eng: impl Engine) -> LegendDBResult<()> {
        let mvcc = Mvcc::new(eng);
        let tx = mvcc.begin()?;
        tx.set(b"key1".to_vec(), b"val1".to_vec())?;
        tx.set(b"key2".to_vec(), b"val2".to_vec())?;
        tx.set(b"key3".to_vec(), b"val3".to_vec())?;
        tx.commit()?;

        let mut tx1 = mvcc.begin()?;
        let tx2 = mvcc.begin()?;

        let iter1 = tx1.scan_prefix(b"key".to_vec())?;
        assert_eq!(
            iter1,
            vec![
                super::ScanResult {
                    key: b"key1".to_vec(),
                    value: b"val1".to_vec()
                },
                super::ScanResult {
                    key: b"key2".to_vec(),
                    value: b"val2".to_vec()
                },
                super::ScanResult {
                    key: b"key3".to_vec(),
                    value: b"val3".to_vec()
                },
            ]
        );

        tx2.set(b"key2".to_vec(), b"val2-1".to_vec())?;
        tx2.set(b"key4".to_vec(), b"val4".to_vec())?;
        tx2.commit()?;

        let iter1 = tx1.scan_prefix(b"key".to_vec())?;
        assert_eq!(
            iter1,
            vec![
                super::ScanResult {
                    key: b"key1".to_vec(),
                    value: b"val1".to_vec()
                },
                super::ScanResult {
                    key: b"key2".to_vec(),
                    value: b"val2".to_vec()
                },
                super::ScanResult {
                    key: b"key3".to_vec(),
                    value: b"val3".to_vec()
                },
            ]
        );
        Ok(())
    }

    #[test]
    fn test_phantom_read() -> LegendDBResult<()> {
        phantom_read(MemoryEngine::new())?;
        let p = tempfile::tempdir()?.into_path().join("sqldb-log");
        phantom_read(DiskEngine::new(p.clone())?)?;
        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }

    // 12. rollback
    fn rollback(eng: impl Engine) -> LegendDBResult<()> {
        let mvcc = Mvcc::new(eng);
        let tx = mvcc.begin()?;
        tx.set(b"key1".to_vec(), b"val1".to_vec())?;
        tx.set(b"key2".to_vec(), b"val2".to_vec())?;
        tx.set(b"key3".to_vec(), b"val3".to_vec())?;
        tx.commit()?;

        let tx1 = mvcc.begin()?;
        tx1.set(b"key1".to_vec(), b"val1-1".to_vec())?;
        tx1.set(b"key2".to_vec(), b"val2-1".to_vec())?;
        tx1.set(b"key3".to_vec(), b"val3-1".to_vec())?;
        tx1.rollback()?;

        let tx2 = mvcc.begin()?;
        assert_eq!(tx2.get(b"key1".to_vec())?, Some(b"val1".to_vec()));
        assert_eq!(tx2.get(b"key2".to_vec())?, Some(b"val2".to_vec()));
        assert_eq!(tx2.get(b"key3".to_vec())?, Some(b"val3".to_vec()));

        Ok(())
    }

    #[test]
    fn test_rollback() -> LegendDBResult<()> {
        rollback(MemoryEngine::new())?;
        let p = tempfile::tempdir()?.into_path().join("sqldb-log");
        rollback(DiskEngine::new(p.clone())?)?;
        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }
}
