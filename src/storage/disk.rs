// 磁盘存储引擎

use std::collections::{btree_map, BTreeMap};
use std::fs::{rename, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::ops::{RangeBounds};
use std::path::PathBuf;
use fs4::fs_std::FileExt;
use btree_map::Range;
use crate::storage::engine::{Engine, EngineIterator};
use crate::custom_error::LegendDBResult;

pub type KeyDir = BTreeMap<Vec<u8>, (u64, u32)>;
// 日志文件头大小 key value 都是u32 所以是8个字节
const LOG_HEADER_SIZE: u32 = 8;


#[derive(Debug)]
pub struct DiskEngine {
    keydir: KeyDir,
    log: Log
}

impl DiskEngine {
    pub fn new(file_path: PathBuf) -> LegendDBResult<Self> {
        let mut log = Log::new(file_path)?;
        // 从 log 中去恢复的 keydir
        let keydir = log.build_keydir()?;
        Ok(Self { keydir, log })
    }

    pub fn new_compact(file_path: PathBuf) -> LegendDBResult<Self> {
        let mut eng = Self::new(file_path)?;
        eng.compact()?;
        Ok(eng)
    }


    fn compact(&mut self) -> LegendDBResult<()> {
        // 新打开一个临时的日志文件
        let mut new_path = self.log.file_path.clone();
        new_path.set_extension("compact");
        let mut new_log = Log::new(new_path)?;
        let mut new_keydir = KeyDir::new();
        // 重写数据到临时文件中
        for (key, (offset, size)) in self.keydir.iter() {
            // 读取key对应的value
            let value = self.log.read_entry(*offset, *size)?;
            // 写入新的log
            let (new_offset, new_size) = new_log.write_entry(key, Some(&value))?;
            // 更新keydir
            new_keydir.insert(key.clone(), (new_offset + new_size as u64 - *size as u64, *size));
        }
        // 将临时文件更改为正式文件
        rename(new_log.file_path, &self.log.file_path)?;
        new_log.file_path = self.log.file_path.clone();
        self.keydir = new_keydir;
        self.log = new_log;
        Ok(())
    }
}

impl Engine for DiskEngine {

    type EngineIterator<'a> = DiskEngineIterator<'a>;

    fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> LegendDBResult<()> {
        // 写日志
        let (offset, size) = self.log.write_entry(&key, Some(&value))?;
        // 更新keydir
        //100-----------------|----150
        //                    130
        // val size = 20
        let val_size = value.len() as u32;
        self.keydir.insert(key, (offset + size as u64 - val_size as u64, val_size));
        Ok(())
    }

    fn get(&mut self, key: Vec<u8>) -> LegendDBResult<Option<Vec<u8>>> {
        match self.keydir.get(&key) {
            Some((offset, size)) => {
                let value = self.log.read_entry(*offset, *size)?;
                Ok(Some(value))
            },
            None => Ok(None),
        }
    }

    fn delete(&mut self, key: Vec<u8>) -> LegendDBResult<()> {
        self.log.write_entry(&key, None)?;
        self.keydir.remove(&key);
        Ok(())
    }

    fn scan(&mut self, range: impl RangeBounds<Vec<u8>>) -> Self::EngineIterator<'_> {
        DiskEngineIterator {
            inner: self.keydir.range(range),
            log: &mut self.log,
        }
    }
    
}

pub struct DiskEngineIterator<'a> {
    inner: Range<'a, Vec<u8>, (u64, u32)>,
    log: &'a mut Log,
}

impl<'a> DiskEngineIterator<'a> {
    
    fn map(&mut self, item: (&Vec<u8>, &(u64, u32))) -> <Self as Iterator>::Item {
        let (key, (offset, size)) = item;
        let value = self.log.read_entry(*offset, *size)?;
        Ok((key.clone(), value))
    }
    
}

impl<'a> EngineIterator for DiskEngineIterator<'a> {}

impl<'a> Iterator for DiskEngineIterator<'a> {
    type Item = LegendDBResult<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|v| self.map(v))
    }
}

impl<'a> DoubleEndedIterator for DiskEngineIterator<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner.next_back().map(|v| self.map(v))
    }
}

#[derive(Debug)]
pub struct Log {
    file_path: PathBuf,
    // 磁盘文件
    file: File,
}

impl Log {

    fn new(file_path: PathBuf) -> LegendDBResult<Self> {
        // 如果目录不存在的话则创建
        // parent 获取父级目录
        if let Some(dir) = file_path.parent() {
            if !dir.exists() {
                std::fs::create_dir_all(&dir)?;
            }
        }

        // 打开文件
        let file = OpenOptions::new()
            // 文件不存在则创建
            .create(true)
            .read(true)
            .write(true)
            .open(&file_path)?;
        //获取文件描述
        // let file_desc = file.as_raw_fd();
        // 加独占锁，排他锁 保证同时只有一个服务使用这个文件
        file.try_lock_exclusive()?;
        Ok(Self { file_path, file })
    }

    fn build_keydir(&mut self) -> LegendDBResult<KeyDir> {
        // 创建一个空的keydir
        let mut keydir = KeyDir::new();
        let mut reader = BufReader::new(&self.file);
        // 获取文件长度
        let file_len = self.file.metadata()?.len();
        let mut offset = 0;
        loop {
            // 先度前面8个字节，前面8个字节固定，包含key和value key的值，进而读到key和value
            if offset >= file_len {
                break;
            }
            let (key, value_size) = Self::read_value(&mut reader, offset)?;
            let key_size = key.len() as u32;
            // 删除的流程
            if value_size == -1 {
                keydir.remove(&key);
                offset += LOG_HEADER_SIZE as u64 + key_size as u64;
            } else {
                // value的长度是offset 加固定的8个字节，再加key的长度
                keydir.insert(key, (offset + LOG_HEADER_SIZE as u64 + key_size as u64, value_size as u32));
                offset += LOG_HEADER_SIZE as u64 + key_size as u64 + value_size as u64;
            }
        }
        Ok(keydir)
    }

    // +-------------+-------------+----------------+----------------+
    // | key len(4)    val len(4)     key(varint)       val(varint)  |
    // +-------------+-------------+----------------+----------------+
    fn write_entry(&mut self, key: &Vec<u8>, value: Option<&Vec<u8>>) -> LegendDBResult<(u64, u32)> {
        // 首先将文件的偏移移动到文件末尾
        let offset = self.file.seek(std::io::SeekFrom::End(0))?;
        let key_size = key.len() as u32;
        // map_or 函数，如果value为Some，则返回value.len()，否则返回0
        let value_size = value.map_or(0, |v| v.len() as u32);
        let entry_size = key_size + value_size + LOG_HEADER_SIZE;
        // 创建一个缓冲区，用于写入日志
        let mut writer = BufWriter::with_capacity(entry_size as usize, &mut self.file);
        // 写入key size
        writer.write_all(&key_size.to_be_bytes())?;
        // 写入value size
        writer.write_all(&value.map_or(-1, |v| v.len() as i32).to_be_bytes())?;
        // 写入key
        writer.write_all(&key)?;
        // 写入value
        if let Some(value) = value {
            writer.write_all(value)?;
        }
        // 刷新缓冲区，将数据写入文件
        writer.flush()?;
        Ok((offset, entry_size))
    }

    fn read_entry(&mut self, offset: u64, size: u32) -> LegendDBResult<Vec<u8>> {
        self.file.seek(SeekFrom::Start(offset))?;
        // read_exact 读取指定数量的字节，如果读取失败，则返回错误
        let mut buf = vec![0; size as usize];
        self.file.read_exact(&mut buf)?;
        Ok(buf)
    }

    fn read_value(buffer_reader: &mut BufReader<&File>, offset: u64) -> LegendDBResult<(Vec<u8>, i32)> {
        buffer_reader.seek(SeekFrom::Start(offset))?;
        let mut key_len = [0; 4];
        // 读取key size
        buffer_reader.read_exact(&mut key_len)?;
        let key_size = u32::from_be_bytes(key_len);

        // 读value size
        buffer_reader.read_exact(&mut key_len)?;
        // value size可能是复数
        let value_size = i32::from_be_bytes(key_len);
        // 读取key
        let mut key = vec![0; key_size as usize];
        buffer_reader.read_exact(&mut key)?;
        Ok((key, value_size))
    }
}

#[cfg(test)]
mod test {
    use std::path::PathBuf;
    use crate::storage::disk::DiskEngine;
    use crate::storage::engine::Engine;
    use crate::custom_error::LegendDBResult;

    #[test]
    fn test_disk_engine_compact() -> LegendDBResult<()> {
        let mut eng = DiskEngine::new(PathBuf::from("/tmp/sqldb/sqldb-log"))?;
        // 写一些数据
        eng.set(b"key1".to_vec(), b"value".to_vec())?;
        eng.set(b"key2".to_vec(), b"value".to_vec())?;
        eng.set(b"key3".to_vec(), b"value".to_vec())?;
        eng.delete(b"key1".to_vec())?;
        eng.delete(b"key2".to_vec())?;
        
        // 重写
        eng.set(b"aa".to_vec(), b"value1".to_vec())?;
        eng.set(b"aa".to_vec(), b"value2".to_vec())?;
        eng.set(b"aa".to_vec(), b"value3".to_vec())?;
        eng.set(b"bb".to_vec(), b"value4".to_vec())?;
        eng.set(b"bb".to_vec(), b"value5".to_vec())?;
        
        let iter = eng.scan(..);
        let v = iter.collect::<LegendDBResult<Vec<_>>>()?;
        assert_eq!(
            v,
            vec![
                (b"aa".to_vec(), b"value3".to_vec()),
                (b"bb".to_vec(), b"value5".to_vec()),
                (b"key3".to_vec(), b"value".to_vec()),
            ]
        );
        drop(eng);
        
        let mut eng2 = DiskEngine::new_compact(PathBuf::from("/tmp/sqldb/sqldb-log"))?;
        let iter2 = eng2.scan(..);
        let v2 = iter2.collect::<LegendDBResult<Vec<_>>>()?;
        assert_eq!(
            v2,
            vec![
                (b"aa".to_vec(), b"value3".to_vec()),
                (b"bb".to_vec(), b"value5".to_vec()),
                (b"key3".to_vec(), b"value".to_vec()),
            ]
        );
        drop(eng2);
        
        std::fs::remove_dir_all("/tmp/sqldb")?;

        Ok(())
    }
}
