use std::collections::btree_map::Range;
use std::collections::BTreeMap;
use std::ops::{RangeBounds};
use crate::sql::storage::engine::{Engine, EngineIterator};
use crate::utils::custom_error::LegendDBResult;

//内存存储引擎定义
#[derive(Debug)]
pub struct MemoryEngine {
    data: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl MemoryEngine {
    pub fn new() -> Self {
        MemoryEngine {
            data: BTreeMap::new(),
        }
    }
}

impl Engine for MemoryEngine {
    type EngineIterator<'a> = MemoryEngineIterator<'a>;

    fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> LegendDBResult<()> {
        self.data.insert(key, value);
        Ok(())
    }

    fn get(&mut self, key: Vec<u8>) -> LegendDBResult<Option<Vec<u8>>> {
        Ok(self.data.get(&key).cloned())
    }

    fn delete(&mut self, key: Vec<u8>) -> LegendDBResult<()> {
        self.data.remove(&key);
        Ok(())
    }

    // <'_> 是Rust中用于简化生命周期标注的语法，表示让编译器自动推断生命周期，避免显式命名的繁琐
    fn scan(&self, range: impl RangeBounds<Vec<u8>>) -> Self::EngineIterator<'_> {
        MemoryEngineIterator {
            inner: self.data.range(range),
        }
    }
}

//内存存储引擎迭代器
pub struct MemoryEngineIterator<'a> {
    inner: Range<'a,  Vec<u8>, Vec<u8>>,
}


impl<'a> Iterator for MemoryEngineIterator<'a> {
    type Item = LegendDBResult<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        // self.inner.next().map(|(k, v)| Ok((k.clone(), v.clone())))
        // self.inner.next().map(|item| Self::map(&item))
        self.inner.next().map(Self::map)
    }
}

impl<'a> DoubleEndedIterator for MemoryEngineIterator<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner.next_back().map( Self::map)
    }

}

impl<'a> MemoryEngineIterator<'a> {
    fn map(item: (&Vec<u8>, &Vec<u8>)) -> <Self as Iterator>::Item {
        let (k, v) = item;
        Ok((k.clone(), v.clone()))
    }
}


impl<'a> EngineIterator for MemoryEngineIterator<'a> {}