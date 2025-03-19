mod engine;
mod memory;

use crate::utils::custom_error::LegendDBResult;

#[derive(Debug, Clone)]
pub struct Mvcc{}

impl Mvcc {
    pub fn new() -> Self {
        Mvcc{}
    }

    pub fn begin(&self) -> LegendDBResult<MvccTraction> {
        Ok(MvccTraction::new())
    }
}

#[derive(Debug, Clone)]
pub struct MvccTraction{}

impl MvccTraction {
    pub fn new() -> Self {
        MvccTraction{}
    }
}