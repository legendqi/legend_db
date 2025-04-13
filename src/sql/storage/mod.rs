pub mod engine;
pub mod memory;
pub mod mvcc;

#[allow(unused)]
pub mod disk;
mod b_plus_tree;
#[allow(unused)]
pub mod keycode;

use crate::utils::custom_error::LegendDBResult;

#[allow(unused)]
#[derive(Debug, Clone)]
pub struct Mvcc{}

#[allow(unused)]
impl Mvcc {
    pub fn new() -> Self {
        Mvcc{}
    }

    pub fn begin(&self) -> LegendDBResult<MvccTraction> {
        Ok(MvccTraction::new())
    }
}

#[allow(unused)]
#[derive(Debug, Clone)]
pub struct MvccTraction{}


#[allow(unused)]
impl MvccTraction {
    pub fn new() -> Self {
        MvccTraction{}
    }
}