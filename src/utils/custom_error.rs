use std::io::Error;
use std::num::{ParseFloatError, ParseIntError};
use std::sync::{Arc, PoisonError};
use rkyv::rancor::Error as RancorError;

//自定义错误类型
pub type LegendDBResult<T> = Result<T, LegendDBError>;

#[derive(Debug, thiserror::Error)]
pub enum LegendDBError {
    #[error("parse int error: {0}")]
    ParseIntError(#[from] ParseIntError),
    #[error("parse float error: {0}")]
    ParseFloatError(#[from] ParseFloatError),
    #[error("internal error: {0}")]
    Error(#[from] Arc<Error>),
    #[error("error kind: {0}")]
    ErrorKind(String),
    #[error("try from slice error : {0}")]
    TryFromSliceError(String),
    #[error("parse error: {0}")]
    Parser(String),
    #[error("not supported")]
    NotSupported,
    #[error("internal error {0}")]
    Internal(String),
    #[error("table exists: {0}")]
    TableExist(String),
    #[error("table not exists: {0}")]
    RkyvError(#[from] RancorError)
}

impl<E> From<PoisonError<E>> for LegendDBError {
    fn from(value: PoisonError<E>) -> Self {
        LegendDBError::Internal(value.to_string())
    }
}