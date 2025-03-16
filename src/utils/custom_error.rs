use std::io::Error;
use std::num::{ParseFloatError, ParseIntError};

//自定义错误类型
pub type LegendDBResult<T> = Result<T, LegendDBError>;

#[derive(Debug, thiserror::Error)]
pub enum LegendDBError {
    #[error("parse int error: {0}")]
    ParseIntError(#[from] ParseIntError),
    #[error("parse float error: {0}")]
    ParseFloatError(#[from] ParseFloatError),
    #[error("internal error: {0}")]
    Error(#[from] Error),
    #[error("error kind: {0}")]
    ErrorKind(String),
    #[error("try from slice error : {0}")]
    TryFromSliceError(String),
    #[error("parse error: {0}")]
    Parser(String),
    #[error("not supported")]
    NotSupported
}