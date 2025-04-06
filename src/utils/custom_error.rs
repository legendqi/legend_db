use std::array::TryFromSliceError;
use std::fmt::{Display};
use std::io::Error;
use std::num::{ParseFloatError, ParseIntError};
use std::string::FromUtf8Error;
use std::sync::{Arc, PoisonError};
use bincode::error::DecodeError;
use bincode::error::EncodeError;
//自定义错误类型
pub type LegendDBResult<T> = Result<T, LegendDBError>;

#[derive(Debug, Clone, thiserror::Error)]
pub enum LegendDBError {
    #[error("parse int error: {0}")]
    ParseIntError(#[from] ParseIntError),
    #[error("parse float error: {0}")]
    ParseFloatError(#[from] ParseFloatError),
    #[error("from utf8 error: {0}")]
    FromUtf8Error(#[from] FromUtf8Error),
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
    TableNotFound(String),
    #[error("decode error: {0}")]
    DecodeError(String),
    #[error("encode error: {0}")]
    EncodeError(String),
    #[error("write mvcc conflict")]
    WriteMvccConflict,
    #[error("serializer error: {0}")]
    SerializerError(String),
    #[error("deserializer error: {0}")]
    DeserializerError(String),
}

impl From<TryFromSliceError> for LegendDBError {
    fn from(value: TryFromSliceError) -> Self {
        LegendDBError::TryFromSliceError(value.to_string())
    }
}

impl serde::ser::Error for LegendDBError {
    fn custom<T>(msg: T) -> Self
    where
        T: Display
    {
        LegendDBError::SerializerError(msg.to_string())
    }
}

impl serde::de::Error for LegendDBError {
    fn custom<T>(msg: T) -> Self
    where
        T: Display
    {
        LegendDBError::DeserializerError(msg.to_string())
    }
}

impl From<DecodeError> for LegendDBError {
    fn from(value: DecodeError) -> Self {
        LegendDBError::DecodeError(value.to_string())
    }
}

impl From<EncodeError> for LegendDBError {
    fn from(value: EncodeError) -> Self {
        LegendDBError::EncodeError(value.to_string())
    }
}

impl From<Error> for LegendDBError {
    fn from(value: Error) -> Self {
        LegendDBError::Error(Arc::new(value))
    }
}

impl<E> From<PoisonError<E>> for LegendDBError {
    fn from(value: PoisonError<E>) -> Self {
        LegendDBError::Internal(value.to_string())
    }
}