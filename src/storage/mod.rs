use std::sync::Arc;

mod column;
mod disk;
mod memory;
mod rowset;

pub use self::column::*;
pub use self::disk::*;
pub use self::memory::*;
pub use self::rowset::*;

#[derive(thiserror::Error, Debug)]
#[error("{0:?}")]
pub struct StorageError(#[from] anyhow::Error);

pub type StorageResult<T> = std::result::Result<T, StorageError>;

#[cfg(memory)]
pub type StorageRef = Arc<InMemoryStorage>;

pub type StorageRef = Arc<DiskStorage>;

pub fn err(error: impl Into<anyhow::Error>) -> StorageError {
    StorageError(error.into())
}
