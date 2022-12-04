#[macro_use]
extern crate log;

#[cfg(test)]
mod test;

pub mod array;
pub mod binder;
pub mod catalog;
pub mod db;
pub mod executor;
pub mod parser;
pub mod storage;
pub mod types;

pub use self::db::{Database, Error};
