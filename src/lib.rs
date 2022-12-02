#[macro_use]
extern crate log;

#[cfg(test)]
mod test;

pub mod db;
pub mod executor;
pub mod parser;

pub use self::db::{Database, Error};
