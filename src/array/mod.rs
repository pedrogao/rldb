use std::convert::TryFrom;

use crate::types::{DataType, DataTypeKind, DataValue};

mod data_chunk;
mod iter;
mod primitive_array;
mod utf8_array;

pub use self::data_chunk::*;
pub use self::iter::ArrayIter;
pub use self::primitive_array::*;
pub use self::utf8_array::*;

pub trait ArrayBuilder: Send + Sync + 'static {
    type Array: Array<Builder = Self>;

    fn with_capacity(capacity: usize) -> Self;

    fn push(&mut self, value: Option<&<Self::Array as Array>::Item>);

    fn append(&mut self, other: &Self::Array);

    fn finish(self) -> Self::Array;
}

pub trait Array: Sized + Send + Sync + 'static {
    type Builder: ArrayBuilder<Array = Self>;

    type Item: ToOwned + ?Sized;

    fn get(&self, idx: usize) -> Option<&Self::Item>;

    fn len(&self) -> usize;

    fn iter(&self) -> ArrayIter<'_, Self> {
        ArrayIter::new(self)
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

pub type BoolArray = PrimitiveArray<bool>;
pub type I32Array = PrimitiveArray<i32>;
pub type F64Array = PrimitiveArray<f64>;

#[derive(Clone, PartialEq)]
pub enum ArrayImpl {
    Bool(BoolArray),
    Int32(I32Array),
    Float64(F64Array),
    Utf8(Utf8Array),
}

pub type BoolArrayBuilder = PrimitiveArrayBuilder<bool>;
pub type I32ArrayBuilder = PrimitiveArrayBuilder<i32>;
pub type F64ArrayBuilder = PrimitiveArrayBuilder<f64>;

pub enum ArrayBuilderImpl {
    Bool(BoolArrayBuilder),
    Int32(I32ArrayBuilder),
    Float64(F64ArrayBuilder),
    Utf8(Utf8ArrayBuilder),
}

#[derive(Debug, Clone)]
pub struct TypeMismatch;

macro_rules! impl_into {
    ($x:ty, $y:ident) => {
        impl From<$x> for ArrayImpl {
            fn from(array: $x) -> Self {
                Self::$y(array)
            }
        }

        impl TryFrom<ArrayImpl> for $x {
            type Error = TypeMismatch;

            fn try_from(array: ArrayImpl) -> Result<Self, Self::Error> {
                match array {
                    ArrayImpl::$y(array) => Ok(array),
                    _ => Err(TypeMismatch),
                }
            }
        }

        impl<'a> TryFrom<&'a ArrayImpl> for &'a $x {
            type Error = TypeMismatch;

            fn try_from(array: &'a ArrayImpl) -> Result<Self, Self::Error> {
                match array {
                    ArrayImpl::$y(array) => Ok(array),
                    _ => Err(TypeMismatch),
                }
            }
        }
    };
}

impl_into! { PrimitiveArray<bool>, Bool }
impl_into! { PrimitiveArray<i32>, Int32 }
impl_into! { PrimitiveArray<f64>, Float64 }
impl_into! { Utf8Array, Utf8 }

impl ArrayBuilderImpl {
    pub fn with_capacity(capacity: usize, ty: &DataType) -> Self {
        match ty.kind() {
            DataTypeKind::Boolean => Self::Bool(BoolArrayBuilder::with_capacity(capacity)),
            DataTypeKind::Int(_) => Self::Int32(I32ArrayBuilder::with_capacity(capacity)),
            DataTypeKind::Float(_) | DataTypeKind::Double => {
                Self::Float64(F64ArrayBuilder::with_capacity(capacity))
            }
            DataTypeKind::Char(_) | DataTypeKind::Varchar(_) | DataTypeKind::String => {
                Self::Utf8(Utf8ArrayBuilder::with_capacity(capacity))
            }
            _ => panic!("unsupported data type"),
        }
    }

    pub fn from_type_of_array(array: &ArrayImpl) -> Self {
        match array {
            ArrayImpl::Bool(_) => Self::Bool(BoolArrayBuilder::with_capacity(0)),
            ArrayImpl::Int32(_) => Self::Int32(I32ArrayBuilder::with_capacity(0)),
            ArrayImpl::Float64(_) => Self::Float64(F64ArrayBuilder::with_capacity(0)),
            ArrayImpl::Utf8(_) => Self::Utf8(Utf8ArrayBuilder::with_capacity(0)),
        }
    }

    pub fn push(&mut self, v: &DataValue) {
        match (self, v) {
            (Self::Bool(a), DataValue::Bool(v)) => a.push(Some(v)),
            (Self::Int32(a), DataValue::Int32(v)) => a.push(Some(v)),
            (Self::Float64(a), DataValue::Float64(v)) => a.push(Some(v)),
            (Self::Utf8(a), DataValue::String(v)) => a.push(Some(v)),
            (Self::Bool(a), DataValue::Null) => a.push(None),
            (Self::Int32(a), DataValue::Null) => a.push(None),
            (Self::Float64(a), DataValue::Null) => a.push(None),
            (Self::Utf8(a), DataValue::Null) => a.push(None),
            _ => panic!("failed to push value: type mismatch"),
        }
    }

    pub fn append(&mut self, array_impl: &ArrayImpl) {
        match (self, array_impl) {
            (Self::Bool(builder), ArrayImpl::Bool(arr)) => builder.append(arr),
            (Self::Int32(builder), ArrayImpl::Int32(arr)) => builder.append(arr),
            (Self::Float64(builder), ArrayImpl::Float64(arr)) => builder.append(arr),
            (Self::Utf8(builder), ArrayImpl::Utf8(arr)) => builder.append(arr),
            _ => panic!("failed to push value: type mismatch"),
        }
    }

    pub fn finish(self) -> ArrayImpl {
        match self {
            Self::Bool(a) => ArrayImpl::Bool(a.finish()),
            Self::Int32(a) => ArrayImpl::Int32(a.finish()),
            Self::Float64(a) => ArrayImpl::Float64(a.finish()),
            Self::Utf8(a) => ArrayImpl::Utf8(a.finish()),
        }
    }
}

impl ArrayImpl {
    pub fn get(&self, idx: usize) -> DataValue {
        match self {
            Self::Bool(a) => match a.get(idx) {
                Some(val) => DataValue::Bool(*val),
                None => DataValue::Null,
            },
            Self::Int32(a) => match a.get(idx) {
                Some(val) => DataValue::Int32(*val),
                None => DataValue::Null,
            },
            Self::Float64(a) => match a.get(idx) {
                Some(val) => DataValue::Float64(*val),
                None => DataValue::Null,
            },
            Self::Utf8(a) => match a.get(idx) {
                Some(val) => DataValue::String(val.to_string()),
                None => DataValue::Null,
            },
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Self::Bool(a) => a.len(),
            Self::Int32(a) => a.len(),
            Self::Float64(a) => a.len(),
            Self::Utf8(a) => a.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
