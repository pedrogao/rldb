pub use sqlparser::ast::DataType as DataTypeKind;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DataType {
    kind: DataTypeKind,
    nullable: bool,
}

impl DataType {
    pub const fn new(kind: DataTypeKind, nullable: bool) -> Self {
        DataType { kind, nullable }
    }

    pub fn is_nullable(&self) -> bool {
        self.nullable
    }

    pub fn kind(&self) -> DataTypeKind {
        self.kind.clone()
    }
}

pub trait DataTypeExt {
    fn nullable(self) -> DataType;

    fn not_null(self) -> DataType;
}

impl DataTypeExt for DataTypeKind {
    fn nullable(self) -> DataType {
        DataType::new(self, true)
    }

    fn not_null(self) -> DataType {
        DataType::new(self, false)
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub enum DataValue {
    // NOTE: Null comes first.
    // => NULL is less than any non-NULL values
    Null,
    Bool(bool),
    Int32(i32),
    Float64(f64),
    String(String),
}

impl ToString for DataValue {
    fn to_string(&self) -> String {
        match self {
            Self::Null => String::from("NULL"),
            Self::Bool(v) => v.to_string(),
            Self::Int32(v) => v.to_string(),
            Self::Float64(v) => v.to_string(),
            Self::String(v) => v.to_string(),
        }
    }
}

impl DataValue {
    pub fn datatype(&self) -> Option<DataType> {
        match self {
            Self::Bool(_) => Some(DataTypeKind::Boolean.not_null()),
            Self::Int32(_) => Some(DataTypeKind::Int(None).not_null()),
            Self::Float64(_) => Some(DataTypeKind::Double.not_null()),
            Self::String(_) => Some(DataTypeKind::Varchar(None).not_null()),
            Self::Null => None,
        }
    }
}
