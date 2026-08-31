//! Backend-agnostic access to a parsed JSON value.
//!
//! `NValue` abstracts the native `Value` type of each JSON backend so the whole
//! ETL (per-record parse -> expression evaluation -> columnar output) can run
//! against that backend's own representation without ever materializing a
//! `serde_json::Value` DOM. Child nodes are returned as `&Self` borrowed from the
//! parsed record, matching how each backend holds its tree in one arena.

use std::fmt;

/// Coarse JSON kind, sufficient for the expression/column engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NKind {
    Null,
    Bool,
    Int,
    UInt,
    Float,
    Str,
    Array,
    Object,
}

impl fmt::Display for NKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            NKind::Null => "null",
            NKind::Bool => "boolean",
            NKind::Int => "integer",
            NKind::UInt => "integer",
            NKind::Float => "float",
            NKind::Str => "string",
            NKind::Array => "array",
            NKind::Object => "object",
        };
        write!(f, "{s}")
    }
}

/// Abstract access to a native JSON value.
pub trait NValue {
    /// Coarse kind of this value.
    fn kind(&self) -> NKind;

    fn is_null(&self) -> bool {
        self.kind() == NKind::Null
    }
    fn is_array(&self) -> bool {
        self.kind() == NKind::Array
    }
    fn is_object(&self) -> bool {
        self.kind() == NKind::Object
    }

    fn as_bool(&self) -> Option<bool>;
    fn as_i64(&self) -> Option<i64>;
    fn as_u64(&self) -> Option<u64>;
    fn as_f64(&self) -> Option<f64>;
    /// String value (must be a string node, returns `None` otherwise).
    fn as_str(&self) -> Option<&str>;

    /// Object field lookup (`None` if not an object or key missing).
    fn get(&self, key: &str) -> Option<&Self>;
    /// Array element (`None` if not an array or index out of range).
    fn get_idx(&self, i: usize) -> Option<&Self>;
    /// Number of elements for arrays (else `None`).
    fn array_len(&self) -> Option<usize>;

    /// Raw JSON text of a sub-container for `to_json_string(...)`. Backends may
    /// serialize (allocating) since this is only invoked on requested sub-nodes.
    fn raw_text(&self) -> Option<String>;
}
