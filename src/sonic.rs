use crate::native::{NKind, NValue};
use sonic_rs::{JsonContainerTrait, JsonType, JsonValueTrait, Value as SonicValue};

/// sonic-rs owned `Value` wrapped for the generic engine. `repr(transparent)`
/// guarantees `&Sonic` and `&SonicValue` share layout, so child references
/// returned from the backend can be reinterpreted safely.
#[repr(transparent)]
pub struct Sonic(pub SonicValue);

impl NValue for Sonic {
    fn kind(&self) -> NKind {
        match self.0.get_type() {
            JsonType::Null => NKind::Null,
            JsonType::Boolean => NKind::Bool,
            JsonType::Number => {
                if self.0.as_i64().is_some() {
                    NKind::Int
                } else if self.0.as_u64().is_some() {
                    NKind::UInt
                } else {
                    NKind::Float
                }
            }
            JsonType::String => NKind::Str,
            JsonType::Object => NKind::Object,
            JsonType::Array => NKind::Array,
        }
    }

    fn as_bool(&self) -> Option<bool> {
        self.0.as_bool()
    }
    fn as_i64(&self) -> Option<i64> {
        self.0.as_i64()
    }
    fn as_u64(&self) -> Option<u64> {
        self.0.as_u64()
    }
    fn as_f64(&self) -> Option<f64> {
        self.0.as_f64()
    }
    fn as_str(&self) -> Option<&str> {
        self.0.as_str()
    }

    fn get(&self, key: &str) -> Option<&Self> {
        if !self.0.is_object() {
            return None;
        }
        let child = self.0.get(key)?;
        // SAFETY: Sonic is repr(transparent) over SonicValue, so this is a
        // layout-identical reborrow.
        Some(unsafe { &*(child as *const SonicValue as *const Sonic) })
    }
    fn get_idx(&self, i: usize) -> Option<&Self> {
        if !self.0.is_array() {
            return None;
        }
        let child = self.0.as_array()?.get(i)?;
        Some(unsafe { &*(child as *const SonicValue as *const Sonic) })
    }
    fn array_len(&self) -> Option<usize> {
        if self.0.is_array() {
            Some(self.0.as_array()?.len())
        } else {
            None
        }
    }

    fn raw_text(&self) -> Option<String> {
        sonic_rs::to_string(&self.0).ok()
    }
}
