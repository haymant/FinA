//! sonicetl core: a native whole-ETL engine (sonic-rs) exposed to Python via
//! PyO3, in the spirit of orjson — a thin Python interface over a fast Rust core.
//!
//! Public surface:
//!   * `run_etl(config_yaml, input, out_dir)` — run a whole ETL (parse +
//!     extract + Parquet write) streaming, per record, natively with sonic-rs.
//!   * `loads(data)` / `dumps(obj)` — orjson-style JSON codecs backed by
//!     sonic-rs.

mod columnar;
mod config;
mod lazy;
mod native;
mod plazy;
mod sonic;

// mimalloc returns freed pages to the OS aggressively, which keeps RES low when
// a workload churns many small allocations (e.g. sonic-rs bump arenas).
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use pyo3::exceptions::{PyRuntimeError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use pyo3::IntoPyObjectExt;

/// Run a whole ETL described by an official ETL YAML schema.
///
/// * `config_yaml` — ETL pipeline YAML (see README / schema docs).
/// * `input` — JSON document bytes (an array of records, or single object).
/// * `out_dir` — directory receiving one `<dataset>.parquet` per dataset.
///
/// Returns a dict: `{timing: [{step, ms}], datasets: {name: rows}, records, out_dir}`.
#[pyfunction]
#[pyo3(signature = (config_yaml, input, out_dir))]
fn run_etl(
    py: Python<'_>,
    config_yaml: &str,
    input: &[u8],
    out_dir: &str,
) -> PyResult<Py<PyDict>> {
    let cfg: config::Config = serde_yaml::from_str(config_yaml).map_err(|e| {
        PyValueError::new_err(format!("invalid ETL YAML config: {e}"))
    })?;
    if cfg.datasets.is_empty() {
        return Err(PyValueError::new_err("config declares no datasets"));
    }

    let res = plazy::run_lazy_stream(&cfg, input, out_dir, |slice| {
        let s = std::str::from_utf8(slice).map_err(|_| ())?;
        let v: sonic_rs::Value = sonic_rs::from_str(s).map_err(|_| ())?;
        Ok(sonic::Sonic(v))
    })
    .map_err(|e| PyRuntimeError::new_err(format!("ETL failed: {e}")))?;

    let timing = PyList::empty(py);
    for (name, ms) in &res.timing {
        let row = PyDict::new(py);
        row.set_item("step", name)?;
        row.set_item("ms", *ms)?;
        timing.append(row)?;
    }

    let datasets = PyDict::new(py);
    for (name, rows) in &res.datasets {
        datasets.set_item(name, *rows)?;
    }

    let records = res.datasets.first().map(|d| d.1).unwrap_or(0);
    let dict = PyDict::new(py);
    dict.set_item("timing", timing)?;
    dict.set_item("datasets", datasets)?;
    dict.set_item("records", records)?;
    dict.set_item("out_dir", out_dir)?;
    Ok(dict.unbind())
}

/// orjson-style `loads`: parse JSON (bytes) with sonic-rs into Python objects.
#[pyfunction]
#[pyo3(signature = (data))]
fn loads(py: Python<'_>, data: &[u8]) -> PyResult<PyObject> {
    let s = std::str::from_utf8(data)
        .map_err(|_| PyValueError::new_err("loads() argument must be valid UTF-8 JSON"))?;
    let v: sonic_rs::Value = sonic_rs::from_str(s)
        .map_err(|_| PyValueError::new_err("loads() failed to parse JSON"))?;
    Ok(sonic_to_py(py, &v)?)
}

/// orjson-style `dumps`: serialize a Python object to JSON bytes with sonic-rs.
///
/// Optional `option: int` is accepted for orjson compatibility; only `0` and
/// `OPT_UTC_Z`-style flags are tolerated (non-zero, non-default flags currently
/// raise for unsupported options).
#[pyfunction]
#[pyo3(signature = (obj, option=0))]
fn dumps(py: Python<'_>, obj: &Bound<'_, PyAny>, option: i32) -> PyResult<Vec<u8>> {
    if option != 0 {
        return Err(PyValueError::new_err(
            "dumps(): unsupported options; only default serialization is implemented",
        ));
    }
    let value = py_to_son(py, obj)?;
    sonic_rs::to_vec(&value)
        .map_err(|e| PyRuntimeError::new_err(format!("dumps() failed: {e}")))
}

fn sonic_to_py(py: Python<'_>, v: &sonic_rs::Value) -> PyResult<PyObject> {
    use sonic_rs::{JsonContainerTrait, JsonValueTrait};
    match v.get_type() {
        sonic_rs::JsonType::Null => Ok(py.None()),
        sonic_rs::JsonType::Boolean => v.as_bool().unwrap_or(false).into_py_any(py),
        sonic_rs::JsonType::Number => {
            if let Some(i) = v.as_i64() {
                i.into_py_any(py)
            } else if let Some(u) = v.as_u64() {
                u.into_py_any(py)
            } else {
                v.as_f64().unwrap_or(0.0).into_py_any(py)
            }
        }
        sonic_rs::JsonType::String => v.as_str().unwrap_or("").into_py_any(py),
        sonic_rs::JsonType::Object => {
            let dict = PyDict::new(py);
            if let Some(obj) = v.as_object() {
                for (k, val) in obj.iter() {
                    dict.set_item(k.to_string(), sonic_to_py(py, val)?)?;
                }
            }
            Ok(dict.into_any().unbind())
        }
        sonic_rs::JsonType::Array => {
            let list = PyList::empty(py);
            if let Some(arr) = v.as_array() {
                for val in arr.iter() {
                    list.append(sonic_to_py(py, val)?)?;
                }
            }
            Ok(list.into_any().unbind())
        }
    }
}

fn py_to_son(py: Python<'_>, obj: &Bound<'_, PyAny>) -> PyResult<sonic_rs::Value> {
    use sonic_rs::Value;
    if obj.is_none() {
        return Ok(Value::from(()));
    }
    if obj.is_instance_of::<pyo3::types::PyBool>() {
        return Ok(Value::from(obj.extract::<bool>()?));
    }
    if obj.is_instance_of::<pyo3::types::PyString>() {
        let s: String = obj.extract()?;
        return Ok(Value::from(s.as_str()));
    }
    if let Ok(d) = obj.downcast::<PyDict>() {
        let mut o = sonic_rs::Object::new();
        for (k, v) in d.iter() {
            let key: String = k.extract()?;
            o.insert(&key, py_to_son(py, &v)?);
        }
        return Ok(Value::from(o));
    }
    if let Ok(l) = obj.downcast::<pyo3::types::PyList>() {
        let mut a = sonic_rs::Array::new();
        for item in l.iter() {
            a.push(py_to_son(py, &item)?);
        }
        return Ok(Value::from(a));
    }
    if let Ok(i) = obj.extract::<i64>() {
        return Ok(Value::from(i));
    }
    if let Ok(f) = obj.extract::<f64>() {
        return Ok(sonic_rs::Value::new_f64(f).unwrap_or_else(sonic_rs::Value::new_null));
    }
    Err(PyTypeError::new_err(
        "dumps() Unsupported type. Try encoding with .default()",
    ))
}

#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add_function(wrap_pyfunction!(run_etl, m)?)?;
    m.add_function(wrap_pyfunction!(loads, m)?)?;
    m.add_function(wrap_pyfunction!(dumps, m)?)?;
    Ok(())
}
