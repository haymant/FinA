//! FinA core: a native whole-ETL engine (sonic-rs) exposed to Python via
//! PyO3, in the spirit of orjson — a thin Python interface over a fast Rust core.
//!
//! Public surface:
//!   * `run_etl(config_yaml, input, out_dir)` — run a whole ETL (parse +
//!     extract + Parquet write) streaming, per record, natively with sonic-rs.
//!   * `loads(data)` / `dumps(obj)` — orjson-style JSON codecs backed by
//!     sonic-rs.

mod columnar;
mod config;
mod etl_sched;
mod lazy;
mod native;
mod plazy;
mod scheduler;
mod sonic;
mod store;

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use pyo3::exceptions::{PyRuntimeError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use pyo3::IntoPyObjectExt;

/// Run a full set of ETL pipelines described by the official ETL YAML schema
/// (see docs/schema.md). The root key is `pipelines: [ ... ]`; the shared
/// in-memory store is scoped to a single call so tables produced by an earlier
/// pipeline are visible to later ones (joins / cross-pipeline references).
///
/// * `config_yaml` — pipelines YAML (a YAML string).
///
/// Returns a dict: `{timing: [{step, ms}], datasets: {name: rows}}`.
#[pyfunction]
#[pyo3(signature = (config_yaml))]
fn run_pipelines(py: Python<'_>, config_yaml: &str) -> PyResult<Py<PyDict>> {
    let cfg: config::PipelinesConfig = serde_yaml::from_str(config_yaml).map_err(|e| {
        PyValueError::new_err(format!("invalid ETL YAML config: {e}"))
    })?;
    if cfg.pipelines.is_empty() {
        return Err(PyValueError::new_err("config declares no pipelines"));
    }
    for p in &cfg.pipelines {
        if p.datasets.is_empty() {
            return Err(PyValueError::new_err(
                "each pipeline must declare at least one dataset",
            ));
        }
    }

    let res = plazy::run_pipelines(&cfg).map_err(|e| PyRuntimeError::new_err(format!("ETL failed: {e}")))?;

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

    let dict = PyDict::new(py);
    dict.set_item("timing", timing)?;
    dict.set_item("datasets", datasets)?;
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

// ---------------------------------------------------------------------------
// Scheduler (docs/scheduler.md) — a generic durable OS-style task scheduler
// kernel exposed to Python. Handles are opaque integers into a global registry.
// ---------------------------------------------------------------------------

static REGISTRY: OnceLock<Mutex<HashMap<u64, Arc<scheduler::SchedulerRuntime>>>> = OnceLock::new();
static NEXT_HANDLE: AtomicU64 = AtomicU64::new(1);

fn registry() -> &'static Mutex<HashMap<u64, Arc<scheduler::SchedulerRuntime>>> {
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Create a scheduler and return an opaque handle. `workers` bounds how many
/// tasks run concurrently. `hook` (optional) is a Python `TaskHook` instance
/// whose lifecycle callbacks run on the task threads; when omitted the
/// built-in AutoFinishHook is used (tasks finish instantly — good for
/// exercising the state machine with no worker).
#[pyfunction]
#[pyo3(signature = (workers=1, hook=None))]
fn scheduler_new(py: Python<'_>, workers: usize, hook: Option<PyObject>) -> PyResult<u64> {
    let hook_arc: Arc<dyn scheduler::TaskHook> = match hook {
        Some(obj) => Arc::new(scheduler::PyHook::new(obj)),
        None => Arc::new(scheduler::AutoFinishHook),
    };
    let handle = NEXT_HANDLE.fetch_add(1, Ordering::SeqCst);
    let rt = py.allow_threads(|| scheduler::SchedulerRuntime::spawn(hook_arc, workers.max(1)));
    registry()
        .lock()
        .map_err(|_| PyRuntimeError::new_err("scheduler registry poisoned"))?
        .insert(handle, Arc::new(rt));
    Ok(handle)
}

/// Dispatch a JSON command to a scheduler (fire-and-forget). See
/// `SchedCmd` for the wire format:
/// `{"cmd":"start","id":..,"priority":..,"max_attempts":..,"info":..}` or
/// `{"cmd":"reschedule","id":..,"reason":"...","retry_delay_ms":..}` or
/// `{"cmd":"restore","tasks":[Task,...]}`.
#[pyfunction]
fn scheduler_cmd(py: Python<'_>, handle: u64, command_json: &str) -> PyResult<()> {
    let cmd: scheduler::SchedCmd = sonic_rs::from_str(command_json)
        .map_err(|e| PyValueError::new_err(format!("bad scheduler command JSON: {e}")))?;
    if std::env::var_os("FINA_TRACE").is_some() {
        eprintln!("[trace] scheduler_cmd enter");
    }
    let rt = {
        let guard = registry()
            .lock()
            .map_err(|_| PyRuntimeError::new_err("scheduler registry poisoned"))?;
        guard
            .get(&handle)
            .cloned()
            .ok_or_else(|| PyValueError::new_err(format!("no scheduler with handle {handle}")))?
    };
    py.allow_threads(move || rt.send(cmd));
    if std::env::var_os("FINA_TRACE").is_some() {
        eprintln!("[trace] scheduler_cmd sent");
    }
    Ok(())
}

/// Snapshot of every task as a JSON array (order: submission order).
#[pyfunction]
fn scheduler_query(py: Python<'_>, handle: u64) -> PyResult<String> {
    let rt = {
        let guard = registry()
            .lock()
            .map_err(|_| PyRuntimeError::new_err("scheduler registry poisoned"))?;
        guard
            .get(&handle)
            .cloned()
            .ok_or_else(|| PyValueError::new_err(format!("no scheduler with handle {handle}")))?
    };
    let tasks = py.allow_threads(|| rt.query());
    sonic_rs::to_string(&tasks)
        .map_err(|e| PyRuntimeError::new_err(format!("scheduler_query serialization failed: {e}")))
}

/// Cheap per-state counters: `{"pending":N,"running":N,"paused":N,
/// "finished":N,"killed":N}`. Scans the snapshot without materializing task
/// JSON, so the throughput benchmark can poll continuously at low cost.
#[pyfunction]
fn scheduler_count(py: Python<'_>, handle: u64) -> PyResult<String> {
    let rt = {
        let guard = registry()
            .lock()
            .map_err(|_| PyRuntimeError::new_err("scheduler registry poisoned"))?;
        guard
            .get(&handle)
            .cloned()
            .ok_or_else(|| PyValueError::new_err(format!("no scheduler with handle {handle}")))?
    };
    let c = py.allow_threads(|| rt.counts());
    let v = serde_json::json!({
        "pending": c[0],
        "running": c[1],
        "paused": c[2],
        "finished": c[3],
        "killed": c[4],
    });
    serde_json::to_string(&v)
        .map_err(|e| PyRuntimeError::new_err(format!("scheduler_count serialization failed: {e}")))
}

/// Stop and release a scheduler. Fire-and-forget: the scheduler's system
/// thread shuts itself down and is never joined (so in-flight Python hooks
/// can still release the GIL).
#[pyfunction]
fn scheduler_close(py: Python<'_>, handle: u64) -> PyResult<()> {
    let rt = {
        let mut guard = registry()
            .lock()
            .map_err(|_| PyRuntimeError::new_err("scheduler registry poisoned"))?;
        guard
            .remove(&handle)
            .ok_or_else(|| PyValueError::new_err(format!("no scheduler with handle {handle}")))?
    };
    py.allow_threads(move || {
        // The registry no longer holds this handle, so the Arc is unique.
        if let Ok(mut rt) = Arc::try_unwrap(rt) {
            rt.close();
        }
    });
    Ok(())
}

// ---------------------------------------------------------------------------
// ETL-to-scheduler expansion & one-shot run (docs/scheduler.md)
// ---------------------------------------------------------------------------

/// Expand an ETL config YAML into the canonical scheduler task plan as JSON:
/// `{"stages": [[{"id","priority","info"}, ...], ...]}`. Stages run serially;
/// tasks inside a stage run concurrently. Configs without `execution:` yield
/// one task per pipeline (serial, legacy behaviour).
#[pyfunction]
#[pyo3(signature = (config_yaml, retries=2))]
fn expand_etl_config(py: Python<'_>, config_yaml: &str, retries: usize) -> PyResult<String> {
    py.allow_threads(|| {
        let cfg: config::PipelinesConfig = serde_yaml::from_str(config_yaml)
            .map_err(|e| format!("invalid ETL YAML config: {e}"))?;
        if cfg.pipelines.is_empty() {
            return Err("config declares no pipelines".to_string());
        }
        let max_attempts = if retries == 0 { 0 } else { (retries + 1) as u32 };
        let plan = etl_sched::build_plan(&cfg, max_attempts)?;
        let mut stages_json: Vec<serde_json::Value> = Vec::new();
        for stage in &plan.stages {
            let mut arr: Vec<serde_json::Value> = Vec::new();
            for t in stage {
                arr.push(etl_sched::task_spec_to_json(t)?);
            }
            stages_json.push(serde_json::Value::Array(arr));
        }
        let out = serde_json::json!({"stages": stages_json});
        serde_json::to_string(&out).map_err(|e| format!("serialize plan: {e}"))
    })
    .map_err(|e: String| PyRuntimeError::new_err(format!("expand_etl_config failed: {e}")))
}

/// Run the whole ETL config through a scheduler in-process and return the
/// aggregated result as JSON. Blocks until every stage is done, polling the
/// snapshot every `poll_ms`. Soft failures (network / rate limit) retried up
/// to `retries` times (bounded by each task's max_attempts); the first hard
/// failure kills the run.
#[pyfunction]
#[pyo3(signature = (config_yaml, workers=1, retries=2, poll_ms=100))]
fn etl_scheduler_run(
    py: Python<'_>,
    config_yaml: &str,
    workers: usize,
    retries: usize,
    poll_ms: u64,
) -> PyResult<String> {
    let res = py.allow_threads(|| {
        etl_sched::run_scheduled(config_yaml, workers, retries, poll_ms)
            .and_then(|v| serde_json::to_string(&v).map_err(|e| format!("serialize result: {e}")))
    });
    match res {
        Ok(json) => Ok(json),
        Err(e) => Err(PyRuntimeError::new_err(format!("scheduled ETL failed: {e}"))),
    }
}

pub(crate) fn sonic_to_py(py: Python<'_>, v: &sonic_rs::Value) -> PyResult<PyObject> {
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
    m.add_function(wrap_pyfunction!(run_pipelines, m)?)?;
    m.add_function(wrap_pyfunction!(loads, m)?)?;
    m.add_function(wrap_pyfunction!(dumps, m)?)?;
    m.add_function(wrap_pyfunction!(scheduler_new, m)?)?;
    m.add_function(wrap_pyfunction!(scheduler_cmd, m)?)?;
    m.add_function(wrap_pyfunction!(scheduler_query, m)?)?;
    m.add_function(wrap_pyfunction!(scheduler_count, m)?)?;
    m.add_function(wrap_pyfunction!(scheduler_close, m)?)?;
    m.add_function(wrap_pyfunction!(expand_etl_config, m)?)?;
    m.add_function(wrap_pyfunction!(etl_scheduler_run, m)?)?;
    Ok(())
}
