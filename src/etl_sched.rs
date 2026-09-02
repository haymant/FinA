//! ETL integration for the scheduler (docs/scheduler.md).
//!
//! Two layers:
//!
//! * [`build_plan`] expands an ETL config YAML into scheduler `TaskSpec`s.
//!   The ETL config groups pipelines into *execution stages* (run serially)
//!   of *groups* (run concurrently); a group node is either a pipeline name or
//!   a `{pipeline, partition: "partition(source.field, N)"}` expression that
//!   splits the source's universe evenly into N partitions, each of which
//!   becomes its own task whose dataset filters read `$task.ctx.units`.
//!   Configs without `execution:` keep the legacy serial one-pipeline-at-a-time
//!   behaviour (treated as one task per pipeline, single worker).
//!
//! * [`run_scheduled`] is the convenience driver: boots a scheduler with the
//!   [`EtlHook`] worker, submits stages in order, waits for each stage to be
//!   terminal before starting the next, retries retryable failures up to the
//!   configured cap, fails on the first `KILLED` task, and aggregates the
//!   results.

use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use sonic_rs::{JsonContainerTrait, JsonValueTrait};

use crate::config::{GroupNode, PipelinesConfig, Source};
use crate::scheduler::{
    spawn_thread, JsonValue, SchedCmd, SchedulerRuntime, Task, TaskHook, TaskNotifier,
    TaskState,
};
use crate::store::{SharedDb, SharedStore};

/// A canonical scheduler task description produced by config expansion.
#[derive(Debug, Clone)]
pub struct TaskSpec {
    pub id: String,
    pub priority: u32,
    pub info: JsonValue,
}

/// Stage-ordered task plan: `stages[i]` runs to completion before
/// `stages[i+1]` starts; tasks inside a stage run concurrently.
#[derive(Debug, Clone)]
pub struct EtlPlan {
    pub stages: Vec<Vec<TaskSpec>>,
}

/// The wire payload stored in `Task.info` for every ETL task:
/// `{"kind":"etl","job":{"pipeline_yaml":...,"max_attempts":...},"ctx":{...}}`.
/// `ctx` is `$task.ctx` inside field/filter expressions.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(dead_code)]
struct EtlJob {
    pipeline_yaml: String,
    max_attempts: u32,
}

const KIND_ETL: &str = "etl";

// ---------------------------------------------------------------------------
// config expansion
// ---------------------------------------------------------------------------

/// Expand an ETL config into stage-ordered tasks. `max_attempts` is baked into
/// each task's `job` so restores keep the same retry budget.
pub fn build_plan(cfg: &PipelinesConfig, max_attempts: u32) -> Result<EtlPlan, String> {
    let legacy = cfg.execution.is_empty();
    let stages: Vec<Vec<GroupNode>> = if legacy {
        cfg.pipelines
            .iter()
            .map(|p| {
                vec![GroupNode::Name(
                    p.name.clone().unwrap_or_else(|| "unnamed".into()),
                )]
            })
            .collect()
    } else {
        cfg.execution.iter().map(|s| s.group.clone()).collect()
    };

    let mut plan: Vec<Vec<TaskSpec>> = Vec::with_capacity(stages.len());
    for (si, stage) in stages.iter().enumerate() {
        let mut db = SharedDb::new()?;
        let mut tasks: Vec<TaskSpec> = Vec::new();
        for node in stage {
            match node {
                GroupNode::Name(name) => {
                    tasks.push(single_task(cfg, name, 0, max_attempts, si)?);
                }
                GroupNode::Node {
                    pipeline,
                    partition,
                    priority,
                } => match partition {
                    Some(expr) => {
                        let (src_name, field, workers) = parse_partition(expr)?;
                        let pipe = find_pipeline(cfg, pipeline)?;
                        let src = pipe
                            .sources
                            .iter()
                            .find(|s| s.name == src_name)
                            .ok_or_else(|| {
                                format!(
                                    "partition source '{src_name}' not found in pipeline '{pipeline}'"
                                )
                            })?;
                        let universe = universe_values(&mut db, src, field.as_deref())?;
                        let chunks = chunk_evenly(&universe, workers);
                        for (i, chunk) in chunks.iter().enumerate() {
                            if chunk.is_empty() {
                                continue;
                            }
                            tasks.push(fanout_task(
                                cfg,
                                pipeline,
                                src,
                                field.as_deref(),
                                i,
                                chunks.len(),
                                chunk,
                                max_attempts,
                                si,
                            )?);
                        }
                    }
                    None => {
                        tasks.push(single_task(
                            cfg,
                            pipeline,
                            (*priority).unwrap_or(0),
                            max_attempts,
                            si,
                        )?);
                    }
                },
            }
        }
        plan.push(tasks);
    }
    Ok(EtlPlan { stages: plan })
}

fn find_pipeline<'a>(cfg: &'a PipelinesConfig, name: &str) -> Result<&'a crate::config::Pipeline, String> {
    cfg.pipelines
        .iter()
        .find(|p| p.name.as_deref().unwrap_or("unnamed") == name)
        .ok_or_else(|| format!("no pipeline named '{name}'"))
}

/// Serialize one pipeline (with the fanout filter injected) as its own task.
fn fanout_task(
    cfg: &PipelinesConfig,
    pipeline: &str,
    src: &Source,
    field: Option<&str>,
    index: usize,
    total: usize,
    chunk: &[String],
    max_attempts: u32,
    stage: usize,
) -> Result<TaskSpec, String> {
    let pipe = find_pipeline(cfg, pipeline)?;
    let mut p2 = pipe.clone();
    if let Some(f) = field {
        let filter = format!("$.{f} IN $task.ctx.units");
        for ds in p2.datasets.iter_mut() {
            if ds.filter.is_none() {
                ds.filter = Some(filter.clone());
            }
        }
    }
    let yaml = serde_yaml::to_string(&p2).map_err(|e| format!("serialize pipeline: {e}"))?;
    let ctx = serde_json::json!({
        "units": chunk,
        "partition": {"index": index, "of": total},
        "universe": {"source": src.name, "field": field},
    });
    let info = etl_info(&yaml, &ctx, max_attempts)?;
    Ok(TaskSpec {
        id: format!("{stage}-{pipeline}/x/{index}"),
        priority: 0,
        info,
    })
}

fn single_task(
    cfg: &PipelinesConfig,
    pipeline: &str,
    priority: u32,
    max_attempts: u32,
    stage: usize,
) -> Result<TaskSpec, String> {
    let pipe = find_pipeline(cfg, pipeline)?;
    let yaml = serde_yaml::to_string(pipe).map_err(|e| format!("serialize pipeline: {e}"))?;
    let info = etl_info(&yaml, &serde_json::Value::Null, max_attempts)?;
    Ok(TaskSpec {
        id: format!("{stage}-{pipeline}"),
        priority,
        info,
    })
}

fn etl_info(pipeline_yaml: &str, ctx: &serde_json::Value, max_attempts: u32) -> Result<JsonValue, String> {
    let job = serde_json::json!({
        "kind": KIND_ETL,
        "job": {"pipeline_yaml": pipeline_yaml, "max_attempts": max_attempts},
        "ctx": ctx,
    });
    json_to_sonic(&job)
}

/// `partition(src.field, N)` or `partition(src, N)`.
fn parse_partition(expr: &str) -> Result<(String, Option<String>, usize), String> {
    let e = expr.trim();
    let inner = e
        .strip_prefix("partition(")
        .and_then(|s| s.strip_suffix(')'))
        .ok_or_else(|| format!("invalid partition expression '{expr}' (expected partition(src.field, N))"))?;
    let mut parts = inner.splitn(3, ',');
    let a = parts.next().unwrap_or("").trim();
    let b = parts.next().map(|s| s.trim()).unwrap_or("");
    let workers: usize = b
        .parse()
        .map_err(|_| format!("partition worker count must be an integer in '{expr}'"))?;
    if workers == 0 {
        return Err(format!("partition worker count must be >= 1 in '{expr}'"));
    }
    match a.rsplit_once('.') {
        Some((src, field)) if !field.is_empty() => Ok((src.to_string(), Some(field.to_string()), workers)),
        _ => Ok((a.to_string(), None, workers)),
    }
}

/// Split a universe as evenly as possible into `k` partitions.
fn chunk_evenly(values: &[String], k: usize) -> Vec<Vec<String>> {
    let k = k.max(1);
    if values.is_empty() {
        return (0..k).map(|_| Vec::new()).collect();
    }
    let base = values.len() / k;
    let rem = values.len() % k;
    let mut out: Vec<Vec<String>> = Vec::with_capacity(k);
    let mut i = 0;
    for c in 0..k {
        let len = base + usize::from(c < rem);
        out.push(values[i..i + len].to_vec());
        i += len;
    }
    out
}

/// Read a source's JSON array and collect the `field` (or whole-element)
/// scalar values as the partition universe.
fn universe_values(db: &mut SharedDb, src: &Source, field: Option<&str>) -> Result<Vec<String>, String> {
    let bytes = crate::plazy::read_source(db, src)?;
    let v: JsonValue = sonic_rs::from_slice(&bytes)
        .map_err(|e| format!("partition universe must be valid JSON: {e}"))?;
    let arr = v
        .as_array()
        .ok_or_else(|| "partition universe source must be a JSON array".to_string())?;
    let mut out = Vec::new();
    for item in arr.iter() {
        let s = match field {
            Some(f) => item
                .get(f)
                .and_then(|x| x.as_str())
                .unwrap_or_default()
                .to_string(),
            None => String::from_utf8_lossy(&sonic_rs::to_vec(item).map_err(|e| format!("{e}"))?)
                .into_owned(),
        };
        if !s.is_empty() {
            out.push(s);
        }
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// wire helpers
// ---------------------------------------------------------------------------

#[allow(dead_code)]
pub(crate) fn sonic_to_json(v: &JsonValue) -> Result<serde_json::Value, String> {
    let text = sonic_rs::to_string(v).map_err(|e| format!("{e}"))?;
    serde_json::from_str(&text).map_err(|e| format!("{e}"))
}

#[allow(dead_code)]
pub(crate) fn json_to_sonic(v: &serde_json::Value) -> Result<JsonValue, String> {
    let text = serde_json::to_string(v).map_err(|e| format!("{e}"))?;
    sonic_rs::from_str(&text).map_err(|e| format!("{e}"))
}

/// Render a `TaskSpec` as JSON (used by `expand_etl_config`).
pub(crate) fn task_spec_to_json(t: &TaskSpec) -> Result<serde_json::Value, String> {
    Ok(serde_json::json!({
        "id": t.id,
        "priority": t.priority,
        "info": sonic_to_json(&t.info)?,
    }))
}

// ---------------------------------------------------------------------------
// ETL worker hook
// ---------------------------------------------------------------------------

/// The built-in ETL worker: runs `plazy::run_pipeline_task` on the task's own
/// thread with `Some(task.info)` exposed as `$task`, then classifies the
/// result — successful → `finish`, soft (network / rate-limit style) failure →
/// `reschedule` (the scheduler bounds attempts via `max_attempts`), anything
/// else → `kill`.
///
/// All tasks of one scheduled run share the [`SharedStore`], so `memory://`
/// tables written by an earlier stage or worker remain visible to later ones.
pub struct EtlHook {
    store: Arc<SharedStore>,
}

impl EtlHook {
    pub fn new(store: Arc<SharedStore>) -> Self {
        EtlHook { store }
    }
}

impl TaskHook for EtlHook {
    fn on_start(&self, task: &Task, notifier: Arc<dyn TaskNotifier>) {
        let id = task.id.clone();
        let store = Arc::clone(&self.store);
        // expose the scheduler task id to the worker so fan-out submissions can
        // target worker-unique outputs (`{task}` URI placeholder).
        let mut info = match sonic_to_json(&task.info) {
            Ok(j) => j,
            Err(e) => {
                notifier.kill(&id, Some(&e));
                return;
            }
        };
        info["id"] = serde_json::Value::String(id.clone());
        let Ok(info) = json_to_sonic(&info) else {
            notifier.kill(&id, Some("task info re-serialization failed"));
            return;
        };
        spawn_thread(format!("etl-{id}"), move || match run_job(&info, &store) {
            Ok(res) => notifier.finish(&id, Some(res), None),
            Err(e) => {
                if is_retryable(&e) {
                    notifier.reschedule(&id, &e, 0);
                } else {
                    notifier.kill(&id, Some(&e));
                }
            }
        });
    }
}

/// Run the embedded pipeline with the task context; returns the per-dataset
/// timing and row-count summary as a JSON object.
fn run_job(info: &JsonValue, store: &SharedStore) -> Result<JsonValue, String> {
    let kind = info.get("kind").and_then(|v| v.as_str()).unwrap_or("");
    if kind != KIND_ETL {
        return Err(format!("task is not an ETL task (kind='{kind}')"));
    }
    let job = info
        .get("job")
        .ok_or_else(|| "ETL task info has no 'job'".to_string())?;
    let job_json = sonic_to_json(job)?;
    let job: EtlJob = serde_json::from_value(job_json)
        .map_err(|e| format!("invalid ETL job payload: {e}"))?;

    let pipe: crate::config::Pipeline = serde_yaml::from_str(&job.pipeline_yaml)
        .map_err(|e| format!("invalid pipeline YAML in ETL job: {e}"))?;
    let name = pipe.name.clone().unwrap_or_else(|| "unnamed".into());
    let cfg = PipelinesConfig {
        pipelines: vec![pipe],
        execution: vec![],
    };
    let res = crate::plazy::run_pipeline_task_shared(&cfg, &name, Some(info), Some(store))
        .map_err(|e| format!("pipeline '{name}' failed: {e}"))?;

    let mut timing = Vec::new();
    for (step, ms) in &res.timing {
        timing.push(serde_json::json!({"step": step, "ms": ms}));
    }
    let mut datasets = serde_json::Map::new();
    for (ds, rows) in &res.datasets {
        datasets.insert(ds.clone(), serde_json::json!(rows));
    }
    let out = serde_json::json!({"timing": timing, "datasets": datasets});
    json_to_sonic(&out)
}

/// Soft failures worth retrying vs. hard failures that should kill the task.
fn is_retryable(err: &str) -> bool {
    let e = err.to_ascii_lowercase();
    [
        "network",
        "connection",
        "timeout",
        "timed out",
        "too many reque",
        "429",
        "503",
        "rate limit",
        "throttl",
        "temporarily",
    ]
    .iter()
    .any(|kw| e.contains(kw))
}

// ---------------------------------------------------------------------------
// one-shot driver
// ---------------------------------------------------------------------------

/// Boot a scheduler with the [`EtlHook`] and run the whole config. Stages are
/// submitted and awaited one by one (a stage is terminal when all of its tasks
/// are `FINISHED` or `KILLED`; the first kill aborts the run). Legacy configs
/// without `execution:` run with a single worker slot (strict serial order).
pub fn run_scheduled(
    cfg_yaml: &str,
    workers: usize,
    retries: usize,
    poll_ms: u64,
) -> Result<serde_json::Value, String> {
    let cfg: PipelinesConfig = serde_yaml::from_str(cfg_yaml)
        .map_err(|e| format!("invalid ETL YAML config: {e}"))?;
    if cfg.pipelines.is_empty() {
        return Err("config declares no pipelines".to_string());
    }
    for p in &cfg.pipelines {
        if p.datasets.is_empty() {
            return Err(format!(
                "pipeline '{}' must declare at least one dataset",
                p.name.as_deref().unwrap_or("unnamed")
            ));
        }
    }

    let slots = if cfg.execution.is_empty() {
        1
    } else {
        workers.max(1)
    };
    // `retries` = extra attempts *after* the first; total attempts = retries + 1.
    let max_attempts = if retries == 0 { 0 } else { (retries + 1) as u32 };

    let plan = build_plan(&cfg, max_attempts)?;
    let store = Arc::new(SharedStore::new());
    let rt = SchedulerRuntime::spawn(Arc::new(EtlHook::new(Arc::clone(&store))), slots);
    let started = Instant::now();
    let mut kills: Vec<(String, String)> = Vec::new();

    for stage in &plan.stages {
        if stage.is_empty() {
            continue;
        }
        let ids: Vec<String> = stage.iter().map(|t| t.id.clone()).collect();
        for t in stage {
            rt.send(SchedCmd::Start {
                id: t.id.clone(),
                priority: t.priority,
                info: t.info.clone(),
                max_attempts,
            });
        }
        // wait until every task of this stage is terminal
        loop {
            let snap = rt.query();
            let mut active = false;
            for id in &ids {
                let t = snap.iter().find(|t| &t.id == id);
                match t {
                    // not registered yet (queue backlog): still active
                    None => active = true,
                    Some(t) => match t.state {
                        TaskState::Finished => {}
                        TaskState::Killed => {
                            kills.push((t.id.clone(), t.error.clone().unwrap_or_default()));
                        }
                        _ => active = true,
                    },
                }
            }
            if !kills.is_empty() || !active {
                break;
            }
            std::thread::sleep(Duration::from_millis(poll_ms.max(1)));
        }
        if !kills.is_empty() {
            break;
        }
    }

    let snap = rt.query();
    let mut timing_rows: Vec<serde_json::Value> = Vec::new();
    let mut datasets = serde_json::Map::new();
    let mut task_rows: Vec<serde_json::Value> = Vec::new();
    for t in &snap {
        task_rows.push(task_to_json(t)?);
        if t.state == TaskState::Finished {
            if let Some(r) = &t.result {
                let rj = sonic_to_json(r)?;
                if let Some(ti) = rj.get("timing").and_then(|x| x.as_array()) {
                    for row in ti {
                        timing_rows.push(row.clone());
                    }
                }
                if let Some(ds) = rj.get("datasets").and_then(|x| x.as_object()) {
                    for (k, v) in ds {
                        let union_rows = v.as_u64().unwrap_or(0)
                            + datasets.get(k).and_then(|x| x.as_u64()).unwrap_or(0);
                        datasets.insert(k.clone(), serde_json::json!(union_rows));
                    }
                }
            }
        }
    }

    let mut out = serde_json::Map::new();
    out.insert("ok".into(), serde_json::json!(kills.is_empty()));
    out.insert(
        "elapsed_ms".into(),
        serde_json::json!(started.elapsed().as_millis() as u64),
    );
    out.insert("slots".into(), serde_json::json!(slots));
    out.insert("timing".into(), serde_json::json!(timing_rows));
    out.insert("datasets".into(), serde_json::Value::Object(datasets));
    out.insert("tasks".into(), serde_json::json!(task_rows));
    if !kills.is_empty() {
        out.insert("errors".into(), serde_json::json!(kills));
    }
    let result = serde_json::Value::Object(out);
    let mut rt = rt;
    rt.close();
    Ok(result)
}

/// Render a scheduler `Task` as its JSON representation (snapshot row).
pub(crate) fn task_to_json(t: &Task) -> Result<serde_json::Value, String> {
    let info = sonic_to_json(&t.info)?;
    let checkpoint = match &t.checkpoint {
        Some(c) => Some(sonic_to_json(c)?),
        None => None,
    };
    let result = match &t.result {
        Some(r) => Some(sonic_to_json(r)?),
        None => None,
    };
    Ok(serde_json::json!({
        "id": t.id,
        "priority": t.priority,
        "state": t.state.as_str(),
        "info": info,
        "checkpoint": checkpoint,
        "result": result,
        "error": t.error,
        "attempts": t.attempts,
        "max_attempts": t.max_attempts,
        "created_ms": t.created_ms,
        "last_started_ms": t.last_started_ms,
        "finished_ms": t.finished_ms,
    }))
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn q4_example_runs_end_to_end() {
        let base = format!("{}/examples/scheduler", env!("CARGO_MANIFEST_DIR"));
        let yaml = std::fs::read_to_string(format!("{base}/pipelines.yml"))
            .expect("examples/scheduler/pipelines.yml");
        // tests run from the crate root: point the example's relative file://
        // URIs at examples/scheduler so the run is self-contained.
        let yaml = yaml
            .replace("file://market.json", &format!("file://{base}/market.json"))
            .replace("file://instruments.json", &format!("file://{base}/instruments.json"))
            .replace("file://out/", &format!("file://{base}/out/"));
        let res = run_scheduled(&yaml, 10, 2, 5).expect("scheduled run failed");
        let datasets = res
            .get("datasets")
            .and_then(|s| s.as_object())
            .expect("datasets object");
        let n = |name: &str| -> u64 { datasets.get(name).and_then(|v| v.as_u64()).unwrap_or(0) };
        assert_eq!(n("market"), 8, "market rows == 8 symbols");
        assert_eq!(n("options"), 9, "options rows == 9 legs (unwound)");
        assert_eq!(n("fanout"), 9, "fanout rows total across partitions");
        // fan-out: one worker-unique parquet per non-empty partition (7 symbols)
        let fanout_dir = format!("{base}/out/fanout");
        let files = std::fs::read_dir(&fanout_dir)
            .expect("fanout parquet dir")
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map(|x| x == "parquet").unwrap_or(false))
            .count();
        assert_eq!(files, 7, "one parquet per fanout partition");
        let _ = std::fs::remove_dir_all(format!("{base}/out"));
    }
}
