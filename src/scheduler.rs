//! Generic durable OS-style task scheduler kernel built on actix.
//!
//! This module implements the core of the scheduler described in
//! docs/scheduler.md: a single actix `SchedulerActor` owns a priority queue,
//! a task registry and a bounded number of concurrency slots. Work done on
//! behalf of a task runs through a pluggable `TaskHook`. Independent tasks
//! run in parallel: each dispatched task's hook runs on its own OS thread and
//! reports back through a `TaskNotifier` (finish / kill / reschedule / pause /
//! checkpoint / update), so the actor itself never blocks.
//!
//! The kernel has no ETL knowledge: ETL-specific hooks and configuration
//! expansion live in `etl_sched.rs`, and the Python bridge in `lib.rs`.

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::sync::Arc;
use std::sync::RwLock;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use actix::prelude::*;
use serde::de;
use serde::{Deserialize, Serialize};
use sonic_rs::Value;

use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3::IntoPyObjectExt;

/// Opaque JSON payload used for task `info`, `checkpoint` and `result`.
pub type JsonValue = Value;

fn null_value() -> JsonValue {
    sonic_rs::from_str("null").expect("json null parse")
}

/// Granularity of the delayed (to-be-retried) queue flush.
const DELAY_TICK_MS: u64 = 100;

// ---------------------------------------------------------------------------
// Task state machine
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TaskState {
    /// Queued, not yet dispatched.
    #[default]
    Pending,
    /// Dispatched; a worker owns it.
    Running,
    /// Cooperative pause with an optional checkpoint.
    Paused,
    /// Terminal, successful.
    Finished,
    /// Terminal, failed (gave up or non-retryable).
    Killed,
}

impl TaskState {
    pub fn as_str(&self) -> &'static str {
        match self {
            TaskState::Pending => "PENDING",
            TaskState::Running => "RUNNING",
            TaskState::Paused => "PAUSED",
            TaskState::Finished => "FINISHED",
            TaskState::Killed => "KILLED",
        }
    }

    fn parse(s: &str) -> Option<TaskState> {
        match s.to_ascii_uppercase().as_str() {
            "PENDING" => Some(TaskState::Pending),
            "RUNNING" => Some(TaskState::Running),
            "PAUSED" => Some(TaskState::Paused),
            "FINISHED" => Some(TaskState::Finished),
            "KILLED" => Some(TaskState::Killed),
            _ => None,
        }
    }

    /// True for the two absorbing (terminal) states.
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn is_terminal(&self) -> bool {
        matches!(self, TaskState::Finished | TaskState::Killed)
    }
}

impl Serialize for TaskState {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for TaskState {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        TaskState::parse(&s)
            .ok_or_else(|| serde::de::Error::custom(format!("unknown task state '{s}'")))
    }
}

/// A scheduler task: identity + priority + opaque payload + lifecycle fields.
/// The whole struct is (de)serializable so tasks can be checkpointed, restored
/// and rendered in the snapshot consumed by Python.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
    pub id: String,
    #[serde(default)]
    pub priority: u32,
    #[serde(default)]
    pub info: JsonValue,
    #[serde(default)]
    pub state: TaskState,
    #[serde(default)]
    pub checkpoint: Option<JsonValue>,
    #[serde(default)]
    pub result: Option<JsonValue>,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub attempts: u32,
    #[serde(default)]
    pub max_attempts: u32,
    #[serde(default)]
    pub created_ms: i64,
    #[serde(default)]
    pub last_started_ms: Option<i64>,
    #[serde(default)]
    pub finished_ms: Option<i64>,
    #[serde(default)]
    pub order: u64,
    /// True when the task was released from pause / restore, so the hook is
    /// invoked with `on_resume` instead of `on_start`.
    #[serde(default)]
    pub resume: bool,
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Priority queue
// ---------------------------------------------------------------------------

/// Queue entry. Higher priority pops first; ties are FIFO by insertion order.
/// `order` is issued from a single global sequence, so no two live entries can
/// share an order (identity uniqueness makes the Ord/PartialEq contract hold).
#[derive(Debug, Clone, PartialEq, Eq)]
struct HeapKey {
    id: String,
    priority: u32,
    order: u64,
}

impl Ord for HeapKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.priority
            .cmp(&other.priority)
            .then_with(|| other.order.cmp(&self.order))
    }
}

impl PartialOrd for HeapKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

// ---------------------------------------------------------------------------
// Hooks and notifier
// ---------------------------------------------------------------------------

/// How a worker reports lifecycle events back to the scheduler. Implementations
/// are fire-and-forget; the caller (a hook thread) must never block on them.
pub trait TaskNotifier: Send + Sync {
    /// Task completed successfully with an optional result and final checkpoint.
    fn finish(&self, id: &str, result: Option<JsonValue>, checkpoint: Option<JsonValue>);
    /// Hard failure: terminal, never retried.
    fn kill(&self, id: &str, reason: Option<&str>);
    /// Soft failure that may be worth a retry.
    fn reschedule(&self, id: &str, reason: &str, retry_delay_ms: u64);
    /// Cooperative pause (worker requested); optionally records a checkpoint.
    #[allow(dead_code)]
    fn pause(&self, id: &str, checkpoint: Option<JsonValue>);
    /// Publish progress without changing state.
    #[allow(dead_code)]
    fn checkpoint(&self, id: &str, value: JsonValue);
    /// Publish a fresh `info` payload (status reporting).
    #[allow(dead_code)]
    fn update(&self, id: &str, info: JsonValue);
}

/// A pluggable worker. A scheduler owns one hook; every lifecycle event for a
/// task is routed to it. Implementations run on the task's own OS thread.
pub trait TaskHook: Send + Sync {
    /// First dispatch of the task.
    fn on_start(&self, task: &Task, notifier: Arc<dyn TaskNotifier>);
    /// Task was cooperatively paused (default: do nothing).
    fn on_pause(&self, _task: &Task, _notifier: Arc<dyn TaskNotifier>, _checkpoint: Option<&JsonValue>) {}
    /// Task was released from pause / restored (default: do nothing).
    fn on_resume(&self, _task: &Task, _notifier: Arc<dyn TaskNotifier>, _checkpoint: Option<&JsonValue>) {}
    /// Task finished (default: do nothing).
    fn on_finish(&self, _task: &Task, _notifier: Arc<dyn TaskNotifier>) {}
    /// Task killed (default: do nothing).
    fn on_kill(&self, _task: &Task, _notifier: Arc<dyn TaskNotifier>, _reason: Option<&str>) {}
    /// Task was rescheduled for another attempt (default: do nothing).
    fn on_reschedule(&self, _task: &Task, _notifier: Arc<dyn TaskNotifier>, _reason: &str) {}
}

/// Default hook: every task auto-finishes immediately. Keeps the pristine
/// kernel fully functional (state machine / priority / pause / restore)
/// without any worker attached.
pub struct AutoFinishHook;

impl TaskHook for AutoFinishHook {
    fn on_start(&self, t: &Task, n: Arc<dyn TaskNotifier>) {
        n.finish(&t.id, None, None);
    }
    fn on_resume(&self, t: &Task, n: Arc<dyn TaskNotifier>, _c: Option<&JsonValue>) {
        n.finish(&t.id, None, None);
    }
}

/// Hook that delegates every lifecycle callback to a Python object (a
/// `fina.scheduler.TaskHook` subclass). Each method is invoked with the GIL
/// acquired on the dispatching thread.
pub struct PyHook {
    obj: Py<PyAny>,
}

impl PyHook {
    pub fn new(obj: Py<PyAny>) -> Self {
        Self { obj }
    }

    fn task_dict(py: Python<'_>, t: &Task) -> PyResult<PyObject> {
        let d = PyDict::new(py);
        d.set_item("id", t.id.as_str())?;
        d.set_item("priority", t.priority)?;
        d.set_item("state", t.state.as_str())?;
        d.set_item("resume", t.resume)?;
        d.set_item("info", crate::sonic_to_py(py, &t.info)?)?;
        if let Some(c) = &t.checkpoint {
            d.set_item("checkpoint", crate::sonic_to_py(py, c)?)?;
        }
        if let Some(r) = &t.result {
            d.set_item("result", crate::sonic_to_py(py, r)?)?;
        }
        if let Some(e) = &t.error {
            d.set_item("error", e.clone())?;
        }
        d.set_item("attempts", t.attempts)?;
        d.set_item("max_attempts", t.max_attempts)?;
        d.set_item("created_ms", t.created_ms)?;
        if let Some(x) = t.last_started_ms {
            d.set_item("last_started_ms", x)?;
        }
        if let Some(x) = t.finished_ms {
            d.set_item("finished_ms", x)?;
        }
        Ok(d.into_any().unbind())
    }

    fn opt_value(py: Python<'_>, v: Option<&JsonValue>) -> PyResult<PyObject> {
        match v {
            Some(x) => crate::sonic_to_py(py, x),
            None => Ok(py.None()),
        }
    }
}

impl TaskHook for PyHook {
    fn on_start(&self, task: &Task, _n: Arc<dyn TaskNotifier>) {
        let _ = Python::with_gil(|py| {
            let d = Self::task_dict(py, task)?;
            self.obj.bind(py).call_method1("on_start", (d,))?;
            Ok::<(), PyErr>(())
        });
    }

    fn on_pause(&self, task: &Task, _n: Arc<dyn TaskNotifier>, checkpoint: Option<&JsonValue>) {
        let _ = Python::with_gil(|py| {
            let d = Self::task_dict(py, task)?;
            let c = Self::opt_value(py, checkpoint)?;
            self.obj.bind(py).call_method1("on_pause", (d, c))?;
            Ok::<(), PyErr>(())
        });
    }

    fn on_resume(&self, task: &Task, _n: Arc<dyn TaskNotifier>, checkpoint: Option<&JsonValue>) {
        let _ = Python::with_gil(|py| {
            let d = Self::task_dict(py, task)?;
            let c = Self::opt_value(py, checkpoint)?;
            self.obj.bind(py).call_method1("on_resume", (d, c))?;
            Ok::<(), PyErr>(())
        });
    }

    fn on_finish(&self, task: &Task, _n: Arc<dyn TaskNotifier>) {
        let _ = Python::with_gil(|py| {
            let d = Self::task_dict(py, task)?;
            self.obj.bind(py).call_method1("on_finish", (d,))?;
            Ok::<(), PyErr>(())
        });
    }

    fn on_kill(&self, task: &Task, _n: Arc<dyn TaskNotifier>, reason: Option<&str>) {
        let _ = Python::with_gil(|py| {
            let d = Self::task_dict(py, task)?;
            let r = match reason {
                Some(s) => s.into_py_any(py)?,
                None => py.None(),
            };
            self.obj.bind(py).call_method1("on_kill", (d, r))?;
            Ok::<(), PyErr>(())
        });
    }

    fn on_reschedule(&self, task: &Task, _n: Arc<dyn TaskNotifier>, reason: &str) {
        let _ = Python::with_gil(|py| {
            let d = Self::task_dict(py, task)?;
            self.obj
                .bind(py)
                .call_method1("on_reschedule", (d, reason))?;
            Ok::<(), PyErr>(())
        });
    }
}

// ---------------------------------------------------------------------------
// Actor messages
// ---------------------------------------------------------------------------

macro_rules! msg {
    ($($t:ty),* $(,)?) => {
        $(impl Message for $t { type Result = (); })*
    };
}

#[derive(Debug, Clone)]
pub struct CmdStart {
    pub id: String,
    pub priority: u32,
    pub info: JsonValue,
    pub max_attempts: u32,
}
#[derive(Debug, Clone)]
pub struct CmdPause {
    pub id: String,
    pub checkpoint: Option<JsonValue>,
}
#[derive(Debug, Clone)]
pub struct CmdResume {
    pub id: String,
    pub checkpoint: Option<JsonValue>,
}
#[derive(Debug, Clone)]
pub struct CmdKill {
    pub id: String,
    pub reason: Option<String>,
}
#[derive(Debug, Clone)]
pub struct CmdFinish {
    pub id: String,
    pub result: Option<JsonValue>,
    pub checkpoint: Option<JsonValue>,
}
#[derive(Debug, Clone)]
pub struct CmdReschedule {
    pub id: String,
    pub reason: String,
    pub retry_delay_ms: u64,
}
#[derive(Debug, Clone)]
pub struct CmdCheckpoint {
    pub id: String,
    pub value: JsonValue,
}
#[derive(Debug, Clone)]
pub struct CmdUpdate {
    pub id: String,
    pub info: JsonValue,
}
#[derive(Debug, Clone)]
pub struct CmdRestore {
    pub tasks: Vec<Task>,
}
#[derive(Debug, Clone)]
pub struct CmdSetSlots {
    pub slots: usize,
}

msg!(
    CmdStart, CmdPause, CmdResume, CmdKill, CmdFinish, CmdReschedule,
    CmdCheckpoint, CmdUpdate, CmdRestore, CmdSetSlots
);

// ---------------------------------------------------------------------------
// Actor
// ---------------------------------------------------------------------------

pub struct SchedulerActor {
    tasks: HashMap<String, Task>,
    queue: BinaryHeap<HeapKey>,
    delayed: Vec<(Instant, HeapKey)>,
    hook: Arc<dyn TaskHook>,
    slots: usize,
    running: usize,
    order: u64,
    me: Option<Arc<ActorNotifier>>,
    snapshot: Arc<RwLock<Snapshot>>,
    /// Submission-ordered ids. Grows only by append; a task's position in this
    /// vec is stable for its lifetime (ids are never removed or reordered), so
    /// its snapshot row can be patched in-place in O(1).
    order_ids: Vec<String>,
    /// id -> index into `order_ids` (and the snapshot rows).
    index: HashMap<String, usize>,
}

#[derive(Debug, Clone, Default)]
pub struct Snapshot {
    pub tasks: Vec<Task>,
}

impl SchedulerActor {
    pub fn new(hook: Arc<dyn TaskHook>, snapshot: Arc<RwLock<Snapshot>>, slots: usize) -> Self {
        Self {
            tasks: HashMap::new(),
            queue: BinaryHeap::new(),
            delayed: Vec::new(),
            hook,
            slots: slots.max(1),
            running: 0,
            order: 0,
            me: None,
            snapshot,
            order_ids: Vec::new(),
            index: HashMap::new(),
        }
    }

    fn next_order(&mut self) -> u64 {
        let o = self.order;
        self.order += 1;
        o
    }

    /// Append a freshly-created task to the snapshot (amortized O(1)). Idempotent:
    /// re-registering an id that already has a row is a no-op (the caller then
    /// uses `sync` to update the existing row).
    fn register(&mut self, t: &Task) {
        if self.index.contains_key(&t.id) {
            return;
        }
        let pos = self.order_ids.len();
        self.order_ids.push(t.id.clone());
        self.index.insert(t.id.clone(), pos);
        if let Ok(mut snap) = self.snapshot.write() {
            snap.tasks.push(t.clone());
        }
    }

    /// Patch a single task's row in the snapshot in place (O(1)). No-op for
    /// unknown ids.
    fn sync(&mut self, id: &str) {
        let Some(&pos) = self.index.get(id) else {
            return;
        };
        let Some(t) = self.tasks.get(id) else {
            return;
        };
        if let Ok(mut snap) = self.snapshot.write() {
            if let Some(row) = snap.tasks.get_mut(pos) {
                *row = t.clone();
            }
        }
    }

    /// Register a task's snapshot row if it is new, otherwise patch it in place.
    /// Reads the authoritative task from `self.tasks`; the task must already be
    /// inserted there.
    fn upsert(&mut self, id: &str) {
        if self.index.contains_key(id) {
            self.sync(id);
        } else if let Some(t) = self.tasks.get(id).cloned() {
            self.register(&t);
        }
    }

    /// Promote delayed tasks whose deadline has passed back into the queue.
    fn move_delayed(&mut self) {
        let now = Instant::now();
        let mut ready = Vec::new();
        self.delayed.retain(|(at, key)| {
            if *at <= now {
                ready.push(key.clone());
                false
            } else {
                true
            }
        });
        for key in ready {
            self.queue.push(key);
        }
    }

    /// Fill free slots from the priority queue; dispatch each Pending task on
    /// its own OS thread.
    fn dispatch(&mut self) {
        if std::env::var_os("FINA_TRACE").is_some()
            && !self.queue.is_empty()
        {
            eprintln!(
                "[trace] dispatch enter queue={} running={} slots={}",
                self.queue.len(),
                self.running,
                self.slots
            );
        }
        while self.running < self.slots {
            let Some(key) = self.queue.pop() else {
                break;
            };
            let Some(task) = self.tasks.get_mut(&key.id) else {
                continue;
            };
            if task.state != TaskState::Pending {
                // stale entry (paused/killed/rescheduled while queued)
                continue;
            }
            task.state = TaskState::Running;
            task.attempts = task.attempts.max(1);
            task.last_started_ms = Some(now_ms());
            self.running += 1;

            let resume = task.resume;
            task.resume = false;
            let snap = task.clone();
            let resume_checkpoint = snap.checkpoint.clone();
            let id = snap.id.clone();
            let hook = self.hook.clone();
            let notifier = self.me.clone().expect("actor started");
            if std::env::var_os("FINA_TRACE").is_some() {
                eprintln!("[trace] dispatch spawn {} (running={})", id, self.running);
            }
            spawn_thread(format!("sched-{id}"), move || {
                if resume {
                    hook.on_resume(&snap, notifier, resume_checkpoint.as_ref());
                } else {
                    hook.on_start(&snap, notifier);
                }
            });
            self.sync(&id);
        }
    }

    fn finish_task(&mut self, id: &str, result: Option<JsonValue>, checkpoint: Option<JsonValue>) {
        let Some(task) = self.tasks.get_mut(id) else {
            return;
        };
        if task.state != TaskState::Running {
            return;
        }
        task.state = TaskState::Finished;
        task.finished_ms = Some(now_ms());
        if result.is_some() {
            task.result = result;
        }
        if checkpoint.is_some() {
            task.checkpoint = checkpoint;
        }
        self.running = self.running.saturating_sub(1);
        let snap = task.clone();
        let hook = self.hook.clone();
        let n = self.me.clone().expect("actor started");
        if std::env::var_os("FINA_TRACE").is_some() {
            eprintln!("[trace] finish_task -> spawn hook thread");
        }
        spawn_thread(format!("sched-finish-{id}"), move || {
            hook.on_finish(&snap, n);
        });
        self.sync(id);
        self.dispatch();
    }

    fn kill_task(&mut self, id: &str, reason: Option<&str>) {
        let reason = reason.map(|s| s.to_string());
        let Some(task) = self.tasks.get_mut(id) else {
            return;
        };
        match task.state {
            TaskState::Finished | TaskState::Killed => return,
            _ => {}
        }
        let was_running = task.state == TaskState::Running;
        task.state = TaskState::Killed;
        task.error = reason.clone();
        task.finished_ms = Some(now_ms());
        if was_running {
            self.running = self.running.saturating_sub(1);
        }
        let snap = task.clone();
        let hook = self.hook.clone();
        let n = self.me.clone().expect("actor started");
        spawn_thread(format!("sched-kill-{id}"), move || {
            hook.on_kill(&snap, n, reason.as_deref());
        });
        self.sync(id);
        self.dispatch();
    }
}

impl Actor for SchedulerActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Context<Self>) {
        self.me = Some(Arc::new(ActorNotifier { addr: ctx.address() }));
        ctx.run_interval(Duration::from_millis(DELAY_TICK_MS), |act, _| {
            act.move_delayed();
            act.dispatch();
        });
    }
}

pub(crate) fn spawn_thread(name: String, f: impl FnOnce() + Send + 'static) {
    let _ = std::thread::Builder::new().name(name).spawn(f);
}

struct ActorNotifier {
    addr: Addr<SchedulerActor>,
}

impl TaskNotifier for ActorNotifier {
    fn finish(&self, id: &str, result: Option<JsonValue>, checkpoint: Option<JsonValue>) {
        self.addr.do_send(CmdFinish {
            id: id.to_string(),
            result,
            checkpoint,
        });
    }
    fn kill(&self, id: &str, reason: Option<&str>) {
        self.addr.do_send(CmdKill {
            id: id.to_string(),
            reason: reason.map(|s| s.to_string()),
        });
    }
    fn reschedule(&self, id: &str, reason: &str, retry_delay_ms: u64) {
        self.addr.do_send(CmdReschedule {
            id: id.to_string(),
            reason: reason.to_string(),
            retry_delay_ms,
        });
    }
    fn pause(&self, id: &str, checkpoint: Option<JsonValue>) {
        self.addr.do_send(CmdPause {
            id: id.to_string(),
            checkpoint,
        });
    }
    fn checkpoint(&self, id: &str, value: JsonValue) {
        self.addr.do_send(CmdCheckpoint {
            id: id.to_string(),
            value,
        });
    }
    fn update(&self, id: &str, info: JsonValue) {
        self.addr.do_send(CmdUpdate {
            id: id.to_string(),
            info,
        });
    }
}

impl Handler<CmdStart> for SchedulerActor {
    type Result = ();
    fn handle(&mut self, m: CmdStart, _ctx: &mut Self::Context) {
        if self.tasks.contains_key(&m.id) {
            return;
        }
        let order = self.next_order();
        let t = Task {
            id: m.id.clone(),
            priority: m.priority,
            info: m.info,
            state: TaskState::Pending,
            checkpoint: None,
            result: None,
            error: None,
            attempts: 0,
            max_attempts: m.max_attempts,
            created_ms: now_ms(),
            last_started_ms: None,
            finished_ms: None,
            order,
            resume: false,
        };
        self.tasks.insert(m.id.clone(), t);
        self.upsert(&m.id);
        self.queue.push(HeapKey {
            id: m.id,
            priority: m.priority,
            order,
        });
        self.dispatch();
    }
}

impl Handler<CmdPause> for SchedulerActor {
    type Result = ();
    fn handle(&mut self, m: CmdPause, _ctx: &mut Self::Context) {
        let Some(task) = self.tasks.get_mut(&m.id) else {
            return;
        };
        match task.state {
            TaskState::Running => {
                if m.checkpoint.is_some() {
                    task.checkpoint = m.checkpoint.clone();
                }
                task.state = TaskState::Paused;
                self.running = self.running.saturating_sub(1);
                let snap = task.clone();
                let hook = self.hook.clone();
                let n = self.me.clone().expect("actor started");
                spawn_thread(format!("sched-pause-{}", snap.id), move || {
                    hook.on_pause(&snap, n, snap.checkpoint.as_ref());
                });
                self.sync(&m.id);
                self.dispatch();
            }
            TaskState::Pending => {
                if m.checkpoint.is_some() {
                    task.checkpoint = m.checkpoint.clone();
                }
                task.state = TaskState::Paused;
                self.sync(&m.id);
            }
            _ => {}
        }
    }
}

impl Handler<CmdResume> for SchedulerActor {
    type Result = ();
    fn handle(&mut self, m: CmdResume, _ctx: &mut Self::Context) {
        let Some(task) = self.tasks.get_mut(&m.id) else {
            return;
        };
        if task.state != TaskState::Paused {
            return;
        }
        task.state = TaskState::Pending;
        task.resume = true;
        if m.checkpoint.is_some() {
            task.checkpoint = m.checkpoint.clone();
        }
        let (id, priority) = (task.id.clone(), task.priority);
        let order = self.next_order();
        self.queue.push(HeapKey { id, priority, order });
        self.sync(&m.id);
        self.dispatch();
    }
}

impl Handler<CmdKill> for SchedulerActor {
    type Result = ();
    fn handle(&mut self, m: CmdKill, _ctx: &mut Self::Context) {
        self.kill_task(&m.id, m.reason.as_deref());
    }
}

impl Handler<CmdFinish> for SchedulerActor {
    type Result = ();
    fn handle(&mut self, m: CmdFinish, _ctx: &mut Self::Context) {
        if std::env::var_os("FINA_TRACE").is_some() {
            eprintln!("[trace] finish CmdFinish id={}", m.id);
        }
        self.finish_task(&m.id, m.result, m.checkpoint);
    }
}

impl Handler<CmdReschedule> for SchedulerActor {
    type Result = ();
    fn handle(&mut self, m: CmdReschedule, _ctx: &mut Self::Context) {
        let user_reason = m.reason.clone();
        let Some(task) = self.tasks.get_mut(&m.id) else {
            return;
        };
        task.attempts += 1;
        task.error = Some(user_reason.clone());
        let was_running = task.state == TaskState::Running;
        let exhausted = task.max_attempts > 0 && task.attempts > task.max_attempts;
        if exhausted {
            task.state = TaskState::Killed;
            task.finished_ms = Some(now_ms());
            if was_running {
                self.running = self.running.saturating_sub(1);
            }
            let snap = task.clone();
            let user_reason2 = user_reason.clone();
            let hook = self.hook.clone();
            let n = self.me.clone().expect("actor started");
            spawn_thread(format!("sched-kill-{}", snap.id), move || {
                hook.on_kill(&snap, n, Some(&user_reason2));
            });
            self.sync(&m.id);
            self.dispatch();
            return;
        }
        if was_running {
            self.running = self.running.saturating_sub(1);
        }
        task.state = TaskState::Pending;
        task.resume = false;
        let snap = task.clone();
        let priority = task.priority;
        let order = self.next_order();
        let key = HeapKey {
            id: m.id.clone(),
            priority,
            order,
        };
        if m.retry_delay_ms > 0 {
            self.delayed.push((
                Instant::now() + Duration::from_millis(m.retry_delay_ms),
                key,
            ));
        } else {
            self.queue.push(key);
        }
        let hook = self.hook.clone();
        let n = self.me.clone().expect("actor started");
        spawn_thread(format!("sched-resched-{}", snap.id), move || {
            hook.on_reschedule(&snap, n, &user_reason);
        });
        self.sync(&m.id);
        self.dispatch();
    }
}

impl Handler<CmdCheckpoint> for SchedulerActor {
    type Result = ();
    fn handle(&mut self, m: CmdCheckpoint, _ctx: &mut Self::Context) {
        if let Some(task) = self.tasks.get_mut(&m.id) {
            task.checkpoint = Some(m.value);
            self.sync(&m.id);
        }
    }
}

impl Handler<CmdUpdate> for SchedulerActor {
    type Result = ();
    fn handle(&mut self, m: CmdUpdate, _ctx: &mut Self::Context) {
        if let Some(task) = self.tasks.get_mut(&m.id) {
            task.info = m.info;
            self.sync(&m.id);
        }
    }
}

impl Handler<CmdRestore> for SchedulerActor {
    type Result = ();
    fn handle(&mut self, m: CmdRestore, _ctx: &mut Self::Context) {
        let n = m.tasks.len();
        if std::env::var_os("FINA_TRACE").is_some() {
            eprintln!("[trace] restore n={} enter", n);
        }
        for mut t in m.tasks {
            let id = t.id.clone();
            match t.state {
                TaskState::Finished | TaskState::Killed => {
                    self.tasks.insert(id.clone(), t);
                    self.upsert(&id);
                }
                _ => {
                    let interrupted = matches!(t.state, TaskState::Running | TaskState::Paused);
                    t.state = TaskState::Pending;
                    t.resume = interrupted;
                    if t.created_ms == 0 {
                        t.created_ms = now_ms();
                    }
                    let order = self.next_order();
                    t.order = order;
                    let (id2, priority) = (t.id.clone(), t.priority);
                    self.tasks.insert(id.clone(), t);
                    self.upsert(&id);
                    self.queue.push(HeapKey {
                        id: id2,
                        priority,
                        order,
                    });
                }
            }
        }
        self.dispatch();
        if std::env::var_os("FINA_TRACE").is_some() {
            eprintln!("[trace] restore n={} exit", n);
        }
    }
}

impl Handler<CmdSetSlots> for SchedulerActor {
    type Result = ();
    fn handle(&mut self, m: CmdSetSlots, _ctx: &mut Self::Context) {
        self.slots = m.slots.max(1);
        self.dispatch();
    }
}

// ---------------------------------------------------------------------------
// Runtime / handle
// ---------------------------------------------------------------------------

/// A handle to a running scheduler: the actor address, the snapshot and the
/// stop signal. `query` is lock-free against the actor's thread.
pub struct SchedulerRuntime {
    addr: Addr<SchedulerActor>,
    snapshot: Arc<RwLock<Snapshot>>,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
}

impl SchedulerRuntime {
    /// Boot an actix System on a dedicated thread and start the actor inside
    /// `block_on`, the canonical `System::new().block_on` lifecycle (see
    /// actix docs) — `Actor::start` resolves to the current runtime, so no
    /// arbiter plumbing is needed.
    pub fn spawn(hook: Arc<dyn TaskHook>, slots: usize) -> SchedulerRuntime {
        let snapshot: Arc<RwLock<Snapshot>> = Arc::new(RwLock::new(Snapshot::default()));
        let (addr_tx, addr_rx) = std::sync::mpsc::channel();
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
        let snap2 = snapshot.clone();
        std::thread::Builder::new()
            .name("fina-scheduler".to_string())
            .spawn(move || {
                actix_rt::System::new().block_on(async move {
                    let addr = SchedulerActor::new(hook, snap2, slots).start();
                    let _ = addr_tx.send(addr);
                    let _ = stop_rx.await;
                    if let Some(sys) = actix_rt::System::try_current() {
                        sys.stop();
                    }
                });
            })
            .expect("failed to spawn scheduler thread");
        let addr = addr_rx
            .recv_timeout(Duration::from_secs(30))
            .expect("scheduler actor did not start");
        SchedulerRuntime {
            addr,
            snapshot,
            stop: Some(stop_tx),
        }
    }

    /// Fire-and-forget dispatch of a JSON command to the actor.
    pub fn send(&self, cmd: SchedCmd) {
        match cmd {
            SchedCmd::Start {
                id,
                priority,
                info,
                max_attempts,
            } => self.addr.do_send(CmdStart {
                id,
                priority,
                info,
                max_attempts,
            }),
            SchedCmd::Pause { id, checkpoint } => {
                self.addr.do_send(CmdPause { id, checkpoint })
            }
            SchedCmd::Resume { id, checkpoint } => {
                self.addr.do_send(CmdResume { id, checkpoint })
            }
            SchedCmd::Kill { id, reason } => self.addr.do_send(CmdKill { id, reason }),
            SchedCmd::Finish {
                id,
                result,
                checkpoint,
            } => self.addr.do_send(CmdFinish {
                id,
                result,
                checkpoint,
            }),
            SchedCmd::Reschedule {
                id,
                reason,
                retry_delay_ms,
            } => self.addr.do_send(CmdReschedule {
                id,
                reason,
                retry_delay_ms,
            }),
            SchedCmd::Checkpoint { id, value } => self.addr.do_send(CmdCheckpoint { id, value }),
            SchedCmd::Update { id, info } => self.addr.do_send(CmdUpdate { id, info }),
            SchedCmd::Restore { tasks } => self.addr.do_send(CmdRestore { tasks }),
            SchedCmd::SetSlots { slots } => self.addr.do_send(CmdSetSlots { slots }),
        }
    }

    /// Clone of the current task list.
    pub fn query(&self) -> Vec<Task> {
        self.snapshot
            .read()
            .expect("snapshot poisoned")
            .tasks
            .clone()
    }

    /// Per-state counts, returned as [pending, running, paused, finished, killed].
    /// Scans the snapshot without cloning tasks (used by the throughput
    /// benchmark / a cheap `scheduler_count` bridge).
    pub fn counts(&self) -> [usize; 5] {
        let mut c = [0usize; 5];
        if let Ok(snap) = self.snapshot.read() {
            for t in &snap.tasks {
                c[match t.state {
                    TaskState::Pending => 0,
                    TaskState::Running => 1,
                    TaskState::Paused => 2,
                    TaskState::Finished => 3,
                    TaskState::Killed => 4,
                }] += 1;
            }
        }
        c
    }

    /// Stop the scheduler: signals the actor/system thread to shut down. The
    /// system thread is intentionally detached (never joined) to avoid GIL
    /// deadlocks when Python hooks are still running.
    pub fn close(&mut self) {
        if let Some(tx) = self.stop.take() {
            let _ = tx.send(());
        }
    }
}

// ---------------------------------------------------------------------------
// JSON command bridge
// ---------------------------------------------------------------------------

/// The JSON wire format used by the Python bridge (`scheduler_cmd`). Tagged
/// internally by `"cmd"`:
///
/// ```json
/// {"cmd": "start", "id": "...", "priority": 1, "max_attempts": 3, "info": {}}
/// {"cmd": "checkpoint", "id": "...", "value": {"offset": 42}}
/// {"cmd": "restore", "tasks": [ {Task JSON...}, ... ]}
/// ```
#[derive(Debug, Clone)]
pub enum SchedCmd {
    Start {
        id: String,
        priority: u32,
        info: JsonValue,
        max_attempts: u32,
    },
    Pause {
        id: String,
        checkpoint: Option<JsonValue>,
    },
    Resume {
        id: String,
        checkpoint: Option<JsonValue>,
    },
    Kill {
        id: String,
        reason: Option<String>,
    },
    Finish {
        id: String,
        result: Option<JsonValue>,
        checkpoint: Option<JsonValue>,
    },
    Reschedule {
        id: String,
        reason: String,
        retry_delay_ms: u64,
    },
    Checkpoint {
        id: String,
        value: JsonValue,
    },
    Update {
        id: String,
        info: JsonValue,
    },
    Restore {
        tasks: Vec<Task>,
    },
    SetSlots {
        slots: usize,
    },
}

impl<'de> Deserialize<'de> for SchedCmd {
    /// Commands carry JSON blobs (`info`, `checkpoint`, `result`, ...). sonic's
    /// `Value` only decodes through sonic's own deserializer, and *internally
    /// tagged* enums buffer their fields through serde's generic content
    /// deserializer — which sonic's `Value` cannot use. So we decode the whole
    /// document as `serde_json::Value` (generic-safe) and convert fields to
    /// sonic `Value` textually (`Task`s decode via sonic directly).
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let v = serde_json::Value::deserialize(deserializer)?;
        let cmd = v
            .get("cmd")
            .and_then(|c| c.as_str())
            .ok_or_else(|| de::Error::custom("scheduler command missing 'cmd'"))?;
        let text = |x: Option<&serde_json::Value>| -> JsonValue {
            match x {
                Some(x) => sonic_rs::from_str(&x.to_string()).unwrap_or_else(|_| null_value()),
                None => null_value(),
            }
        };
        let get_str =
            |v: &serde_json::Value, k: &str| -> Option<String> { v.get(k).and_then(|x| x.as_str()).map(String::from) };
        let get_u32 = |v: &serde_json::Value, k: &str| -> u32 { v.get(k).and_then(|x| x.as_u64()).unwrap_or(0) as u32 };
        let get_u64 = |v: &serde_json::Value, k: &str| -> u64 { v.get(k).and_then(|x| x.as_u64()).unwrap_or(0) };
        let get_val = |v: &serde_json::Value, k: &str| -> Option<JsonValue> {
            v.get(k).map(|x| text(Some(x)))
        };
        let tasks = |v: &serde_json::Value| -> Result<Vec<Task>, D::Error> {
            let arr = v
                .get("tasks")
                .and_then(|x| x.as_array())
                .ok_or_else(|| de::Error::custom("restore command missing 'tasks' array"))?;
            arr.iter()
                .map(|t| {
                    sonic_rs::from_str(&t.to_string())
                        .map_err(|e| de::Error::custom(format!("invalid task JSON: {e}")))
                })
                .collect()
        };
        match cmd {
            "start" => Ok(SchedCmd::Start {
                id: get_str(&v, "id").ok_or_else(|| de::Error::custom("start missing 'id'"))?,
                priority: get_u32(&v, "priority"),
                info: text(v.get("info")),
                max_attempts: get_u32(&v, "max_attempts"),
            }),
            "pause" => Ok(SchedCmd::Pause { id: get_str(&v, "id").ok_or_else(|| de::Error::custom("pause missing 'id'"))?, checkpoint: get_val(&v, "checkpoint") }),
            "resume" => Ok(SchedCmd::Resume { id: get_str(&v, "id").ok_or_else(|| de::Error::custom("resume missing 'id'"))?, checkpoint: get_val(&v, "checkpoint") }),
            "kill" => Ok(SchedCmd::Kill { id: get_str(&v, "id").ok_or_else(|| de::Error::custom("kill missing 'id'"))?, reason: get_str(&v, "reason") }),
            "finish" => Ok(SchedCmd::Finish { id: get_str(&v, "id").ok_or_else(|| de::Error::custom("finish missing 'id'"))?, result: get_val(&v, "result"), checkpoint: get_val(&v, "checkpoint") }),
            "reschedule" => Ok(SchedCmd::Reschedule { id: get_str(&v, "id").ok_or_else(|| de::Error::custom("reschedule missing 'id'"))?, reason: get_str(&v, "reason").ok_or_else(|| de::Error::custom("reschedule missing 'reason'"))?, retry_delay_ms: get_u64(&v, "retry_delay_ms") }),
            "checkpoint" => Ok(SchedCmd::Checkpoint { id: get_str(&v, "id").ok_or_else(|| de::Error::custom("checkpoint missing 'id'"))?, value: text(v.get("value")) }),
            "update" => Ok(SchedCmd::Update { id: get_str(&v, "id").ok_or_else(|| de::Error::custom("update missing 'id'"))?, info: text(v.get("info")) }),
            "restore" => Ok(SchedCmd::Restore { tasks: tasks(&v)? }),
            "set_slots" => Ok(SchedCmd::SetSlots { slots: get_u64(&v, "slots") as usize }),
            other => Err(de::Error::custom(format!("unknown scheduler command '{other}'"))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn null_val() -> JsonValue {
        sonic_rs::from_str("null").expect("json null")
    }

    fn push(h: &mut BinaryHeap<HeapKey>, id: &str, priority: u32, order: u64) {
        h.push(HeapKey {
            id: id.to_string(),
            priority,
            order,
        });
    }

    #[test]
    fn queue_pops_highest_priority_then_fifo() {
        let mut h = BinaryHeap::new();
        push(&mut h, "a", 1, 1);
        push(&mut h, "b", 3, 2);
        push(&mut h, "c", 3, 3);
        push(&mut h, "d", 2, 4);
        let mut ids = Vec::new();
        while let Some(k) = h.pop() {
            ids.push(k.id);
        }
        assert_eq!(ids, ["b", "c", "d", "a"]);
    }

    #[test]
    fn command_json_deserializes() {
        let cmd: SchedCmd =
            sonic_rs::from_str(r#"{"cmd":"start","id":"t1","priority":2,"info":{"x":1}}"#)
                .unwrap();
        match cmd {
            SchedCmd::Start { id, priority, .. } => {
                assert_eq!(id, "t1");
                assert_eq!(priority, 2);
            }
            _ => unreachable!(),
        }

        let restore: SchedCmd = sonic_rs::from_str(
            r#"{"cmd":"restore","tasks":[{"id":"a","state":"RUNNING","attempts":1}]}"#,
        )
        .unwrap();
        match restore {
            SchedCmd::Restore { tasks } => {
                assert_eq!(tasks.len(), 1);
                assert_eq!(tasks[0].state, TaskState::Running);
                assert_eq!(tasks[0].attempts, 1);
            }
            _ => unreachable!(),
        }
    }

    // ---------------------------------------------------------------------
    // Full lifecycle: exercises every command and every hook event against a
    // real actix runtime (System::new().block_on, single worker thread — actor
    // and test future interleave on the same tokio runtime).
    // ---------------------------------------------------------------------
    #[derive(Clone, Default)]
    struct RecHook(Arc<std::sync::Mutex<Vec<String>>>);

    impl TaskHook for RecHook {
        fn on_start(&self, t: &Task, _n: Arc<dyn TaskNotifier>) {
            self.0.lock().unwrap().push(format!("start {}", t.id));
        }
        fn on_pause(&self, t: &Task, _n: Arc<dyn TaskNotifier>, _c: Option<&JsonValue>) {
            self.0.lock().unwrap().push(format!("pause {}", t.id));
        }
        fn on_resume(&self, t: &Task, _n: Arc<dyn TaskNotifier>, _c: Option<&JsonValue>) {
            self.0.lock().unwrap().push(format!("resume {}", t.id));
        }
        fn on_finish(&self, t: &Task, _n: Arc<dyn TaskNotifier>) {
            self.0.lock().unwrap().push(format!("finish {}", t.id));
        }
        fn on_kill(&self, t: &Task, _n: Arc<dyn TaskNotifier>, _r: Option<&str>) {
            self.0.lock().unwrap().push(format!("kill {}", t.id));
        }
        fn on_reschedule(&self, t: &Task, _n: Arc<dyn TaskNotifier>, _r: &str) {
            self.0.lock().unwrap().push(format!("resched {}", t.id));
        }
    }

    fn val(txt: &str) -> JsonValue {
        sonic_rs::from_str(txt).unwrap_or_else(|_| null_val())
    }

    async fn wait_state(snapshot: &Arc<RwLock<Snapshot>>, id: &str, state: TaskState) {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let tasks = snapshot.read().unwrap().tasks.clone();
            if let Some(t) = tasks.iter().find(|t| t.id == id) {
                if t.state == state {
                    return;
                }
            }
            assert!(Instant::now() < deadline, "timeout waiting for {id} {state:?}");
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }

    fn task_snapshot(snapshot: &Arc<RwLock<Snapshot>>, id: &str) -> Task {
        snapshot
            .read()
            .unwrap()
            .tasks
            .iter()
            .find(|t| t.id == id)
            .cloned()
            .expect("task present")
    }

    #[test]
    fn actor_full_lifecycle() {
        actix_rt::System::new().block_on(async {
            let hook = Arc::new(RecHook::default());
            let snapshot = Arc::new(RwLock::new(Snapshot::default()));
            let slots = 2;
            let addr = SchedulerActor::new(hook.clone(), snapshot.clone(), slots).start();

            // START -> RUNNING, priority honored
            addr.do_send(CmdStart {
                id: "A".into(),
                priority: 1,
                info: val(r#"{"job":"a"}"#),
                max_attempts: 0,
            });
            wait_state(&snapshot, "A", TaskState::Running).await;

            // CHECKPOINT persists progress without changing state
            addr.do_send(CmdCheckpoint {
                id: "A".into(),
                value: val(r#"{"offset":42}"#),
            });
            tokio::time::sleep(Duration::from_millis(10)).await;
            assert_eq!(
                task_snapshot(&snapshot, "A").checkpoint,
                Some(val(r#"{"offset":42}"#))
            );

            // PAUSE (cooperative, with checkpoint) -> on_pause
            addr.do_send(CmdPause {
                id: "A".into(),
                checkpoint: Some(val(r#"{"offset":42}"#)),
            });
            wait_state(&snapshot, "A", TaskState::Paused).await;

            // RESUME -> on_resume (not on_start)
            addr.do_send(CmdResume {
                id: "A".into(),
                checkpoint: None,
            });
            wait_state(&snapshot, "A", TaskState::Running).await;

            // FINISH -> on_finish, result recorded
            addr.do_send(CmdFinish {
                id: "A".into(),
                result: Some(val(r#"{"rows":7}"#)),
                checkpoint: None,
            });
            wait_state(&snapshot, "A", TaskState::Finished).await;
            assert_eq!(
                task_snapshot(&snapshot, "A").result,
                Some(val(r#"{"rows":7}"#))
            );

            // RESCHEDULE soft failure; bounded by max_attempts.
            addr.do_send(CmdStart {
                id: "B".into(),
                priority: 2,
                info: val(r#"{"job":"b"}"#),
                max_attempts: 2,
            });
            wait_state(&snapshot, "B", TaskState::Running).await;
            addr.do_send(CmdReschedule {
                id: "B".into(),
                reason: "network glitch".into(),
                retry_delay_ms: 0,
            });
            wait_state(&snapshot, "B", TaskState::Running).await; // retried
            tokio::time::sleep(Duration::from_millis(20)).await;
            assert_eq!(task_snapshot(&snapshot, "B").attempts, 2);
            // second soft failure exceeds max_attempts=2 -> KILLED
            addr.do_send(CmdReschedule {
                id: "B".into(),
                reason: "network glitch 2".into(),
                retry_delay_ms: 0,
            });
            wait_state(&snapshot, "B", TaskState::Killed).await;
            tokio::time::sleep(Duration::from_millis(20)).await;
            assert_eq!(task_snapshot(&snapshot, "B").attempts, 3);

            // DIRECT KILL -> on_kill with reason
            addr.do_send(CmdStart {
                id: "C".into(),
                priority: 1,
                info: null_val(),
                max_attempts: 0,
            });
            wait_state(&snapshot, "C", TaskState::Running).await;
            addr.do_send(CmdKill {
                id: "C".into(),
                reason: Some("hard failure".into()),
            });
            wait_state(&snapshot, "C", TaskState::Killed).await;
            assert_eq!(
                task_snapshot(&snapshot, "C").error.as_deref(),
                Some("hard failure")
            );

            // RESTORE: Pending/Running/Paused tasks re-enqueue (Running/Paused
            // get the resume hook), terminal tasks are kept as-is; UPDATE
            // replaces info.
            addr.do_send(CmdRestore {
                tasks: vec![
                    Task {
                        id: "D".into(),
                        priority: 3,
                        info: null_val(),
                        state: TaskState::Running,
                        checkpoint: None,
                        result: None,
                        error: None,
                        attempts: 1,
                        max_attempts: 0,
                        created_ms: 0,
                        last_started_ms: None,
                        finished_ms: None,
                        order: 0,
                        resume: false,
                    },
                    Task {
                        id: "E".into(),
                        priority: 0,
                        info: null_val(),
                        state: TaskState::Finished,
                        checkpoint: None,
                        result: Some(val(r#"{"rows":3}"#)),
                        error: None,
                        attempts: 0,
                        max_attempts: 0,
                        created_ms: 5,
                        last_started_ms: None,
                        finished_ms: Some(9),
                        order: 0,
                        resume: false,
                    },
                ],
            });
            wait_state(&snapshot, "D", TaskState::Running).await; // restored -> dispatched, resume hook
            tokio::time::sleep(Duration::from_millis(10)).await;
            // E was terminal; stays FINISHED, never redispatched
            assert_eq!(task_snapshot(&snapshot, "E").state, TaskState::Finished);
            let ev = hook.0.lock().unwrap().clone();
            assert!(ev.iter().any(|e| e == "resume D"), "D resumes: {ev:?}");

            // UPDATE publishes a fresh info payload
            addr.do_send(CmdUpdate {
                id: "D".into(),
                info: val(r#"{"status":"processed-1of2"}"#),
            });
            tokio::time::sleep(Duration::from_millis(10)).await;
            assert_eq!(
                task_snapshot(&snapshot, "D").info,
                val(r#"{"status":"processed-1of2"}"#)
            );

            // durable finish of the restored task
            addr.do_send(CmdFinish {
                id: "D".into(),
                result: None,
                checkpoint: Some(val(r#"{"rows":3}"#)),
            });
            wait_state(&snapshot, "D", TaskState::Finished).await;

            // every hook event fired exactly as expected
            let ev = hook.0.lock().unwrap().clone();
            for expected in [
                "start A",
                "pause A",
                "resume A",
                "finish A",
                "start B",
                "resched B",
                "kill B",
                "start C",
                "kill C",
                "resume D",
                "finish D",
                "start E", // should NOT happen for a restored terminal task
            ] {
                let present = ev.iter().any(|e| e == expected);
                if expected == "start E" {
                    assert!(!present, "restored terminal task must not start: {ev:?}");
                } else {
                    assert!(present, "missing hook event {expected} in {ev:?}");
                }
            }
        });
    }

    // ---------------------------------------------------------------------
    // Kernel throughput benchmark (manual): runs the pure actix actor path —
    // restore-batched submissions against AutoFinishHook over a wall-clock
    // window, then drains and reports completed tasks / sec. Run with:
    //
    //   cargo test --release -- --ignored --nocapture kernel_throughput
    //
    // Tune with FINA_KERNEL_BENCH_COUNT (default 20k), *_SLOTS (default
    // 256) and *_SETTLE_MS (default 500). It submits a bounded number of
    // tasks (a burst) and reports how many reach a terminal state during the
    // settle window — the native dispatch path with no Python on the loop.
    // ---------------------------------------------------------------------
    #[test]
    #[ignore]
    fn kernel_throughput() {
        let slots = std::env::var("FINA_KERNEL_BENCH_SLOTS")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(256);

        actix_rt::System::new().block_on(async {
            let snapshot = Arc::new(RwLock::new(Snapshot::default()));
            let addr = SchedulerActor::new(Arc::new(AutoFinishHook), snapshot.clone(), slots).start();

            const BATCH: usize = 1000;
            let count = std::env::var("FINA_KERNEL_BENCH_COUNT")
                .ok()
                .and_then(|s| s.parse::<usize>().ok())
                .unwrap_or(20_000);
            let start = Instant::now();
            let mut submitted: usize = 0;
            let mut buf: Vec<Task> = Vec::with_capacity(BATCH);
            while submitted < count {
                buf.clear();
                let end = (submitted + BATCH).min(count);
                for _ in submitted..end {
                    buf.push(Task {
                        id: format!("t{submitted}"),
                        priority: 0,
                        info: null_val(),
                        state: TaskState::Pending,
                        checkpoint: None,
                        result: None,
                        error: None,
                        attempts: 0,
                        max_attempts: 0,
                        created_ms: now_ms(),
                        last_started_ms: None,
                        finished_ms: None,
                        order: 0,
                        resume: false,
                    });
                    submitted += 1;
                }
                addr.do_send(CmdRestore { tasks: std::mem::take(&mut buf) });
            }
            let submit_elapsed = start.elapsed().as_secs_f64();

            // settle: give the actor a bounded grace period to drain the
            // backlog, then report what reached a terminal state. We do NOT
            // wait for full drain: the O(registry) snapshot commit on every
            // event means a saturated registry may not fully drain in a bounded
            // time, and the metric of interest is terminal throughput, not
            // drain completion.
            let settle_for = Duration::from_millis(
                std::env::var("FINA_KERNEL_BENCH_SETTLE_MS")
                    .ok()
                    .and_then(|s| s.parse::<u64>().ok())
                    .unwrap_or(500),
            );
            let settle_deadline = Instant::now() + settle_for;
            let terminal = loop {
                let tasks = snapshot.read().unwrap().tasks.clone();
                let terminal = tasks
                    .iter()
                    .filter(|t| t.state.is_terminal())
                    .count();
                if terminal >= submitted || Instant::now() >= settle_deadline {
                    break terminal;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            };
            let total = start.elapsed().as_secs_f64();
            let rate = terminal as f64 / total.max(1e-9);
            let leftover = {
                let tasks = snapshot.read().unwrap().tasks.clone();
                tasks.iter().filter(|t| !t.state.is_terminal()).count()
            };
            println!(
                "\n=== kernel_throughput | slots={slots} count={count} settle={settle_for:?} === \
                 \nsubmitted   : {submitted} \
                 \nterminal    : {terminal} \
                 \nin_flight   : {leftover} (non-terminal at settle end) \
                 \nsubmit wall : {submit_elapsed:.2}s \
                 \ntotal wall  : {total:.2}s \
                 \nterminal/sec: {rate:.0}",
            );
            assert!(terminal > 0, "benchmark completed no tasks");
        });
    }
}