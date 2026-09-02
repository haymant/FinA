# sonicetl scheduler — Rust–Python durable OS-style task scheduler

Status: **implemented** (Rust kernel + ETL intake + Python bindings + tests).

The scheduler is a **general-purpose, durable, distributed-OS-style task
scheduler** written in Rust on top of the [actix] actor framework, exposed to
Python through PyO3. It is not ETL-specific: ETL is one *adapter* that turns a
`pipelines:` config into scheduler tasks. Any Python program can use the same
kernel to build its own durable, prioritized, checkpointable worker
orchestration (DuckDB / Redis / Kafka persistence, remote workers, etc.).

This document is the reference design. It supersedes the earlier draft (which
kept a growing in-memory hook trait but had no actor model, no `reschedule`,
no checkpoint-based `resume`, and no ETL intake).

[actix]: https://github.com/actix/actix

---

## 1. Why an actor model (actix)

The kernel is an **actix actor** (`SchedulerActor`):

- a single actor owns the **priority queue**, the task registry, and the
  concurrency slots, so all scheduling decisions are serialized (no locks on
  the hot path);
- messages (`start`, `pause`, `resume`, `kill`, `finish`, `reschedule`,
  `checkpoint`, `update`, `restore`, `set_slots`) are sent **fire-and-forget**
  from any thread — worker threads, Python threads, hooks — so nothing ever
  blocks the caller;
- each dispatched task runs its hook on its **own OS thread**, so independent
  tasks genuinely execute in parallel (needed for fan-out) while the actor
  itself only mutates bookkeeping state;
- a shared `RwLock<Snapshot>` is updated by the actor on every transition so
  Python can query task state without an actor round-trip.

One actix `System` (one tokio runtime) runs on a dedicated Rust thread per
scheduler; the Python layer talks to it via a numeric handle and JSON
commands.

## 2. Core requirements

- **Priority queue scheduling** — tasks ordered by `priority` (higher first),
  FIFO tie-break by insertion order.
- **Lifecycle management** — `start`, `pause`, `resume`, `kill`, `finish`,
  plus **`reschedule`** (retry with optional delay and a retry cap).
- **Custom taskinfo** — JSON payload carried per task; workers put
  domain-specific status here (sub-pipeline config, instrument partition, ...).
- **Event hooks** — `on_start`, `on_pause`, `on_resume`, `on_kill`,
  `on_finish`, **`on_reschedule`**; implemented in Rust (e.g. the built-in ETL
  hook) or in Python (user hooks).
- **Checkpoint-based resume** — a task can be paused with a checkpoint,
  resumed from it, and the whole registry can be exported/restored for
  crash recovery.
- **Dual interface** — hooks and commands callable from Rust directly or via
  the Python wrapper.
- **Fallback mode** — with no hook registered the scheduler runs fully in
  memory (pure queue + state machine).

## 3. Task model

```rust
pub enum TaskState { Pending, Running, Paused, Finished, Killed }

pub struct Task {
    pub id: String,
    pub priority: u32,
    pub info: sonic_rs::Value,      // JSON taskinfo (arbitrary worker payload)
    pub state: TaskState,
    pub checkpoint: Option<Value>,  // opaque checkpoint for resume/recovery
    pub result: Option<Value>,      // status payload attached on finish
    pub error: Option<String>,
    pub attempts: u32,
    pub max_attempts: u32,          // 0 = no retry cap
    pub created_ms / last_started_ms / finished_ms: u128,
    pub order: u64,                 // FIFO tie-break sequence
}
```

The JSON a Python caller sees for a task is:

```json
{
  "id": "prod/x/2",
  "priority": 0,
  "state": "RUNNING",           // PENDING|RUNNING|PAUSED|FINISHED|KILLED
  "info":  {"kind": "etl", ...},
  "checkpoint": null,
  "result":  null,
  "error":   null,
  "attempts": 1,
  "created_ms": 1756789123456,
  "last_started_ms": 1756789123478,
  "finished_ms": null
}
```

## 4. Priority queue

`BinaryHeap` of heap keys `{ id, priority, order }`, ordered so the **greatest**
element is `priority`-max then `order`-min:

```rust
impl Ord for HeapKey {
    fn cmp(&self, o: &Self) -> Ordering {
        self.priority.cmp(&o.priority)
            .then_with(|| o.order.cmp(&self.order))  // earlier order pops first
    }
}
```

A task is popped into a concurrency slot only while it is `Pending`; the actor
keeps a detached `delayed` list for `reschedule(retry_delay_ms > 0)`.

## 5. Hooks

```rust
pub trait TaskHook: Send + Sync {
    fn on_start      (&self, task: &Task, n: TaskNotifier);
    fn on_pause      (&self, task: &Task, n: TaskNotifier, checkpoint: Option<&Value>) {}
    fn on_resume     (&self, task: &Task, n: TaskNotifier, checkpoint: Option<&Value>) {}
    fn on_kill       (&self, task: &Task, n: TaskNotifier, reason: Option<&str>) {}
    fn on_finish     (&self, task: &Task, n: TaskNotifier) {}
    fn on_reschedule (&self, task: &Task, n: TaskNotifier, reason: &str) {}
}
```

`TaskNotifier` is a cloneable gate back into the actor:

```rust
pub trait TaskNotifier: Send + Sync {
    fn finish(&self, id: &str, result: Option<Value>, checkpoint: Option<Value>);
    fn kill(&self, id: &str, reason: Option<&str>);
    fn reschedule(&self, id: &str, reason: &str, retry_delay_ms: u64);
    fn pause(&self, id: &str, checkpoint: Option<Value>);
    fn checkpoint(&self, id: &str, value: Value);
    fn update(&self, id: &str, info: Value);
}
```

Hooks are implemented either in Rust (the built-in ETL hook) or in Python
(a `PyHook` adapter that attaches the GIL and calls the user methods). With no
hook the scheduler uses `AutoFinishHook`, which finishes every task the moment
it starts → pure in-memory queue state machine.

## 6. Commands and state transitions

| command            | from                    | to      | event fired            |
|--------------------|-------------------------|---------|------------------------|
| `start(info, prio)`| —                       | Pending | —                      |
| dispatch (slot)    | Pending                | Running | `on_start`             |
| `pause(id)`        | Running / Pending      | Paused  | `on_pause(checkpoint)` (Running only) |
| `resume(id, chk?)` | Paused                 | Pending→Running | `on_resume(checkpoint)` |
| `kill(id, reason)` | Running / Pending / Paused | Killed | `on_kill(reason)`  |
| `finish(id, chk?, result?)` | Running        | Finished | `on_finish`        |
| `reschedule(id, reason, delay)` | Running / Pending | Pending (delayed) | `on_reschedule(reason)` |
| `checkpoint(id, v)`| any                    | —       | —                     |
| `update(id, info)` | any                    | —       | —                     |
| `set_slots(n)`     | any                    | —       | —                     |
| `restore(tasks)`   | aborted → Pending      | Pending | —                     |

States:

```
         ┌──►  Paused ◄──────── resume ──────────┐
         │         ▲                              │
Pending ─┴──► Running ──► Finished                │
               │  │        (finish)               │
               │  └─ pause (checkpoint) ──────────┘
               └── kill / retries-exhausted ──► Killed
```

`reschedule` bumps `attempts`; when `attempts > max_attempts` (or no retries
allowed) the task is killed with the last reason. A retry delay parks the task
in the delayed list until its deadline, then it is eligible for dispatch again
at its (optionally elevated) priority.

## 7. Checkpoint & recovery

- `pause(id, checkpoint)` / `checkpoint(id, value)` store an opaque JSON blob
  on the task; `resume(id, checkpoint)` seeds it. Hooks that run external state
  use it to rebuild worker state on recovery.
- `tasks()` exports the full registry (state, info, checkpoint, attempts,
  result, error) — persist it anywhere (DuckDB, Redis, a file) to make the
  scheduler *durable*.
- `restore(tasks)` re-enqueues a persisted registry and dispatches pending
  work again — crash recovery without losing `Killed`/history semantics.
- Per-task hooks stay cooperative: pausing an in-flight runnable is handled at
  the scheduler level (slot freed, state recorded), and the worker either
  observes it on its next notifier call or is re-run from its checkpoint on the
  next `resume`.

## 8. Scheduler semantics

- `slots` = max simultaneously `Running` tasks (default `workers`).
- dispatch loop: after every mutation, move expired delayed tasks back into the
  queue, then pop `Pending` tasks into free slots (highest priority first).
- a `Running` task counts against a slot until `finish`/`kill`/`pause`
  (frees it) — or `reschedule` (frees it and re-queues).

## 9. Python interface

```python
import json
import sonicetl
from sonicetl.scheduler import Scheduler, TaskHook, run_pipelines_scheduled

class MyHook(TaskHook):
    """Python hooks receive task dicts on the task's own OS thread."""
    def on_start(self, task):            # task is a dict snapshot
        print("start", task["id"], task["info"])
        # ... do work; transition the task via scheduler.cmd(...) (reachable
        # by closure), e.g. schedule.cmd({"cmd": "kill", "id": task["id"]})
    def on_reschedule(self, task, reason): ...

sched = Scheduler(hook=MyHook(), workers=4)
sched.start("job/1", info={"url": "..."}, max_attempts=3)   # fire-and-forget
sched.cmd({"cmd": "reschedule", "id": "job/1", "reason": "network", "retry_delay_ms": 500})
sched.cmd({"cmd": "kill", "id": "job/1", "reason": "gave up"})
snap = sched.query()                     # [TaskInfo, ...]
t = sched.state("job/1"); t.terminal     # True when FINISHED/KILLED
done = sched.wait(["job/1"])             # block until terminal
sched.close()
```

`TaskHook` (see `src/scheduler.rs::PyHook`) is invoked under the GIL with a
task **dict**; a Python hook thread that must resolve a task calls
`scheduler.cmd(...)` with that task's `id` — the Python-side handle is whatever
the hook captured. Without a hook, `Scheduler()` uses `AutoFinishHook`, so
`start(); wait(...)` completes instantly (full state-machine smoke testing).

Every method is a thin wrapper over `_core.scheduler_cmd(handle, json)` /
`_core.scheduler_query(handle)`. `run_pipelines_scheduled(yaml, workers=1,
retries=2, poll_ms=100)` runs a whole ETL config through the scheduler in one
native call and returns an aggregate `{ok, elapsed_ms, slots, timing,
datasets, tasks, errors}`.

## 10. ETL intake (fan-out a pipeline to parallel workers)

The ETL ETA is the reference use of the kernel: extend the existing
`pipelines:` config so that legacy configs keep their **serial by default**
behaviour while new configs fan out.

### 10.1 Schema extension

```yaml
pipelines:
  - name: instrumentMaster              # ordinary pipeline (one task)
    sources: [{name: uni, uri: file://uni.json, format: json}]
    datasets: [...]
  - name: prodETL                       # to be fanned out below
    sources: [{name: uni, uri: file://uni.json, format: json}]
    datasets: [...]

execution:                        # optional; absent → run pipelines serially
  - group: [instrumentMaster]     #     stage 1 runs alone
  - group:                        #     stage 2: parallel group — either an
    - prodETL                     #       array of pipeline names, or
    - {pipeline: prodETL, partition: "partition(uni.name, 8)"}
```

- **stage** = one `group:`; stages run **serially** (a data-flow/ordering
  barrier).
- **group** = a list of nodes that run **in parallel**.
- **node** = a pipeline name (one task) or a fan-out node
  `{pipeline, partition}`.
- `partition(source.field, workers)` splits the universe of records read from
  `source` (a source declared by that pipeline) by the `field` value into
  `workers` **evenly-sized, order-preserving** chunks. One task is scheduled
  per chunk; every chunk is exposed to the sub-pipeline as `$task.ctx.units`
  (the list of instrument names) → **one worker per partition**.
- Each fan-out task is its own run of the sub-pipeline with an injected
  per-dataset filter `"$.<field> IN $task.ctx.units"`, so worker *i* only sees
  its own partition of the instrument universe.
- Legacy behaviour: no `execution:` → every pipeline is its own task in
  declaration order and the scheduler is created with `workers = 1`
  (one-by-one, exactly like today's `run_pipelines`).

### 10.2 Task info for an ETL job

```json
{
  "kind": "etl",
  "job": {
    "pipeline_yaml": "name: prodETL\nsources: [...]\ndatasets:\n  - {... filter: '$.name IN $task.ctx.units' ...}",
    "max_attempts": 3
  },
  "ctx": {
    "units": ["A", "B", "C"],
    "partition": {"index": 2, "of": 8},
    "universe": {"source": "uni", "field": "name"}
  }
}
```

The worker hook injects the scheduler task `id` into `info` (`info.id`) before
running, so a fan-out sub-pipeline can address worker-unique outputs.

### 10.3 Expression engine additions for workers

- **`$task.<path>`** — a new root in field/condition expressions that resolves
  against the task context (the JSON above), e.g. `$task.ctx.units`.
- **`x IN y`** — set membership: true iff the scalar `x` equals any element of
  the (array) operand `y`; e.g. `$.name IN $task.ctx.units`.
- **`Dataset.filter`** — optional boolean expression evaluated per output row;
  rows failing the filter are dropped for that dataset (used by the fan-out
  adapter, and useful on its own).
- **`{task}` / `{index}` / `{of}`** — output-URI placeholders substituted from
  the task context at run time (`{task}` = sanitized scheduler task id), so
  every fan-out worker writes to its own target (e.g.
  `to.uri: file://out/{task}.parquet`). A plain `run_pipelines` call leaves the
  placeholders as literal text.

### 10.4 Assembling a new JSON root per row (`row_root`)

A fan-out worker often needs to emit the *raw* source record *and* the joined
market row under one new JSON root, then extract typed columns. `Dataset.row_root`
maps child keys of the assembled document to their content:

* `"$"` — this source's raw record (whole JSON document);
* an **unwind alias** — the raw JSON of the unwound element for this row;
* a **join alias** — the joined row (the join's selected columns).

```yaml
datasets:
  - name: fanout
    type: unwound
    source: instruments
    to: {uri: file://out/fanout/{task}.parquet, format: parquet}
    unwind_rules:
      - {name: leg, condition: "$.legs[0]", unwind_path: "$.legs", output_alias: leg}
    join:
      alias: mkt
      target: memory://market
      left_key: "$.leg.underlying"
      right_key: symbol
      columns: [bid, ask, spot, exchange]
    row_root:             # one assembled JSON object per output row:
      instrument: "$"     #   the raw instrument document
      leg: leg            #   the unwound leg element
      market: mkt         #   the joined quote row
    fields:
      - {name: instrument_id, expression: "$.instrument.id"}
      - {name: leg_type, expression: "$.leg.leg"}
      - {name: spot_mid, expression: "cast($.market.spot as double)"}
```

Extracted fields become separate typed columns in the Parquet target. Output
rows (legs) exceed input records (instruments). See the end-to-end example in
`examples/scheduler/pipelines.yml`.

### 10.5 Shared store across tasks

Each ETL task of one scheduled run shares one in-memory duckdb backend
(`store::SharedStore`, an `Arc<Mutex<SharedDb>>` threaded through `EtlHook`):
`memory://` tables written by an earlier stage or worker stay visible to later
stages and parallel workers. Run a scheduled run and every fan-out partition
can join `memory://market` exactly as a plain `run_pipelines` would. Note that
a `run_pipeline_task` outside the scheduler still uses its *own* store.

## 11. Durable & distributed layers

- **In-process (default):** the actix actor + OS threads execute everything
  locally; `tasks()`/`restore()` give checkpoint-aware durability.
- **Python durability hooks:** a Python `TaskHook` can persist every event to
  DuckDB / Redis / Kafka (backed by the checkpoint JSON), run workers in other
  processes, and drive them back through the same notifier commands —
  effectively turning the kernel into a control plane for a distributed
  worker fleet.
- **Retry classification (built-in ETL hook):** a failed ETL task is
  `reschedule`d (bounded by `max_attempts`) when its error mentions network /
  connection / timeout / rate-limit hints (`network`, `connection`, `timeout`,
  `too many request`, `429`, `503`, `temporarily`); anything else is `kill`ed.

## 12. Reference API (Rust core + PyO3)

Rust modules:
- `src/scheduler.rs` — `Task`, `TaskState`, `TaskHook`, `TaskNotifier`,
  `SchedulerActor`, `SchedulerRuntime` (system thread + handle registry),
  priority queue, `SchedCmd`, `PyHook`/`AutoFinishHook`.
- `src/etl_sched.rs` — `EtlPlan` (config → task expansion), `EtlHook`
  (in-process ETL worker + retry classification), `run_scheduled`, `SharedStore`.
- `src/store.rs` — `SharedDb`, `SharedStore` (scheduler-wide in-memory store).
- `src/plazy.rs` — `run_pipeline_task_shared(cfg, name, task, store)`.

PyO3 functions on `sonicetl._core`:
- `scheduler_new(workers, hook) -> handle`
- `scheduler_cmd(handle, json)` — `start/pause/resume/kill/finish/reschedule/
  checkpoint/update/restore/set_slots`
- `scheduler_query(handle) -> tasks JSON`
- `scheduler_close(handle)`
- `expand_etl_config(yaml, retries) -> task-spec JSON` (no execution starts anything)
- `etl_scheduler_run(yaml, workers, retries, poll_ms) -> result JSON`
  (expand → schedule → wait → aggregate, one native call)

Python wrapper (`python/sonicetl/scheduler.py`):
- `Scheduler` — context-manager over the native handle: `start()`, `cmd()`,
  `query() -> list[TaskInfo]`, `state(id)`, `wait(ids)`, `close()`.
- `TaskHook` — Python base class for the six lifecycle callbacks.
- `run_pipelines_scheduled(yaml, workers=1, retries=2, poll_ms=100)` →
  `ScheduledRun` (`ok`, `rows`, `timing`, `tasks`, `errors`).
- `_core.expand_etl_config` returns the stage-ordered task plan as JSON.

## 13. Dev/test plan

1. Rust unit tests — priority ordering + FIFO tie-break; `$task` / `IN` /
   per-dataset filter; `partition()` chunking; config parsing (absent
   `execution:` = serial); `SchedCmd` JSON bridge (incl. `Value` fields).
2. Actor tests — lifecycle state machine (start→pause→resume→finish),
   `reschedule` with retry cap, restore-after-snapshot, slot concurrency,
   hook-event ordering (`src/scheduler.rs::tests`).
3. ETL end-to-end — `examples/scheduler/pipelines.yml` through
   `run_scheduled`: market → `memory://market`, instruments unwind+join →
   parquet, `partition(instruments.name, 10)` fan-out with `row_root`
   (`src/etl_sched.rs::tests::q4_example_runs_end_to_end`).
4. Python smoke tests — `python/tests/test_scheduler.py`: state machine via a
   `TaskHook`, auto-finish, config expansion, and the scheduled Q4 example.

## 14. Out of scope (v1)

- Preemptive pause of an in-flight native ETL scan (cooperative only; the
  checkpoint is the job spec so a resumed task replays from the checkpoint).
- Distributed worker pool and cross-node queue protocol (Python hooks are the
  extension point).
- Heartbeats / crash detection of worker threads.
- Interruptible scan checkpoints inside `plazy` (a whole-scan replay from a
  checkpoint is honored, not a mid-scan resume).