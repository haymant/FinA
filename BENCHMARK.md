# FinA benchmarks

Results and methodology for the ETL engine and the scheduler kernel. Everything
here is reproducible with the scripts linked below; any perf-sensitive change
should re-run the relevant benchmark and update the tables.

## 1. ETL engine — 3 GB / 150k records, single-core

Script: [`examples/bench_3gb.py`](examples/bench_3gb.py)

```
python examples/bench_3gb.py --config examples/demo/pipelines.yml --input examples/demo/uni.json --out /tmp/pq
```

These figures come from a 3 GB stress corpus (150k records, the same ETL
expressions as `data/pipelines.yml`, scaled up). The big input is kept out of
the repo by size; reproduce via the `rust_simdetl` harness or a large generated
`uni.json`. `disk read` (~2 s) is excluded from the ETL total because it is
identical for every path.

### Whole ETL — `fina_core.run_pipelines` (native sonic-rs, streaming, no DOM)

| dataset        | time     |
|----------------|----------|
| instrument_raw |  9,077 ms |
| instrument_master | 3,916 ms |
| instrument_unwound | 3,172 ms |
| **ETL total (sum of per-dataset)** | **16,165 ms** |
| wall clock (incl. read + allocator warm-up) | 19,067 ms |
| process peak RSS | ~12.7 GB |
| rows | 150k raw / 150k master / 420,262 unwound |

> Datasets that share a source are evaluated in **one streaming pass** (each
> record is parsed once and evaluated against every dataset in the group); a
> dataset with its own source or a join gets its own pass. The shared-scan time
> is split across the grouped datasets in the timing report.
>
> The engine streams records one at a time and never builds a `serde_json`/DOM
> representation, which is what keeps memory low for a 3 GB input and
> avoids the multi-GB DOM that a naive whole-file parse would allocate.
> (`loads`/`dumps`, when called explicitly, do materialize a DOM for Python
> interop; the ETL path itself avoids them.)
>
> Notes: single-core; 12-core box. The original single-dataset-pass runner
> measured 10,332 ms ETL / 15,604 ms wall / ~6.7 GB RSS; the grouped runner is
> the price of per-dataset sources and joins, and RSS is higher because output
> columns are materialized in full before write (the old runner streamed parquet
> row-groups and returned freed pages via the mimalloc allocator, which was
> dropped to keep the wheel importable on stock glibc).

### Known performance regression (TODO)

The current engine is slower and uses more memory than the original
single-dataset-pass runner (10,332 ms ETL / ~6.7 GB RSS on the 3 GB corpus
above). This is a deliberate trade-off from the schema generalization, but the
gap should be closed. Concrete levers, in expected order of impact:

1. **Streaming writes during the scan** — `scan_group` currently accumulates
   full output columns for every dataset and writes them only at the end. For
   non-partitioned `file://` targets (the common case) open a `ParquetSink`
   per dataset and flush row-groups every `CHUNK` rows, clearing the columns.
   This alone should bring RSS back near the old ~6.7 GB. Partitioned /
   `memory://` / `duckdb://` / joined targets still need full materialization
   (partition layout / table insert / join temp table).
2. **A low-fragmentation allocator without initial-exec TLS** — mimalloc was
   removed because its `#[thread_local]` produced `R_X86_64_TPOFF64` relocations,
   forcing glibc to statically allocate the module's ~8.6 KB TLS block (mostly
   duckdb's `pg_parser_state`) at `dlopen`, which overflowed the static-TLS
   surplus on stock glibc ("cannot allocate memory in static TLS block"). A
   wheel must have zero IE TLS relocs to import cleanly. Reintroducing an
   allocator (mimalloc or jemalloc) therefore requires building it with a
   general-dynamic TLS model (e.g. nightly `-Ztls-model=global-dynamic`), or
   shrinking the module's TLS block (excluding duckdb's libpg_query).
3. **One pass across multiple sources** — datasets reading different sources
   still get one pass each; a multi-source single pass would reuse the parse
   across sources that share the same underlying bytes.

Re-benchmark with `examples/bench_3gb.py` after any of these.

---

## 2. Scheduler — maximum task throughput in 60 s

Script: [`examples/scheduler/bench.py`](examples/scheduler/bench.py)

```
python examples/scheduler/bench.py --duration 60 --mode bounded   # steady state
python examples/scheduler/bench.py --duration 60 --mode stream     # unbounded registry
python examples/scheduler/bench.py --mode burst --count 200000     # one finite batch
```

### Metric

**Completed tasks per second over a 60-second wall window.** A task is
"completed" when it reaches a terminal state (`FINISHED` or `KILLED`). The
benchmark keeps the submission queue permanently non-empty, so the window rate
is the scheduler's sustained *consumption* rate, not the driver's poke rate.

Three modes measure subtly different things:

| mode     | question |
|----------|----------|
| `bounded` | max sustainable throughput with a bounded number of in-flight tasks (`--max-inflight`, default 10 000). The registry is still allowed to grow by finished tasks, so this is not an upper bound — see note below. |
| `stream`  | throughput including the full cost of an unbounded registry over the window. |
| `burst`   | total wall time to drain one finite batch (`--count`) — submission + completion of a single horde. |

Two further knobs attribute the cost:

* `--submit batch` (default) — inject tasks with the `restore` command,
  `--batch-size` (default 2000) tasks per JSON command, the kernel-fast path.
  `--submit fanout` — one `start()` PyO3 call per task, the per-task Python path.
* `--hook auto` (default, native `AutoFinishHook`, no Python on the hot loop) vs
  `--hook python` (a no-op Python `TaskHook` invoked under the GIL on every
  task) — isolates the PyO3/GIL hook cost.

The driver samples cheap per-state counters (`_core.scheduler_count`, an
O(registry) scan that does not materialize task JSON) once per `--sample-every`
seconds so accounting does not perturb the measurement.

### Known bottlenecks (what the number absorbs)

1. ~~Snapshot commit is O(registry) and runs on every event~~ — **fixed.** The
   scheduler used to clone + sort the **entire** task registry into the shared
   snapshot on every `dispatch()`/`finish` (`SchedulerActor::commit`), making a
   run of N tasks cost O(N²). The snapshot is now maintained **incrementally**:
   tasks are registered once on creation (append, amortized O(1)) and each state
   change patches a single row in place (O(1)), so bulk throughput is O(N)
   total. See the section "Snapshot redesign (O(N²) → O(N))" below.
2. **Two OS threads per task.** Dispatch spawns a worker thread, and a `finish`
   spawns a separate `on_finish` hook thread (even when that hook is a no-op).
   Thread creation is ~10–50 µs; at hundreds of thousands of tasks this dominates
   the millisecond budget. This is now the dominant remaining cost.
3. **Per-task PyO3/JSON cost** (only for `--submit fanout` / `--hook python`):
   every `start` pays a Python→JSON→PyO3→parse round trip, and every Python
   hook pays the GIL.
4. **Single-threaded actor + single-threaded driver.** The actix current-thread
   runtime serializes all scheduling work; the benchmark driver is one Python
   thread on top.

The gap between `fanout`/`python` and `batch`/`auto`, and between `stream` and
`bounded`, is the attribution for these layers.

### Native kernel reference

A pure-Rust actor-path benchmark (no Python, same queue/snapshot/threads) runs
as an ignored test. It submits a bounded **burst** (`FINA_KERNEL_BENCH_COUNT`,
default 20k) and reports how many reach a terminal state during a bounded settle
window (`FINA_KERNEL_BENCH_SETTLE_MS`, default 500):

```
FINA_KERNEL_BENCH_COUNT=20000 FINA_KERNEL_BENCH_SLOTS=256 \
  FINA_KERNEL_BENCH_SETTLE_MS=800 \
  cargo test --release -- --ignored --nocapture kernel_throughput
```

It brackets the Python-attributable overhead: any Python-facing run cannot beat
it, and the delta is the PyO3/GIL/JSON layers.

### Snapshot redesign (O(N²) → O(N))

The O(N²) came from rebuilding the whole shared `Snapshot` (clone all tasks +
sort by `order`) at the end of every dispatch and every finish event — each
event paid for all prior events. The redesign keeps `Snapshot { tasks: Vec<Task> }`
as the read API but stops rebuilding it:

* The actor now also tracks `order_ids: Vec<String>` (submission order,
  append-only) and `index: HashMap<String, usize>` (id → snapshot row).
* New tasks are **registered** once: append to `order_ids`, record the index,
  push a row (amortized O(1)) — no sort needed because `order` is assigned in
  creation order.
* Every mutation (`finish`/`kill`/`pause`/`resume`/`checkpoint`/`update`/
  `reschedule`/`dispatch`) **patches its single row in place** via `sync(id)`
  (O(1)).
* `commit()` is gone; the periodic tick and all handlers use the O(1) helpers.

Result: bulk throughput is O(N) total instead of O(N²), and registry size no
longer decouples throughput (see the flat max-inflight sweep below).

### Environment & latest numbers

To be reproduced per-machine (nothing about the scheduler is hardware-tuned);
actual runs should record, alongside the tables:

* hardware (CPU cores/model, RAM), OS, Python version, fina commit;
* `workers` (slots), `mode`, `submit`, `hook`, `max-inflight`, `batch-size`;
* submitted / finished / in-flight at the 60 s deadline;
* `tasks/sec`.

**Latest measured (date, commit, machine):**

Recorded on a DevBox (multi-core, Rust release + Python 3.13 PyO3 release build;
see `git rev-parse HEAD` for the exact commit). **Post snapshot-redesign** —
the O(N²) commit bottleneck is removed, so throughput is now flat across the
max-inflight sweep.

Python driver (`examples/scheduler/bench.py`, `workers=256`, `--submit batch`,
`--hook auto`, full `summary_json` captured for each run):

* **bounded, `--max-inflight 2000`, 60 s** → `13,387 tasks/sec`
  `{"mode":"bounded","workers":256,"duration_s":60.0,"max_inflight":2000,"submit":"batch","hook":"auto","window_s":60.004,"submitted":805533,"finished_at_window":803253,"tasks_per_sec":13387,"in_flight_pending":1438,"in_flight_running":256}`
* **max-inflight sweep** (30 s runs, same workers) — **flat** (the O(N²) decay
  is gone):
  * `--max-inflight 500` → `16,806 tasks/sec` (fin 504,255)
  * `--max-inflight 2000` → `16,695 tasks/sec` (fin 500,885)
  * `--max-inflight 10000` → `16,153 tasks/sec` (fin 484,591)
* **`--submit fanout`, 20 s, `--max-inflight 2000`** → `18,724 tasks/sec`
  (fin 374,501) — the per-task `start()` round trip no longer collapses.
* **`--mode burst --count 10000`** → total wall (submit+drain) `26,721 tasks/sec`.
* **`--mode stream` (20 s, `--max-total 200000`)** → `10,000 tasks/sec` and all
  200,000 drained (`in_flight_pending: 0`) — the unbounded registry no longer
  wedges the actor.
* **`--hook python`** (`FinishOnStart`, 10 s) → `9,984 tasks/sec` (fin 99,992) —
  now bounded by the PyO3/GIL layer, not the snapshot.

Native kernel reference (`kernel_throughput`, `count=20000`, `slots=256`,
`settle=800 ms`): **submitted 20,000, terminal 20,000, `terminal/sec ≈ 26,533`**
(submit wall 0.03 s, total 0.75 s) — the no-Python actor path. The Python
`batch`/`auto` runs (~13–18k/s) are now within ~2x of native, so the remaining
gap is the two-OS-threads-per-task overhead + driver round trips, not the
snapshot.

> **Where the remaining headroom is.** Throughput is now O(N) and dominated by
> the remaining §2 bottleneck: two OS thread spawns per task (`sched-{id}` +
> `sched-finish-{id}`). Replacing that with an in-process worker pool + notifier
> channels is the path to the native ~26k/s ceiling for the Python path, and a
> more memory-efficient pool would push beyond it.

**Deadlock note (PyO3 GIL + registry Mutex):** the original Python-hook path
could freeze. The cycle: the main thread took the global registry `Mutex` and
released the GIL inside `py.allow_threads`, while a `PyHook.on_start` thread
held the GIL and blocked on that same `Mutex`. Resolved in `src/lib.rs` by
storing an `Arc<SchedulerRuntime>` in the registry, cloning it under a short
lock, dropping the guard, and only then releasing the GIL — no native lock is
held across `allow_threads`.

---

## 3. Reproducibility rules

* Run from the repo root with the source tree installed (`uv pip install -e .`
  or `pip install -e .`) so `_core.abi3.so` matches the tested commit.
* Prefer `--release` for the native kernel reference; keep the Python build
  defaults (Release for PyO3 in `pyproject.toml`).
* Report the full `summary_json` line, not just the headline rate.
* If a bottleneck from section 2 is addressed, re-run all three modes plus the
  native reference and update the tables above.