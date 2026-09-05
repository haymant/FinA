# FinA

**FinA (financial agent) — a native Python library backed by a Rust core: a
durable, OS-process-scheduler-like task core on top of which financial tasks
(ETL, greeks, P&L, …) are built.**

The heavy lifting happens in a compiled Rust extension (PyO3) while Python sees
a small, clean API. At the center is an **actix task scheduler** that behaves
like an OS process scheduler: tasks with priorities, concurrency slots, retries,
pause/resume, checkpoints and durable state, driven from Python through a single
`Scheduler` handle. **ETL is the first built-in task type** — read JSON → parse
each record natively → evaluate YAML field expressions directly against the
native `sonic_rs::Value` → stream typed columns to Parquet or a duckdb table —
**without ever materializing a `serde_json::Value` DOM**. More task types (greek
sensitivities, P&L calculation, …) plug in on top of the same core.

```
                    ┌──────────────────────────────────────────┐
  Python API ──────►│  FinA core (Rust)                        │
                    │  ┌────────────────────────────────────┐  │
                    │  │ task scheduler (OS scheduler-like) │  │
                    │  │  priorities · slots · retries      │  │
                    │  │  pause/resume · checkpoints · durab│  │
                    │  └────────────────────────────────────┘  │
                    │  ┌────────────────────────────────────┐  │
                    │  │ built-in task types                │  │
                    │  │  • ETL (native sonic-rs, no DOM)   │  │
                    │  │  • … greeks, P&L, more to come     │  │
                    │  └────────────────────────────────────┘  │
                    └──────────────────────────────────────────┘
```

The scheduler core is what the project is named after — everything else is a
task kind running on it.

[sonic-rs]: https://github.com/cloudwego/sonic-rs

---

## Features

- **Scheduler core** — a Rust/actix task scheduler that behaves like an OS
  process scheduler: priority queue, concurrency slots (`workers`), retries with
  backoff, pause/resume, checkpoints, durable task state, and a single shared
  snapshot. Driven from Python via `fina_core.Scheduler` (see
  [`docs/scheduler.md`](docs/scheduler.md)).
- **Scheduled pipelines** — `run_pipelines_scheduled(config, workers=…)` runs an
  ETL plan as a dependency graph of scheduler tasks: serial stages, concurrent
  fan-out partitions, retries, all through the scheduler core.
- **ETL task type** — `run_pipelines(config)` runs one or more pipelines, each
  with named sources and datasets, in a single native call.
- **JSON codecs** — `loads` / `dumps` powered by sonic-rs.
- **Store URIs** — every source and target is a duckdb-style URI:
  `file://` (parquet/JSON), `memory://<table>` (in-memory duckdb, shared across
  the call), or `duckdb://<file>?table=<t>` (file-backed tables).
- **LEFT JOIN** — datasets can join against tables produced earlier in the same
  call (`join:` block), with right columns projected as `alias.col`.
- **Hive partitioning** — `to.partition_by` writes `k=v/.../data.parquet` trees.
- **No DOM** — expressions evaluate against `sonic_rs::Value` directly
  (`src/native.rs` supplies a small accessor trait; `src/sonic.rs` implements it
  for sonic-rs). Peak memory stays ~2× input, not 5–8×.
- **Official ETL YAML schema** — see [`docs/schema.md`](docs/schema.md) and
  [`schema/etl.schema.json`](schema/etl.schema.json). Build configs
  programmatically with `PipelinesConfig` / `Pipeline` / `Source` / `Dataset` /
  `Field` / `UnwindRule` / `Join` / `Output`.
- **Streaming Parquet writer** — row-grouped, so even a multi-GB `json_blob`
  column never exceeds Arrow's `i32` offsets (`src/columnar.rs`).

Only JSON parsing library linked into the core is **sonic-rs**; the multi
backend scaffolding (`serde_json`, `simd-json`, `nosj`, `arrow-json`) was
removed.

---

## Installation

Prebuilt wheels are published to [PyPI], so installing from a package manager
is a one-liner — no Rust toolchain required. Requires **Python ≥ 3.9**.

```bash
pip install fina-core
# or, with uv:
uv add fina-core
```

Wheels are provided per platform/arch (Linux `manylinux`, macOS `x86_64` and
`arm64`, Windows `amd64`); `pip`/`uv` pick the right one automatically.

To install a specific version or into an existing env:

```bash
pip install "fina-core==0.1.1"
uv pip install fina-core --python 3.12
```

### Building from source (optional)

Not needed for normal use. If you're on an unsupported platform you can build
the Rust extension yourself with [maturin]:

```bash
pip install maturin            # or: uvx maturin build
maturin develop --release      # installs into the active venv (editable)
```

or build a wheel:

```bash
maturin build --release
pip install target/wheels/fina_core-*.whl
```

[PyPI]: https://pypi.org/project/fina-core
[maturin]: https://github.com/PyO3/maturin

---

## Quick start

```python
import time
import fina_core

# scheduler core -------------------------------------------------------------
# no hook = AutoFinishHook: each task finishes as soon as its on_start returns.
s = fina_core.Scheduler(workers=4)
s.cmd({"cmd": "restore", "tasks": [
    {"id": "t1", "priority": 1, "info": {}},
    {"id": "t2", "priority": 2, "info": {}},
]})
time.sleep(0.05)
s.counts()   # {'pending': 0, 'running': 0, 'finished': 2, ...}
s.close()

# JSON codecs ----------------------------------------------------------------
fina_core.loads(b'{"a":1,"b":[true,null,"x"]}')
# {'a': 1, 'b': [True, None, 'x']}
fina_core.dumps({"k": [1, 2.5, True]})
# b'{"k":[1,2.5,true]}'

# the ETL task type (one built-in task kind) --------------------------------
result = fina_core.run_pipelines("examples/demo/pipelines.yml")
result.rows        # {'spot': 12, 'products': 1200, 'fx_pairs': 2}
result.breakdown() # "pipeline 'mktDataETL' dataset 'spot'=1.3 ms  ..."
result.total_etl_ms()

# or as a scheduled dependency graph (serial stages + fan-out) --------------
result = fina_core.run_pipelines_scheduled(
    "examples/scheduler/pipelines.yml", workers=10, retries=2)
```

The demo runs three pipelines: `mktDataETL` loads a spot reference table into the
shared in-memory duckdb store (`memory://spot`); `prodETL` unwinds one row per
underlying, enriches each row with the live spot via a LEFT JOIN, and writes a
Hive-partitioned parquet dataset; `fxCartesianETL` shows
`cartesian_product(...)` producing an instrument's FX-pair array
(`["HKDUSD","SGDUSD"]` for instrument `USD` with underlyings `HKD, USD, SGD`,
the equal `USDUSD` pair filtered out):

```
examples/demo/
  out/products/currency=USD/data.parquet
  out/products/currency=EUR/data.parquet
  out/products/currency=GBP/data.parquet
```

You can pass the config as a **`PipelinesConfig` object**, a plain **`dict`**, a
**YAML file path**, or a **YAML string**:

```python
fina_core.run_pipelines({"pipelines": [{"name": "x", "datasets": [...]}]})
```

---

## The ETL task: YAML schema

See **[`docs/schema.md`](docs/schema.md)** for the full reference and
**[`schema/etl.schema.json`](schema/etl.schema.json)** for the machine-readable
schema. A minimal pipeline:

```yaml
pipelines:
  - name: demo
    sources:
      - {name: instruments, uri: file://instruments.json, format: json}
    datasets:
      - name: instrument_master
        type: master
        source: instruments
        to: {uri: file://out}
        fields:
          - {name: instrument_id, expression: "coalesce($.instrumentName, $.instrument.name)"}
          - {name: notional, expression: "cast(coalesce($.notional.$numberDecimal, '0') as double)"}
          - {name: family, expression: "coalesce($.instrument.classification.family, 'EQD')"}
      - name: instrument_unwound
        type: unwound
        source: instruments
        to: {uri: file://out}
        unwind_rules:
          - {name: unwind_underlyings, condition: "$.instrumentName CONTAINS 'FCN'",
             unwind_path: "$.KIKOSelect.underlying", output_alias: "symbol"}
        fields:
          - {name: instrument_id, expression: "coalesce($.instrumentName, $.instrument.name)"}
          - {name: symbol, expression: "$.symbol"}
```

### Store URIs

| scheme                              | as input               | as output                          |
|-------------------------------------|------------------------|------------------------------------|
| `file:///path` / bare path          | JSON (or parquet)      | parquet file / partition directory |
| `memory://<table>`                  | existing in-memory table | in-memory duckdb table           |
| `duckdb://<file>?table=<t>`         | existing duckdb table  | duckdb-file table                  |

`memory://` tables are shared across the whole `run_pipelines` call, so earlier
pipelines can feed later ones (joins included).

### Dataset types

| type       | row semantics                                        |
|------------|------------------------------------------------------|
| `raw`      | one output row per input record                      |
| `master`   | one output row per input record (canonical view)     |
| `unwound`  | zero-or-more rows per record, driven by `unwind_rules` |

### Joins

A dataset may join against any table produced earlier in the call:

```yaml
join:
  alias: mkt
  target: memory://spot      # store URI of the table to join
  left_key: "$.u"            # this dataset's key, evaluated per row
  right_key: "name"          # target table's key column
  columns: [spot]            # right columns exposed as mkt.spot
```

`LEFT JOIN`s run inside duckdb (the temp dataset is materialized to a
`memory://` table and joined with `ORDER BY` preserving row order).

### Field expression grammar

| form                                   | meaning                                    |
|----------------------------------------|--------------------------------------------|
| `$`                                    | whole record (as raw JSON text)            |
| `$.a.b[0].c`                           | JSON-path addressing                       |
| `$.a[].c`                              | `[]` wildcard — fan out over array elements |
| `$alias`                               | unwind alias (e.g. `$.symbol`)             |
| `alias.col` / `cast(alias.col as TYPE)`| joined-column access                       |
| `coalesce(a, b, 'DEF')`                | first non-null                             |
| `cast(x as double\|integer\|string)`   | typed coercion                             |
| `to_json_string(x)`                    | re-serialize a sub-node to JSON string     |
| `cartesian_product(a, b[, 'l != r'])`  | cross-product pairs (see below)            |
| `'lit'` / `true` / `false` / `null` / `42` / `3.14` | literals                       |

`cartesian_product(a, b[, 'l != r'])` cross-products two (array) operands and
returns the pairs as a JSON-array string (e.g. `["HKDUSD","SGDUSD"]`), each pair
being the concatenation of one element of `a` and one of `b`. A scalar operand
is treated as a single element. The optional quoted filter references the pair
as `l` / `r`: `'l != r'` drops equal pairs (e.g. when an underlying's currency
equals the instrument currency):

```yaml
- {name: fx_pairs, expression: "cartesian_product($.underlyings[].currency, $.currency, 'l != r')"}
```

Column Parquet types are inferred from `cast ... as TYPE` (else string/bool).

---

## The scheduler core

This is the foundation FinA is built on. It is intentionally shaped like an OS
process scheduler (hence "financial **agent**": the agent's task engine), and
task kinds such as ETL are layered on top of it.

Full reference: [`docs/scheduler.md`](docs/scheduler.md).

### Task model

- **Tasks** carry `id`, `priority`, `info`, `max_attempts`, and state
  (`pending → running → paused / finished / killed`), plus a `checkpoint` and a
  `result`.
- **Workers** (`Scheduler(workers=N)`) bound how many tasks run concurrently;
  the priority queue picks the highest-`priority` task (FIFO tie-break).
- **Retries** — `reschedule` re-queues a task with a delay; `max_attempts` moves
  it to `killed` when exhausted.
- **Pause / resume** — a running task can be paused (its `on_pause` hook fires),
  then resumed to finish.
- **Hooks** — a Python `TaskHook` subclass receives `on_start` / `on_finish` /
  `on_pause` / `on_resume` / `on_kill` / `on_reschedule`; the scheduler gives it
  no notifier handle, so a hook completes a task via `scheduler.cmd(...)`.
- **Shared snapshot** — Python reads the durable task list and per-state counts
  through one lock-free snapshot (`Scheduler.query()` / `Scheduler.counts()`),
  maintained incrementally (O(1) per task event).

### Wire commands

The scheduler is driven by JSON commands through one handle — `start`, `restore`,
`finish`, `kill`, `reschedule`, `pause`, `resume`, `checkpoint`, `update`,
`set_slots` — see `SchedCmd` in `docs/scheduler.md`. Example:

```python
s = fina_core.Scheduler(workers=4)          # no hook -> AutoFinishHook
s.cmd({"cmd": "restore", "tasks": [{"id": "t1", "priority": 1, "info": {}}]})
s.counts()
```

### Scheduled pipelines (ETL as a task graph)

`run_pipelines_scheduled(yaml, workers=…)` expands an ETL config into a task
plan — serial stages, then a fan-out stage where one scheduler task handles each
partition — and runs it on the scheduler core with retries. See
[`examples/scheduler/pipelines.yml`](examples/scheduler/pipelines.yml) and
[`docs/scheduler.md`](docs/scheduler.md).

---

## Programmatic configuration

```python
import fina_core

cfg = fina_core.PipelinesConfig([
    fina_core.Pipeline(
        name="mktDataETL",
        sources=[fina_core.Source("spot", "file://spot.json")],
        datasets=[
            fina_core.Dataset("spot", "raw", to=fina_core.Output("memory://spot"),
                             fields=[fina_core.Field("name", "$._id")]),
        ],
    ),
    fina_core.Pipeline(
        name="prodETL",
        sources=[fina_core.Source("uni", "file://uni.json")],
        datasets=[
            fina_core.Dataset(
                "products", "unwound", source="uni",
                to=fina_core.Output("file://out/products", partition_by=["currency"]),
                unwind_rules=[fina_core.UnwindRule("u", "$.underlyings[0]", "$.underlyings", "u")],
                join=fina_core.Join("mkt", "memory://spot", "$.u", "name", ["spot"]),
                fields=[fina_core.Field("spot", "cast(mkt.spot as double)")],
            ),
        ],
    ),
])
yml = cfg.to_yaml()             # -> official ETL YAML string
fina_core.run_pipelines(cfg)     # or pass the dict / YAML / path
```

---

## Examples

- [`examples/demo.py`](examples/demo.py) — JSON codecs + programmatic config +
  a whole-ETL run with output inspection.
- [`examples/scheduler/run.py`](examples/scheduler/run.py) — the scheduled ETL
  example (market → `memory://`, instruments unwind+join, 10-way fan-out with
  per-worker parquet) through `run_pipelines_scheduled`.
- [`examples/bench_3gb.py`](examples/bench_3gb.py) — benchmark the ETL task type
  on a multi-GB corpus (defaults to the sample data in `examples/demo/`).
- [`examples/scheduler/bench.py`](examples/scheduler/bench.py) — benchmark the
  scheduler core's maximum task throughput over a 60-second window.

```bash
cd fina
python examples/demo.py
python examples/scheduler/run.py
python examples/bench_3gb.py --config examples/demo/pipelines.yml --input examples/demo/uni.json --out /tmp/pq
python examples/scheduler/bench.py --duration 60
```

---

## Benchmarking

Benchmark methodology, known bottlenecks and latest numbers live in
[`BENCHMARK.md`](BENCHMARK.md):

- **Scheduler core** — maximum task throughput in one minute
  (`examples/scheduler/bench.py`, three modes) plus the native kernel
  reference run (`cargo test --release -- --ignored --nocapture
  kernel_throughput`);
- **ETL task type** — 3 GB / 150k-record single-core run (per-dataset timing,
  RSS, the known visibility-regression notes).

---

## Project layout

```
fina/
  Cargo.toml            Rust crate (cdylib, PyO3) — deps: pyo3, sonic-rs,
                        parquet/arrow (write only), duckdb (bundled), serde_yaml,
                        actix/actix-rt, tokio
  pyproject.toml        maturin build, package "fina-core"
  src/
    lib.rs              PyO3 bindings: scheduler_*, run_pipelines, loads, dumps
    scheduler.rs        the scheduler core: actix kernel (queue, states, hooks,
                        PyHook, durable snapshot)
    etl_sched.rs        ETL task type: ETL→scheduler expansion + run_scheduled,
                        EtlHook
    config.rs           ETL task YAML schema (serde structs)
    native.rs           NValue accessor trait (no DOM)
    lazy.rs             expression compiler + evaluator
    plazy.rs            streaming per-record runner (pipelines / datasets)
    columnar.rs         typed columnar Parquet sink (+ Hive partitioning)
    store.rs            store URIs (file/memory/duckdb) + duckdb join/tables
    sonic.rs            sonic-rs implementation of NValue
  python/fina_core/  pure-Python public API (__init__.py, scheduler.py)
  examples/             demo + benchmark scripts (ETL task, scheduler core)
  docs/scheduler.md     scheduler core reference
  docs/schema.md        ETL task YAML reference
  schema/etl.schema.json  machine-readable JSON Schema (ETL task)
  BENCHMARK.md          benchmark methodology + results
```

## Development

```bash
cargo build --release          # build the Rust core (tests: cargo test)
maturin develop --release      # build + install the Python extension
python examples/demo.py        # smoke test
python examples/scheduler/run.py   # scheduled ETL on the scheduler core
python examples/bench_3gb.py --config examples/demo/pipelines.yml --input examples/demo/uni.json --out /tmp/pq
```

`cargo test` runs the core unit tests (expression compiler / evaluator round
trips, scheduler lifecycle, ETL→scheduler end-to-end). The Python API is verified
end-to-end against the standalone CLI to produce byte-identical Parquet output.

---

## Cross-platform support (manylinux / macOS / Windows)

The extension is `abi3` (`abi3-py39`), so a single wheel built for a given
platform works across Python ≥ 3.9 on that platform. Because the ETL task whips
up Parquet/Arrow (`arrow-*`, `parquet`) and a bundled duckdb, building from
source needs a Rust toolchain (plus a C/C++ compiler for duckdb), but **published
wheels are prebuilt so end users need nothing**.

| platform        | wheels you publish                     | notes                                   |
|-----------------|----------------------------------------|-----------------------------------------|
| Linux           | `manylinux_2_38_x86_64` (+ aarch64)    | built in the `manylinux` container      |
| macOS (Apple)   | `macosx_*_x86_64`, `macosx_*_arm64`    | universal2 or per-arch                  |
| Windows         | `win_amd64` (+ win_arm64)              | built on Windows runners                |

Key points:

- **abi3** (`abi3-py39`) means one wheel per (platform, arch) — no per-Python-version
  matrix.
- The Linux wheel is tagged `manylinux_2_38` because the bundled
  `parquet`/`arrow`/`duckdb` stack needs a modern glibc. Build with the official
  `ghcr.io/pyo3/maturin build --release --target x86_64-unknown-linux-gnu`
  inside the manylinux image, or the PyO3 Docker images.
- On macOS set `RUSTFLAGS` as needed for a universal2 build, or just rely on the
  CI matrix producing separate `x86_64` and `arm64` wheels.
- No OS-specific code in `src/`; `duckdb` (bundled) is cross-platform.
  Windows/macOS builds need no code changes.

A convenience `sdist` (`python -m build --sdist`) is always published so users
on unsupported platforms can build from source (requires Rust + maturin).

---

## Publishing to PyPI

Wheels are built by [maturin] and uploaded with [twine]. Publish a wheel for
every platform you support (see the table above); PyPI will serve the correct
one per install.

1. **Release version** — bump in `Cargo.toml` and `pyproject.toml`; keep them
   in sync (they must match).

   ```bash
   cd fina
   # e.g. bump both files to 0.2.0
   ```

2. **Build wheels + sdist**:

   ```bash
   pip install maturin twine build

   # local platform (e.g. linux on this machine)
   maturin build --release

   # aarch64 linux (manylinux) — from a manylinux-based builder:
   maturin build --release --target aarch64-unknown-linux-gnu

   # source distribution for everyone else:
   python -m build --sdist
   ```

3. **Verify** what you are about to upload:

   ```bash
   maturin list
   # or
   ls target/wheels/
   ```

4. **Upload to TestPyPI first** (recommended):

   ```bash
   twine upload --repository testpypi target/wheels/*.whl dist/*.tar.gz
   ```

5. **Upload to PyPI**:

   ```bash
   twine upload target/wheels/*.whl dist/*.tar.gz
   ```

6. **Tag the release** in git:

   ```bash
   git tag 0.2.0 && git push origin 0.2.0
   ```

> Use a PyPI API token (`~/.pypirc`) rather than a password. Automate steps 2–5
> with GitHub Actions (`actions/setup-python`, `PyO3/maturin-action`, and an
> `upload-pypi` step) or the equivalent CI on your forge.

---

## License

[MIT](LICENSE)
