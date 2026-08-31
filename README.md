# sonicetl

**Native whole-ETL for Python, backed by a Rust core built on [sonic-rs].**

The heavy lifting happens in a compiled Rust extension (PyO3) while Python sees
a small, clean API. It runs a *whole ETL* (read JSON → parse each record
natively → evaluate YAML field expressions directly against the native
`sonic_rs::Value` → stream typed columns to Parquet or a duckdb table) **without
ever materializing a `serde_json::Value` DOM**.

```
JSON input ──► (streaming) ──► sonic-rs parse per record ──► native expression
eval ──► typed columns ──► parquet file / memory:// table / duckdb:// table
```

The same engine that was benchmarked here as a CLI is now behind a Python
library.

[sonic-rs]: https://github.com/cloudwego/sonic-rs

---

## Features

- **JSON codecs** — `loads` / `dumps` powered by sonic-rs.
- **Whole-ETL pipelines** — `run_pipelines(config)` runs one or more pipelines,
  each with named sources and datasets, in a single native call.
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
pip install sonicetl
# or, with uv:
uv add sonicetl
```

Wheels are provided per platform/arch (Linux `manylinux`, macOS `x86_64` and
`arm64`, Windows `amd64`); `pip`/`uv` pick the right one automatically.

To install a specific version or into an existing env:

```bash
pip install "sonicetl==0.1.0"
uv pip install sonicetl --python 3.12
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
pip install target/wheels/sonicetl-*.whl
```

[PyPI]: https://pypi.org/project/sonicetl
[maturin]: https://github.com/PyO3/maturin

---

## Quick start

```python
import sonicetl

# JSON codecs ----------------------------------------------------------------
sonicetl.loads(b'{"a":1,"b":[true,null,"x"]}')
# {'a': 1, 'b': [True, None, 'x']}
sonicetl.dumps({"k": [1, 2.5, True]})
# b'{"k":[1,2.5,true]}'

# whole ETL (pipelines) -----------------------------------------------------
result = sonicetl.run_pipelines("examples/demo/pipelines.yml")
result.rows        # {'spot': 12, 'products': 1200}
result.breakdown() # "pipeline 'mktDataETL' dataset 'spot'=1.3 ms  ..."
result.total_etl_ms()
```

The demo runs two pipelines: `mktDataETL` loads a spot reference table into the
shared in-memory duckdb store (`memory://spot`), and `prodETL` unwinds one row
per underlying, enriches each row with the live spot via a LEFT JOIN, and writes
a Hive-partitioned parquet dataset:

```
examples/demo/
  out/products/currency=USD/data.parquet
  out/products/currency=EUR/data.parquet
  out/products/currency=GBP/data.parquet
```

You can pass the config as a **`PipelinesConfig` object**, a plain **`dict`**, a
**YAML file path**, or a **YAML string**:

```python
sonicetl.run_pipelines({"pipelines": [{"name": "x", "datasets": [...]}]})
```

---

## The ETL YAML schema

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
| `$alias`                               | unwind alias (e.g. `$.symbol`)             |
| `alias.col` / `cast(alias.col as TYPE)`| joined-column access                       |
| `coalesce(a, b, 'DEF')`                | first non-null                             |
| `cast(x as double\|integer\|string)`   | typed coercion                             |
| `to_json_string(x)`                    | re-serialize a sub-node to JSON string     |
| `'lit'` / `true` / `false` / `null` / `42` / `3.14` | literals                       |

Column Parquet types are inferred from `cast ... as TYPE` (else string/bool).

---

## Programmatic configuration

```python
import sonicetl

cfg = sonicetl.PipelinesConfig([
    sonicetl.Pipeline(
        name="mktDataETL",
        sources=[sonicetl.Source("spot", "file://spot.json")],
        datasets=[
            sonicetl.Dataset("spot", "raw", to=sonicetl.Output("memory://spot"),
                             fields=[sonicetl.Field("name", "$._id")]),
        ],
    ),
    sonicetl.Pipeline(
        name="prodETL",
        sources=[sonicetl.Source("uni", "file://uni.json")],
        datasets=[
            sonicetl.Dataset(
                "products", "unwound", source="uni",
                to=sonicetl.Output("file://out/products", partition_by=["currency"]),
                unwind_rules=[sonicetl.UnwindRule("u", "$.underlyings[0]", "$.underlyings", "u")],
                join=sonicetl.Join("mkt", "memory://spot", "$.u", "name", ["spot"]),
                fields=[sonicetl.Field("spot", "cast(mkt.spot as double)")],
            ),
        ],
    ),
])
yml = cfg.to_yaml()             # -> official ETL YAML string
sonicetl.run_pipelines(cfg)     # or pass the dict / YAML / path
```

---

## Examples

- [`examples/demo.py`](examples/demo.py) — codecs + programmatic config +
  whole-pipelines run with output inspection.
- [`examples/bench_3gb.py`](examples/bench_3gb.py) — benchmark a whole ETL
  (defaults to the sample corpus in `examples/demo/`).

```bash
cd sonicetl
python examples/demo.py
python examples/bench_3gb.py --config examples/demo/pipelines.yml --input examples/demo/uni.json --out /tmp/pq
```

---

## Benchmark (3 GB / 150k records, single-core)

These figures come from a 3 GB stress corpus (150k records, the same ETL
expressions as [`data/pipelines.yml`](../data/pipelines.yml), scaled up). The
big input is kept out of the repo by size; reproduce via the `rust_simdetl`
harness or a large generated `uni.json`. `disk read` (~2 s) is excluded from
the ETL total because it is identical for every path.

### Whole ETL — `sonicetl.run_pipelines` (native sonic-rs, streaming, no DOM)

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
   non-partitioned `file://` targets (the common case) open a
   [`ParquetSink`](src/columnar.rs) per dataset and flush row-groups every
   `CHUNK` rows, clearing the columns. This alone should bring RSS back near the
   old ~6.7 GB. Partitioned / `memory://` / `duckdb://` / joined targets still
   need full materialization (partition layout / table insert / join temp table).
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

## Project layout

```
sonicetl/
  Cargo.toml            Rust crate (cdylib, PyO3) — deps: pyo3, sonic-rs,
                        parquet/arrow (write only), duckdb (bundled), serde_yaml
  pyproject.toml        maturin build, package "sonicetl"
  src/
    lib.rs              PyO3 bindings: run_pipelines, loads, dumps
    config.rs           official ETL YAML schema (serde structs)
    native.rs           NValue accessor trait (no DOM)
    lazy.rs             expression compiler + evaluator
    plazy.rs            streaming per-record runner (pipelines / datasets)
    columnar.rs         typed columnar Parquet sink (+ Hive partitioning)
    store.rs            store URIs (file/memory/duckdb) + duckdb join/tables
    sonic.rs            sonic-rs implementation of NValue
  python/sonicetl/      pure-Python public API (__init__.py)
  examples/             demo + 3 GB benchmark scripts
  docs/schema.md        official ETL YAML reference
  schema/etl.schema.json  machine-readable JSON Schema
```

## Development

```bash
cargo build --release          # build the Rust core (tests: cargo test)
maturin develop --release      # build + install the Python extension
python examples/demo.py        # smoke test
python examples/bench_3gb.py --config examples/demo/pipelines.yml --input examples/demo/uni.json --out /tmp/pq
```

`cargo test` runs the core unit tests (expression compiler / evaluator round
trips). The Python API is verified end-to-end against the standalone CLI to
produce byte-identical Parquet output.

---

## Cross-platform support (manylinux / macOS / Windows)

The extension is `abi3` (`abi3-py39`), so a single wheel built for a given
platform works across Python ≥ 3.9 on that platform. Because the ETL whips up
Parquet/Arrow (`arrow-*`, `parquet`) and a bundled duckdb, building from source
needs a Rust toolchain (plus a C/C++ compiler for duckdb), but **published
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
   cd sonicetl
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
   git tag v0.2.0 && git push origin v0.2.0
   ```

> Use a PyPI API token (`~/.pypirc`) rather than a password. Automate steps 2–5
> with GitHub Actions (`actions/setup-python`, `PyO3/maturin-action`, and an
> `upload-pypi` step) or the equivalent CI on your forge.

---

## License

[MIT](LICENSE)
