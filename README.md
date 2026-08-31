# sonicetl

**Native whole-ETL for Python, backed by a Rust core built on [sonic-rs].**

The heavy lifting happens in a compiled Rust extension (PyO3) while Python sees
a small, clean API. It runs a *whole ETL* (read JSON → parse each record
natively → evaluate YAML field expressions directly against the native
`sonic_rs::Value` → stream typed columns to Parquet) **without ever
materializing a `serde_json::Value` DOM**.

```
JSON input ──► (streaming) ──► sonic-rs parse per record ──► native expression
eval ──► typed columns ──► <dataset>.parquet
```

The same engine that was benchmarked here as a CLI is now behind a Python
library.

[sonic-rs]: https://github.com/cloudwego/sonic-rs

---

## Features

- **JSON codecs** — `loads` / `dumps` powered by sonic-rs.
- **Whole-ETL** — `run(config, input, out_dir)` runs parse + extract + Parquet
  write in one native pass per record.
- **No DOM** — expressions evaluate against `sonic_rs::Value` directly
  (`src/native.rs` supplies a small accessor trait; `src/sonic.rs` implements it
  for sonic-rs). Peak memory stays ~2× input, not 5–8×.
- **Official ETL YAML schema** — see [`docs/schema.md`](docs/schema.md) and
  [`schema/etl.schema.json`](schema/etl.schema.json). Build configs
  programmatically with `ETLConfig` / `Dataset` / `FieldSpec` / `UnwindRule`.
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

# whole ETL -----------------------------------------------------------------
result = sonicetl.run("examples/demo/etl.yml", "examples/demo/uni.json", out_dir="out")
result.rows        # {'instrument_raw': 300, 'instrument_master': 300, 'instrument_unwound': 800}
result.breakdown() # 'parse=0.2 ms  extract=1.4 ms  write=0.9 ms'
result.records     # 300
```

After a run, `out_dir/` contains one Parquet file per dataset:
`out/instrument_master.parquet`, `out/instrument_raw.parquet`, etc.

You can pass the config as an **`ETLConfig` object**, a plain **`dict`**, a
**YAML file path**, or a **YAML string**:

```python
sonicetl.run({"pipeline_name": "x", "source": {"file_path": "i.json"},
              "datasets": [...]}, input_bytes, out_dir="out")
```

---

## The ETL YAML schema

See **[`docs/schema.md`](docs/schema.md)** for the full reference and
**[`schema/etl.schema.json`](schema/etl.schema.json)** for the machine-readable
schema. A minimal pipeline:

```yaml
pipeline_name: demo
source:
  file_path: instruments.json
datasets:
  - name: instrument_master
    type: master
    fields:
      - {name: instrument_id, expression: "coalesce($.instrumentName, $.instrument.name)"}
      - {name: notional, expression: "cast(coalesce($.notional.$numberDecimal, '0') as double)"}
      - {name: family, expression: "coalesce($.instrument.classification.family, 'EQD')"}
  - name: instrument_unwound
    type: unwound
    unwind_rules:
      - {name: unwind_underlyings, condition: "$.instrumentName CONTAINS 'FCN'",
         unwind_path: "$.KIKOSelect.underlying", output_alias: "symbol"}
    fields:
      - {name: instrument_id, expression: "coalesce($.instrumentName, $.instrument.name)"}
      - {name: symbol, expression: "$.symbol"}
```

### Dataset types

| type       | row semantics                                        |
|------------|------------------------------------------------------|
| `raw`      | one output row per input record                      |
| `master`   | one output row per input record (canonical view)     |
| `unwound`  | zero-or-more rows per record, driven by `unwind_rules` |

### Field expression grammar

| form                                   | meaning                                    |
|----------------------------------------|--------------------------------------------|
| `$`                                    | whole record (as raw JSON text)            |
| `$.a.b[0].c`                           | JSON-path addressing                       |
| `$alias`                               | unwind alias (e.g. `$.symbol`)             |
| `coalesce(a, b, 'DEF')`                | first non-null                             |
| `cast(x as double\|integer\|string)`   | typed coercion                             |
| `to_json_string(x)`                    | re-serialize a sub-node to JSON string     |
| `'lit'` / `true` / `false` / `null` / `42` / `3.14` | literals                       |

Column Parquet types are inferred from `cast ... as TYPE` (else string/bool).

---

## Programmatic configuration

```python
import sonicetl

cfg = sonicetl.ETLConfig(
    pipeline_name="demo",
    source_file_path="instruments.json",
    datasets=[
        sonicetl.Dataset("instrument_master", "master", [
            sonicetl.FieldSpec("instrument_id", "coalesce($._id.$oid, $.instrumentName)"),
        ]),
        sonicetl.Dataset("instrument_unwound", "unwound",
            fields=[sonicetl.FieldSpec("symbol", "$.symbol")],
            unwind_rules=[
                sonicetl.UnwindRule("u1", "$.instrumentName CONTAINS 'FCN'",
                                    "$.KIKOSelect.underlying", "symbol"),
            ]),
    ],
)
yml = cfg.to_yaml()     # -> official ETL YAML string
sonicetl.run(cfg, input_bytes, out_dir="out")
```

---

## Examples

- [`examples/demo.py`](examples/demo.py) — codecs + programmatic config +
  whole-ETL run with output inspection.
- [`examples/bench_3gb.py`](examples/bench_3gb.py) — benchmark a whole ETL
  (defaults to the sample corpus in `examples/demo/`).

```bash
cd sonicetl
python examples/demo.py
python examples/bench_3gb.py --config examples/demo/etl.yml --input examples/demo/uni.json --out /tmp/pq
```

---

## Benchmark (3 GB / 150k records, single-core)

These figures come from a 3 GB stress corpus (150k records, the same ETL
expressions as [`examples/demo/etl.yml`](examples/demo/etl.yml), scaled up).
The big input is kept out of the repo by size; reproduce via the `rust_simdetl`
harness or a large generated `uni.json`. `disk read` (~2 s) is excluded from
the ETL total because it is identical for every path.

### Whole ETL — `sonicetl.run` (native sonic-rs, streaming, no DOM)

| stage  | time      |
|--------|-----------|
| parse  |  2,019 ms |
| extract|  2,607 ms |
| write  |  5,707 ms |
| **ETL total** | **10,332 ms** |
| wall clock (incl. read + allocator warm-up) | 15,604 ms |
| process peak RSS | ~6.7 GB |
| rows | 150k raw / 150k master / 420,262 unwound |

> The engine streams records one at a time and never builds a `serde_json`/DOM
> representation, which is what keeps memory at ~6.7 GB for a 3 GB input and
> avoids the multi-GB DOM that a naive whole-file parse would allocate.
> (`loads`/`dumps`, when called explicitly, do materialize a DOM for Python
> interop; the ETL path itself avoids them.)
>
> Notes: single-core; 12-core box.

---

## Project layout

```
sonicetl/
  Cargo.toml            Rust crate (cdylib, PyO3) — deps: pyo3, sonic-rs,
                        parquet/arrow (write only), serde_yaml, mimalloc
  pyproject.toml        maturin build, package "sonicetl"
  src/
    lib.rs              PyO3 bindings: run_etl, loads, dumps
    config.rs           official ETL YAML schema (serde structs)
    native.rs           NValue accessor trait (no DOM)
    lazy.rs             expression compiler + evaluator
    plazy.rs            streaming per-record runner
    columnar.rs         typed columnar Parquet sink
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
python examples/bench_3gb.py --config examples/demo/etl.yml --input examples/demo/uni.json --out /tmp/pq
```

`cargo test` runs the core unit tests (expression compiler / evaluator round
trips). The Python API is verified end-to-end against the standalone CLI to
produce byte-identical Parquet output.

---

## Cross-platform support (manylinux / macOS / Windows)

The extension is `abi3` (`abi3-py39`), so a single wheel built for a given
platform works across Python ≥ 3.9 on that platform. Because the ETL whips up
Parquet/Arrow (`arrow-*`, `parquet`) and the allocator (`mimalloc`), building
from source needs a Rust toolchain, but **published wheels are prebuilt so end
users need nothing**.

| platform        | wheels you publish                     | notes                                   |
|-----------------|----------------------------------------|-----------------------------------------|
| Linux           | `manylinux_2_34_x86_64` (+ aarch64)    | built in the `manylinux` container      |
| macOS (Apple)   | `macosx_*_x86_64`, `macosx_*_arm64`    | universal2 or per-arch                  |
| Windows         | `win_amd64` (+ win_arm64)              | built on Windows runners                |

Key points:

- **abi3** (`abi3-py39`) means one wheel per (platform, arch) — no per-Python-version
  matrix.
- `manylinux_2_34` is required because newer `parquet`/`arrow` pulls a glibc
  baseline of 2.34 (mimalloc/musl are unrelated). Build with the official
  `ghcr.io/pyo3/maturin build --release --target x86_64-unknown-linux-gnu`
  inside the manylinux image, or the PyO3 Docker images.
- On macOS set `RUSTFLAGS` as needed for a universal2 build, or just rely on the
  CI matrix producing separate `x86_64` and `arm64` wheels.
- No OS-specific code in `src/`; `mimalloc` is cross-platform. Windows/macOS
  builds need no code changes.

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
