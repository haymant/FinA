# sonicetl ETL schema (official)

sonicetl consumes an **ETL YAML document** that describes one or more output
datasets derived from a single JSON input document. Every ETL run:

1. **reads** the input JSON record-by-record (streaming, never a full DOM),
2. **parses** each record natively with **sonic-rs**,
3. **evaluates** each declared field expression directly against the native
   `sonic_rs::Value` (no `serde_json::Value` is ever built),
4. **streams** the extracted, typed columns into one Parquet file per dataset
   (`<out_dir>/<dataset>.parquet`).

A JSON Schema mirror is provided at
[`schema/etl.schema.json`](../schema/etl.schema.json).

## Top-level structure

```yaml
pipeline_name: derivative_instruments_normalization
source:
  file_path: examples/instruments_sample.json   # input JSON (array of records)
  json_path: "item"                             # optional root path (informational)
datasets: [ ... ]                                # >= 1 dataset
output:
  format: parquet
  output_directory: dist/normalized
  partition_by: [symbol, prodType]
```

| key               | type     | required | meaning                                   |
|-------------------|----------|----------|-------------------------------------------|
| `pipeline_name`   | `string` | no       | human-readable pipeline name              |
| `source.file_path`| `string` | **yes**  | path to the input JSON                    |
| `source.json_path`| `string` | no       | optional root path to records (metadata)  |
| `datasets`        | `list`   | **yes**  | one or more datasets                      |
| `output`          | `object` | no       | format / output dir / partition hints     |

## Dataset

| key            | type   | required | meaning                                                            |
|----------------|--------|----------|--------------------------------------------------------------------|
| `name`         | `str`  | **yes** | output name → `<name>.parquet`                                      |
| `type`         | `str`  | **yes** | `raw`, `master`, or `unwound` (row semantics below)                 |
| `fields`       | `list` | no      | output columns (`name` + `expression`)                              |
| `unwind_rules` | `list` | no      | only meaningful for `type: unwound`                                 |

### `type` semantics

* **`raw`** — one output row per input record.
* **`master`** — one output row per input record (the canonical / normalized
  view; identical per-record semantics to `raw`, kept as a distinct dataset).
* **`unwound`** — zero or more output rows per input record. For each matching
  `unwind_rule` the array at `unwind_path` is "unwound" into one row per element.

## Field expression grammar

A `fields[].expression` is a small composition language evaluated natively:

| form                     | meaning                                                    |
|--------------------------|------------------------------------------------------------|
| `$`                      | the whole record (as its raw JSON text)                    |
| `$.a.b[0].c`             | JSON-path addressing (`.` keys, `[i]` indices)             |
| `$alias`                 | an unwind alias (e.g. `$.symbol`) for `unwound` datasets   |
| `coalesce(a, b, 'DEF')`  | first non-null (short-circuit)                             |
| `cast(x as double)`      | coerce to `double` / `integer` / `string`                  |
| `to_json_string(x)`      | re-serialize a sub-node to a JSON string                   |
| `'literal'` / `true` / `false` / `null` / `42` / `3.14` | literals |

The column's Parquet type is inferred from the expression's `cast ... as TYPE`
if present, else from boolean `true`/`false` literals, otherwise `string`:

| cast target        | Parquet type |
|--------------------|--------------|
| `as double`        | `DOUBLE`     |
| `as integer`       | `INT64`      |
| `as string`        | `UTF8`       |
| (string/bool)      | `UTF8` / `BOOLEAN` |

## Unwind rule

| key            | type   | example                                        | meaning                                     |
|----------------|--------|------------------------------------------------|---------------------------------------------|
| `name`         | `str`  | `unwind_fcn_underlyings`                       | rule identifier                             |
| `condition`    | `str`  | `$.instrumentName CONTAINS 'FCN'`              | only unwinds when true                      |
| `unwind_path`  | `str`  | `$.KIKOSelect.underlying`                      | JSON path to the array to unwind            |
| `output_alias` | `str`  | `symbol`                                       | name exposed to field expressions as `$.symbol` |

Condition grammar: `PATH CONTAINS 'TXT'`, `PATH = VALUE`, or a bare literal
(truthiness).

## Example

```yaml
pipeline_name: demo
source: { file_path: "instruments.json" }
datasets:
  - name: instrument_master
    type: master
    fields:
      - { name: instrument_id, expression: "coalesce($._id.$oid, $.instrumentName, $.instrument.name)" }
      - { name: notional, expression: "cast(coalesce($.notional.$numberDecimal, '0') as double)" }
      - { name: family, expression: "coalesce($.instrument.classification.family, 'EQD')" }
  - name: instrument_unwound
    type: unwound
    unwind_rules:
      - name: unwind_underlyings
        condition: "$.instrumentName CONTAINS 'FCN'"
        unwind_path: "$.KIKOSelect.underlying"
        output_alias: "symbol"
    fields:
      - { name: instrument_id, expression: "coalesce($._id.$oid, $.instrumentName, $.instrument.name)" }
      - { name: symbol, expression: "$.symbol" }
```

## Programmatic construction

The Python surface exposes the schema as dataclasses, so you can build a config
without hand-writing YAML:

```python
import sonicetl

cfg = sonicetl.ETLConfig(
    pipeline_name="demo",
    source_file_path="instruments.json",
    datasets=[
        sonicetl.Dataset("instrument_master", "master", [
            sonicetl.FieldSpec("instrument_id", "coalesce($._id.$oid, $.instrumentName)"),
            sonicetl.FieldSpec("notional", "cast($.notional.$numberDecimal as double)"),
        ]),
        sonicetl.Dataset("instrument_unwound", "unwound",
            fields=[sonicetl.FieldSpec("symbol", "$.symbol")],
            unwind_rules=[
                sonicetl.UnwindRule("u1", "$.instrumentName CONTAINS 'FCN'",
                                    "$.KIKOSelect.underlying", "symbol"),
            ],
        ),
    ],
)
yml = cfg.to_yaml()
```

You can also pass the dataclass config (or a plain `dict`, or a YAML file path)
directly to `sonicetl.run(config, input, out_dir)`.
