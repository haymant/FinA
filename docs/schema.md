# sonicetl ETL schema (official)

sonicetl consumes an **ETL YAML document** whose root is `pipelines: [ ... ]`.
Each *pipeline* declares any number of named **sources** (inputs) and
**datasets** (outputs). Every pipeline run:

1. **reads** each source record-by-record (streaming, never a full DOM),
2. **parses** each record natively with **sonic-rs**,
3. **evaluates** each declared field expression directly against the native
   `sonic_rs::Value` (no `serde_json::Value` is ever built),
4. **streams** the extracted, typed columns into a per-dataset target: a
   Parquet file/directory, an in-memory duckdb table, or a duckdb file table.

Datasets may optionally **LEFT JOIN** against a table produced earlier in the
same `run_pipelines` call (a `memory://` or `duckdb://` target).

A JSON Schema mirror is provided at
[`schema/etl.schema.json`](../schema/etl.schema.json).

## Store URIs

Every source and target is addressed by a duckdb-style **URI**:

| URI scheme          | as input (source)                          | as output (target)                        |
|---------------------|--------------------------------------------|-------------------------------------------|
| `file:///path` / bare path | a JSON document (or parquet when `format: parquet`) | a parquet file (or a partitioned directory) |
| `memory://<table>`  | an existing in-memory table from this call | an in-memory duckdb table                 |
| `duckdb://<file>?table=<t>` | an existing duckdb-file table       | a duckdb-file table                       |

The in-memory store is scoped to a single `run_pipelines` call, so tables
written by an earlier pipeline are visible to later ones (joins and
cross-pipeline references).

Hive-style partitioning is expressed on the target: with
`partition_by: [a, b]` the parquet output is written as
`<dir>/a=val1/b=val2/data.parquet` (partition columns are removed from the
data files; `NULL` partitions become `__HIVE_DEFAULT_PARTITION__`).

## Top-level structure

```yaml
pipelines:
  - name: market_and_products            # human-readable pipeline name
    sources:
      - {name: spot, uri: file://spot.json, format: json}
      - {name: uni,  uri: file://uni.json,  format: json, json_path: "records"}
    datasets: [ ... ]                     # >= 1 dataset per pipeline
```

| key            | type   | required | meaning                                    |
|----------------|--------|----------|--------------------------------------------|
| `pipelines`    | `list` | **yes**  | one or more pipelines                      |
| `pipeline.name`| `str`  | no       | human-readable pipeline name               |
| `pipeline.sources` | `list` | no   | named inputs (see below)                   |
| `pipeline.datasets` | `list` | **yes** | one or more datasets                     |

### Source

| key          | type   | required | meaning                                                     |
|--------------|--------|----------|-------------------------------------------------------------|
| `name`       | `str`  | **yes** | referenced by datasets via `dataset.source`                 |
| `uri`        | `str`  | **yes** | store URI (see above)                                       |
| `format`     | `str`  | no       | file sources only: `json` (default) or `parquet`            |
| `json_path`  | `str`  | no       | optional sub-path of a JSON document (e.g. `$.records`)     |

## Dataset

| key            | type    | required | meaning                                                            |
|----------------|---------|----------|--------------------------------------------------------------------|
| `name`         | `str`   | **yes** | output name / table name                                           |
| `type`         | `str`   | **yes** | `raw`, `master`, or `unwound` (row semantics below)                |
| `source`       | `str`   | no       | default source (a `Source.name`); `$` resolves to it               |
| `to`           | `obj`   | no       | target (see `Output`)                                              |
| `fields`       | `list`  | no       | output columns (`name` + `expression`)                             |
| `unwind_rules` | `list`  | no       | only meaningful for `type: unwound`                                |
| `join`         | `obj`   | no       | optional LEFT JOIN (see below)                                     |

### Output

| key            | type   | required | meaning                                    |
|----------------|--------|----------|--------------------------------------------|
| `uri`          | `str`  | **yes** | store URI of the target                    |
| `format`       | `str`  | no       | informational; `parquet` is the file default |
| `partition_by` | `list` | no       | Hive partition columns                     |

A file target `file://out/products` with `partition_by: [currency]` writes
`out/products/currency=EUR/data.parquet`, ... (no partition columns → a single
`<path>/<name>.parquet` file).

### `type` semantics

* **`raw`** — one output row per input record.
* **`master`** — one output row per input record (the canonical / normalized
  view; identical per-record semantics to `raw`, kept as a distinct dataset).
* **`unwound`** — zero or more output rows per input record. For each matching
  `unwind_rule` the array at `unwind_path` is "unwound" into one row per element.

### Join

| key          | type   | required | meaning                                                       |
|--------------|--------|----------|---------------------------------------------------------------|
| `alias`      | `str`  | **yes** | column prefix exposed to field expressions (e.g. `mkt`)       |
| `target`     | `str`  | **yes** | store URI of the table to join against                        |
| `left_key`   | `str`  | **yes** | this dataset's join key, evaluated per row (e.g. `$.u`)       |
| `right_key`  | `str`  | **yes** | target table's key column                                     |
| `columns`    | `list` | no       | right-table columns to expose under `alias`                   |

The join is a `LEFT JOIN` on `left_key = right_key`. Selected right columns are
read in field expressions as `alias.col` or `cast(alias.col as TYPE)`.

## Field expression grammar

A `fields[].expression` is a small composition language evaluated natively:

| form                          | meaning                                                    |
|-------------------------------|------------------------------------------------------------|
| `$`                           | the whole record (as its raw JSON text)                    |
| `$.a.b[0].c`                  | JSON-path addressing (`.` keys, `[i]` indices)             |
| `$.a[].c`                     | `[]` wildcard — fan out over every array element           |
| `$alias`                      | an unwind alias (e.g. `$.symbol`) for `unwound` datasets   |
| `source.path` / `$.path`      | bare path = this dataset's default source                  |
| `alias.col`                   | a joined column (see `join`)                               |
| `coalesce(a, b, 'DEF')`       | first non-null (short-circuit)                             |
| `cast(x as double\|integer\|string)` | typed coercion                                      |
| `to_json_string(x)`           | re-serialize a sub-node to a JSON string                   |
| `cartesian_product(a, b[, 'l != r'])` | cross-product (see below)                          |
| `'literal'` / `true` / `false` / `null` / `42` / `3.14` | literals              |

The column's Parquet type is inferred from the expression's `cast ... as TYPE`
if present, else from boolean `true`/`false` literals, otherwise `string`:

| cast target   | Parquet type |
|---------------|--------------|
| `as double`   | `DOUBLE`     |
| `as integer`  | `INT64`      |
| `as string`   | `UTF8`       |
| (string/bool) | `UTF8` / `BOOLEAN` |

### `cartesian_product(a, b[, cond])`

Cross-products two (array) operands and returns the pairs as a JSON-array string
(e.g. `["HKDUSD","SGDUSD"]`). Each pair is the concatenation of one element from
`a` and one from `b`; either operand may be a scalar (treated as a single
element) or an array. Use the `[]` wildcard to collect a field from every array
element, e.g. `$.underlyings[].currency`.

The optional third argument is a quoted filter over the pair values, referenced
as `l` (from `a`) and `r` (from `b`): `'l != r'` drops equal pairs,
`'l = r'` keeps only equal pairs, `'l CONTAINS 'HKD''` filters on a literal.

```yaml
# instrument currency = USD, underlying currencies = [HKD, USD, SGD]
#   fx_pairs     -> ["HKDUSD","SGDUSD"]      (USD==USD excluded)
#   fx_pairs_all -> ["HKDUSD","USDUSD","SGDUSD"]
fields:
  - {name: fx_pairs,     expression: "cartesian_product($.underlyings[].currency, $.currency, 'l != r')"}
  - {name: fx_pairs_all, expression: "cartesian_product($.underlyings[].currency, $.currency)"}
```

## Unwind rule

| key            | type   | example                     | meaning                                    |
|----------------|--------|-----------------------------|--------------------------------------------|
| `name`         | `str`  | `unwind_underlyings`        | rule identifier; also the alias fallback   |
| `condition`    | `str`  | `$.underlyings[0]`          | only unwinds when truthy                   |
| `unwind_path`  | `str`  | `$.underlyings`             | JSON path to the array to unwind           |
| `output_alias` | `str`  | `u`                         | exposed to expressions as `$.u` (fallback: `name`) |

Condition grammar: `PATH CONTAINS 'TXT'`, `PATH = VALUE`, or a bare expression
(truthiness; `null`/missing is falsy).

## Full example (two pipelines, cross-pipeline join)

```yaml
pipelines:
  # load a spot reference table into the shared in-memory duckdb store
  - name: mktDataETL
    sources:
      - {name: spot, uri: file://spot.json, format: json}
    datasets:
      - name: spot
        type: raw
        to: {uri: memory://spot}
        fields:
          - {name: name, expression: "$._id"}
          - {name: spot, expression: "cast($.spot as double)"}

  # unwind one row per underlying and enrich with the live spot (LEFT JOIN)
  - name: prodETL
    sources:
      - {name: uni, uri: file://uni.json, format: json}
    datasets:
      - name: products
        type: unwound
        source: uni
        to:
          uri: file://out/products
          partition_by: [currency]
        unwind_rules:
          - {name: u, condition: "$.underlyings[0]", unwind_path: "$.underlyings", output_alias: u}
        join:
          alias: mkt
          target: memory://spot
          left_key: "$.u"
          right_key: "name"
          columns: [spot]
        fields:
          - {name: instrument_id, expression: "coalesce($.id, $.name)"}
          - {name: currency, expression: "coalesce($.currency, 'USD')"}
          - {name: symbol, expression: "$.u"}
          - {name: spot, expression: "cast(mkt.spot as double)"}
```

Run with `sonicetl.run_pipelines("examples/demo/pipelines.yml")`.

## Programmatic construction

The Python surface exposes the schema as dataclasses, so you can build a config
without hand-writing YAML:

```python
import sonicetl

cfg = sonicetl.PipelinesConfig([
    sonicetl.Pipeline(
        name="mktDataETL",
        sources=[sonicetl.Source("spot", "file://spot.json")],
        datasets=[
            sonicetl.Dataset("spot", "raw", to=sonicetl.Output("memory://spot"),
                             fields=[
                                 sonicetl.Field("name", "$._id"),
                                 sonicetl.Field("spot", "cast($.spot as double)"),
                             ]),
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
                fields=[
                    sonicetl.Field("instrument_id", "coalesce($.id, $.name)"),
                    sonicetl.Field("symbol", "$.u"),
                    sonicetl.Field("spot", "cast(mkt.spot as double)"),
                ],
            ),
        ],
    ),
])
yml = cfg.to_yaml()
```

You can also pass the dataclass config (or a plain `dict`, or a YAML file path)
directly to `sonicetl.run_pipelines(config)`.