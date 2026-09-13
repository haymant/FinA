# FinA ETL

ETL stage for the FinA ecosystem: turn reference inputs into the normalized
schemas the compute stages consume, then persist results to the **shared store**
so fina-risk can price them and fina-olap / fina-table can analyse them.

## Where it fits

```
reference termsheet / market snapshot
        │  extract
        ▼
   augment  ── clone an instrument into an N-sized batch with permuted economics
        │
        ▼
   transform ── compile each variant into fina-risk's pricing-request schema
        │
        ▼
   load     ── write JSON/Parquet to the shared store (local | GCS | S3)
        │
        ├── fina-risk  (price the batch; write risk_wide / risk_long Parquet)
        └── fina-olap  (fina-table queries the same store)
```

The scheduler (`fina-core-scheduler`) owns *when/how* a stage runs; this skill
owns the *data contract* between stages.

## Augmentation model

Mirrors `fina-risk/scripts/generate_benchmark.py::make_instruments`: each variant
keeps the ELI structure (PUT / FUNDING / COUPON legs) but randomises economics
deterministically from a seed:

| Field | Permutation |
| --- | --- |
| relative `strike` | `base_strike × U(0.85, 1.05)` |
| knock-in `KIBarrier` | `strike × U(0.80, 0.95)` |
| call barrier (`gblBarPrice`) | `U(1.02, 1.20)` |
| `notional` | one of `100k / 250k / 500k / 1M` |
| underlyings / spot | 2–3 names sampled from the market snapshot |
| coupon `accruRate` | `U(0.006, 0.018)` |
| expiry / maturity / KO calendards | ±10 business days |

## Tools (fina-risk MCP)

| Tool | Purpose |
| --- | --- |
| `augment_termsheet` | clone the bundled/referenced term sheet into `count` variants with permuted economics (`seed`, `out_path`). Deterministic. |
| `compile_pricing_requests` | convert augmented term sheets into `pricing-request` objects (validated against the schema). |
| `ingest_instruments` / `ingest_market_data` | append normalized instrument / market records to the store. |
| `store_config` / `store_configure` / `store_resolve` | shared store location + partition layout. |

Input shape: `fina-risk/skills/fina-risk/refs/termsheet1.md.json`
(`Chunk.Jobs[]` → `commonData.dealData` + `commonData.marketData`).
Output schema: `fina-risk/skills/fina-risk/schema/pricing-request.schema.json`
(`instrument_key`, `market_data`, `legs`, `parameters`, `common_economics`).

CLI references: `fina-risk/scripts/generate_benchmark.py` (augmentation),
`fina-risk/scripts/ingest_100k_inputs.py` (normalized ingest).

## Scheduler integration

`fina-core-scheduler` drives the batch via these handlers (registered by
`register_fina_handlers(..., risk_callable=...)`): `fina-etl.augment_termsheet`,
`fina-etl.compile_pricing_requests`, then `fina-risk.risk_batch` and
`fina-olap.olap_query`. See the canonical process
`FinA/examples/processes/fina-risk-augment-to-olap.yml` (thread `priority`
makes the ETL stages run first; `risk` uses `risk_request.use_compiled: true`
to consume the compiled requests). MCP equivalents: `run_etl_task` and
`run_risk_task`.

## Shared store contract

Identical env vars to fina-olap (`FINA_OLAP_STORE`,
`FINA_OLAP_PARQUET_ROOT`, `FINA_OLAP_BUCKET`, `FINA_OLAP_PATH`,
`FINA_OLAP_PARTITION_GLOB`, `FINA_OLAP_HIVE_PARTITIONING`) — point the ETL write
and the OLAP read at the same location. See
`fina-risk/skills/fina-risk/SKILL.md` → *Shared store configuration*.

## Verification

```bash
uv --directory fina-risk run pytest tests/test_etl.py -q
# executable coordinator e2e (needs fina-core built + fina-risk):
PYTHONPATH=FinA/python python FinA/examples/e2e_fina_risk_olap.py --count 30 --paths 128 --root /tmp/fina-risk-e2e
```
