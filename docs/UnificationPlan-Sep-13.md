# FinA Unification Plan — canonical `ProductTerms` + bidirectional projections

Status: **draft for discussion** (Sep 13). Not an implementation contract yet —
expect to revise this together before freezing fields.

## 1. Why this document

FinA today has four capabilities at different maturity levels:

1. a street-facing schema to build an RFQ UI on,
2. an ETL layer that maps inputs in seconds,
3. a risk/pricing engine that computes sensi + P&L in seconds,
4. an OLAP layer that queries any report version in seconds.

The engine, ETL and OLAP thirds are largely built. What is missing is the
**street-facing apex** and, more importantly, **one canonical economics object**
that all downstream representations derive from. Today the same product exists
as three loosely-coupled artifacts:

- the human term sheet — `fina-risk/skills/fina-risk/refs/termsheet1.md`
- the legacy (Murex "memraki") request — `.../refs/termsheet1.md.json`
- FinA's internal request — `fina-risk/skills/fina-risk/schema/pricing-request.schema.json`
  (and the reference engine's `fina-pricer/.../schema/pricing_request.schema.json`)

The mapping between them is **one-way** (legacy → internal only) and lossy. The
proposal is to insert a single `ProductTerms` model as the keystone and make
every representation a deterministic projection of it. `fina-trade`
(`FinA/modules/fina-trade`, the Postgres MCP server) is the **state machine /
system of record** that carries those terms through their life, and
`LifecycleState` (event-sourced from `fina-trade`) is the third input the engine
needs.

```
   street / RFQ form ──▶ ProductTerms ──emit──▶ human term sheet (A)
          │                   │
          │                   ├──emit──▶ legacy memraki request (B)
          │                   │
   fina-trade (Postgres)      └──emit──▶ FinA pricing-request (C) ──▶ engine ──▶ risk_wide/long ──▶ OLAP/UI (D)
    rfq → quote(v) → trade                  ▲                       ▲
    → instrument → positions                │                       │
    → lifecycle events ──LifecycleState─────┘                       │
          │                                                         │
   MarketSnapshot (versioned, external) ────────────────────────────┘
```

Design rules:
- **`ProductTerms` = immutable contract terms only.** Not market, not state.
- **`MarketSnapshot`** is external and versioned; injected at projection time.
- **`LifecycleState`** (locked assets, `already_knock_in`, paid fixings `N1`,
  KO/memory events, fixes) is mutable, **event-sourced by `fina-trade`**, and
  fed to projection C as the engine's `UpdatedLifecycle`.

## 2. The keystone: `ProductTerms`

A product-neutral core plus a typed `template` extension. Round-trippable and
UI-shaped (every required field is something a salesperson can type or pick).

```jsonc
{
  "product_id": "ELIFCN_KI_FINA1",          // stable instrument identity
  "template": {
    "name": "ELIFCN_KI",                    // product template
    "version": 1,
    "model_id": "memraki"                   // pricing model variant
  },
  "issuer": { "name": "[ISSUER]", "series": "[PRODUCT-ID]", "isin": null },
  "currency": "USD",                        // settlement currency
  "dates": {
    "trade_date": "2026-04-24",
    "issue_date": "2026-05-01",
    "initial_fixing_date": "2026-04-24",
    "final_fixing_date": "2027-02-01",
    "expiry_date": "2027-02-03",
    "maturity_date": "2027-02-03"
  },
  "notional": { "amount": 50000, "issue_price": 10000, "denomination": 10000, "minimum_investment": 50000 },
  "basket": {
    "type": "worst_of",
    "performance": "close / initial_spot",
    "underlyings": [
      { "id": "ADBE UW", "market": "US", "exchange": "US exchange", "currency": "USD",
        "calendar": "NYSE", "initial_spot": 239.97, "weight": 1, "adjustment_factor": 1 },
      { "id": "AMZN UW", "market": "US", "exchange": "US exchange", "currency": "USD",
        "calendar": "NYSE", "initial_spot": 260.00, "weight": 1, "adjustment_factor": 1 }
    ]
  },
  "strike": { "ratio": 0.78 },              // exercise price = ratio x initial_spot
  "knock_in": {
    "type": "EKI",
    "barrier_ratio": 0.70,
    "observation": "final_fixing_only",
    "payoff_on_hit": "physical_delivery",
    "settlement_type": "nominal / exercise_price shares of worst asset",
    "redemption_cap": 1.0,
    "already_knock_in": false
  },
  "call_memory": {
    "global": true,
    "call_barrier_ratio": 1.10,
    "observation": "daily_call_fixing",
    "memory_event": "close >= call_price",
    "call_condition": "all_underlyings_locked",
    "accrued_coupon_on_call": true,
    "locked": [ { "underlying": "ADBE UW", "locked": true, "date": "2026-06-01" },
                { "underlying": "AMZN UW", "locked": false, "date": null } ]
  },
  "coupon": {
    "type": "range_accrual_strip",
    "indicator": "WPS",
    "rate_kind": "day_in",                  // "fixed" for period 1, "day_in" after
    "rate": 0.009642,
    "range": { "floor_ratio": 0.10, "cap_ratio": null },
    "unpaid_rule": "N2 - N1 with partial current period and five full future periods",
    "memory_carry": true,
    "payment_lag_rule": "paymentDate - endDate per period; final lag after final fixing",
    "call_accrual_rule": "accrue unpaid in-range fixings through call",
    "periods": [
      { "index": 1, "start": "2026-05-01", "end": "2026-06-01", "type": "Fixed",    "total_trading_days": 21 },
      { "index": 2, "start": "2026-06-02", "end": "2026-07-01", "type": "Variable", "total_trading_days": 21 }
      // ... periods 3..9
    ]
  },
  "settlement": { "currency": "USD", "notional_return_leg": true, "physical_delivery_leg": true, "fractional_shares": "cash" },
  "conventions": {
    "observation_time": "1600", "timezone": "United States", "fx_source": "BFIX", "fx_attribute": "Mid",
    "bump": { "size": 0.01, "mode": "relative", "source": "marketData.equity[].spot", "crn": true }
  },
  "legacy_fields": {}                        // lossless retention of unmapped source fields
}
```

Notes on the model:
- `barrier_ratio`/`call_barrier_ratio`/`range.*_ratio` are relative; resolved to
  absolute prices against `initial_spot` at projection time (so `barrierPrice`,
  `strikePrice` in the legacy JSON are derived, not authoritative).
- `legacy_fields` is the escape hatch for loss-less round-trip (mirrors
  `term-sheet-conventions.schema.json` `legacy_fields`).
- `template.name/version/model_id` are the convention-versioning anchor; a
  registry (`product_templates/ELIFCN_KI@1.yaml`) would hold defaults + the
  projection rules.
- `knock_in.already_knock_in` and `call_memory.locked` are shown inline for
  readability but are **state, not terms**. Only their *inception* values belong
  here; the evolving values live in `LifecycleState` (§4) and are what projection
  C sends as `UpdatedLifecycle`.

## 3. The three projections

All are pure functions of `(ProductTerms, MarketSnapshot?)`.

### A. → human term sheet (`termsheet1.md` shape)

Terms-only template; market appears as "Initial Pricing Parameters"
(initial_spot, call price, exercise price, knock-in price), computed from
`basket.underlyings[].initial_spot` and the ratios. This is fully mechanical and
is the natural preview in an RFQ page.

### B. → legacy memraki request (`termsheet1.md.json` shape)

Must reproduce:
- `Chunk` envelope (`finaRefChunkID`, `libVersion`, `modelVersion`, `Jobs[]`).
- three `Jobs` (PUT / FUNDING / COUPON) sharing one `commonData.marketData`,
  differing only in `dealData` (`legName`, `legId`, `multiplier`).
- `marketData`: equity quotes, `corr`, `eqVol` grids, `estCurves`/`discCurves`,
  `MCPara.numPaths`, `evaluationDate` — all from `MarketSnapshot`.
- `dealData`: `KIKOSelect`, `knockInStar`, `RGACCLKO`, `globalKOStar`,
  `EQDPosition`, plus the identity fields.
- `Tasks[]`: base PV + the four ±1% relative bumps (`data_type`: -1 base, 1 stock,
  ±1% of the **quoted** spot).
- Date fields as Excel serials (`excel_date`/`year_fraction` already in
  `fina-risk/pricing.py`).

This emitter does not exist yet and is the main new work for concern 1.

### C. → FinA pricing-request

Two targets already exist and should be treated as two views of C:
- `fina-risk/skills/fina-risk/schema/pricing-request.schema.json`
  (`market_data` / `legs` / `parameters` / `execution`) — what `daily_termsheet.py`
  and the native kernel consume.
- `fina-pricer/.../pricing_request.schema.json`
  (`InstrumentKey` / `UnwindMapRaw` / `RiskFactorKeys` / `MarketDataSnapshot` /
  `UpdatedLifecycle` / `Legs` / `parameters`) — what the reference engine consumes.

The existing `fina-risk/src/fina_risk/etl.py:compile_pricing_request` is the
seed for the C-emitter; it should be refactored to emit from `ProductTerms`
rather than re-parse the legacy JSON each time.

### Reverse: parse → `ProductTerms`

`parse_legacy_request` (from `etl.py`) and a future `parse_pricing_request` give
round-trip. **Round-trip invariant to test:** `parse(emit(terms)) == terms`
(modulo `legacy_fields`), and `emit(parse(legacy))` reproduces the legacy request
bit-for-field for the fixture.

## 4. The state machine: `fina-trade` (system of record)

`FinA/modules/fina-trade` is the persistent, event-sourced spine. Its Postgres
schema (`modules/fina-trade/schema/postgres.sql`) owns the lifecycle the rest of
the paradigm must not duplicate:

| Table | Role | Payload it should carry |
|---|---|---|
| `rfqs` | client request + status (`RECEIVED→QUOTED→CONVERTED/…`) | **`ProductTerms`** (street input) |
| `quotes` | immutable versions (`quote_version`) of a priced RFQ | projection **C** (`pricing_request` JSONB) + PV/price summary |
| `trades` | accepted quote, economic `terms`, portfolio/qty | `ProductTerms` snapshot + accepted economics |
| `instruments` | tradeable identity, `indicative` until first accept, `priced_at` | product identity + reference request |
| `positions` | aggregate of non-terminal trades per `(portfolio, instrument_id)` | derived, not authored |
| `trade_lifecycle_events` | append-only `observe` / `corporate_event` / `amend` / `cancel` | the **event log** that produces `LifecycleState` (only `amend`/`cancel` are wired through Postgres today — see gap below) |

MCP tools (`fina_trade/mcp_server.py`): `rfq_create`, `quote_persist`,
`trade_accept`, `trade_amend`, `trade_cancel`, `trade_get`, `trade_lifecycle`,
plus `*_query` and `database_health`. The in-memory `TradeRepository` is the
deterministic test double; `PostgresTradeRepository` is production.

Where it sits in the paradigm:

- **Concern 1 (street schema): it is the RFQ home.** `rfqs.request` is the
  natural home of `ProductTerms`; the RFQ page reads/writes it. Nothing else
  should own street input.
- **Concern 3 (engine): it is the trigger, not the calculator.** `quote_persist`
  stores the priced result; `observe` (daily fixing) and `apply_corporate_event`
  are the *events* that produce a new `LifecycleState` and request a re-price
  (via the scheduler EventBus, per `refs/../modules/fina-trade/SKILL.md`). It
  never computes PV/Greeks.
- **Concern 4 (OLAP versioning): it is the version anchor.** `quote_version`,
  `instruments.priced_at`, and the append-only `trade_lifecycle_events` supply the
  report store with the `(instrument, version, as_of)` keys it currently lacks.
- **`ProductTerms` ↔ `fina-trade`:** the terms object is the *payload* the server
  persists and versions; the server is the *authority* over state transitions.

Two consequences for the model:

0. **Gap: lifecycle intake is not wired in Postgres.** `TradeRepository`
   (in-memory) has `observe()` and `apply_corporate_event()`, but
   `PostgresTradeRepository` / `mcp_server.py` expose only `trade_amend` and
   `trade_cancel` as state transitions. For `fina-trade` to actually be the
   lifecycle authority (daily fixing, corporate events) it needs typed MCP tools
   `trade_observe(trade_id, FixingObservation)` and
   `trade_corporate_event(trade_id, CorporateAction)` that append events and
   emit the reprice trigger.
1. `LifecycleState` should be a first-class DTO, not folded into `ProductTerms`:
   `{already_knock_in, locked[], ko_events[], paid_fixings N1[],
   applied_fixings[], as_of}`. Projection C merges
   `(ProductTerms, MarketSnapshot, LifecycleState)` →
   `pricing_request.UpdatedLifecycle`.
2. The engine's output must be written back to `quotes` as a **new
   `quote_version`**, never overwriting — the pattern the fina-trade SKILL.md
   already mandates.

## 5. The leg model and the instrument/leg state machine

A flow trader replicating an ELIFCN with public instruments does not think of
"one note" — they think of a **replicating book** of legs:

| Leg | Public-instrument replication | Economics |
|---|---|---|
| FUNDING | zero / money-market / issuer bond | upfront payment at par, redeemed at maturity or call |
| COUPON | range-accrual coupon strip (range/digital notes) | accrues per in-range daily fixing, memory carry |
| PUT | worst-of down-and-in put (physical) | KI at final fixing, delivery of worst asset |

Greek sensi **per leg** is what makes the book monitorable and hedgeable: each
leg maps to a public instrument, so delta/vega/etc. can be netted and hedged
leg-by-leg instead of as an opaque note. Today the leg knowledge is re-encoded
independently in at least three places (`daily_termsheet.py` loop,
`postgres.sql` columns, SKILL.md tables). The proposal: define it **once**,
declaratively, as an `InstrumentModel` with three facets:

1. **Structure** — leg decomposition + payoff template (feeds `ProductTerms`,
   projections B/C, and the risk engine's per-leg payoff functions).
2. **State machine** — per-leg states, transition events, guards, operations, and
   resulting state (the four questions below).
3. **Dependencies** — the static/dynamic market data each leg needs, and when it
   must be published (feeds §6 and scheduling).

### 5.a Per-leg state machine

Each leg is an independent state machine; the instrument is the product of its
legs' states, and instrument-level transitions are guards over leg states.

```jsonc
{
  "template": "ELIFCN_KI", "version": 1,
  "legs": {
    "FUNDING": {
      "states": ["PENDING", "SETTLED", "MATURED", "CANCELLED"], "initial": "PENDING",
      "transitions": [
        { "from": "PENDING", "event": "trade.accepted",     "operations": ["disburse", "record_cashflow"], "to": "SETTLED" },
        { "from": "SETTLED", "event": "instrument.matured", "operations": ["settle_principal"],            "to": "MATURED" }
      ]
    },
    "COUPON": {
      "states": ["INITIAL", "ACCRUING", "PAID", "CALLED", "MATURED"], "initial": "INITIAL",
      "memory": { "mode": "carry", "per_period": true, "denominator": "N2" },
      "transitions": [
        { "from": "*", "event": "market.fixing.observed", "guard": "in_range",               "operations": ["accrue", "carry_memory"], "to": "ACCRUING" },
        { "from": "ACCRUING", "event": "coupon.payment_date",                                 "operations": ["pay_coupon", "reset_memory"], "to": "PAID" },
        { "from": "*", "event": "instrument.called",                                          "operations": ["pay_accrued"],           "to": "CALLED" }
      ]
    },
    "PUT": {
      "states": ["ALIVE", "KNOCKED_IN", "KNOCKED_OUT", "DELIVERED", "EXPIRED"], "initial": "ALIVE",
      "transitions": [
        { "from": "ALIVE", "event": "market.fixing.observed", "guard": "worst <= ki_barrier",
          "monitoring": "final_fixing_only", "operations": ["mark_knocked_in"], "to": "KNOCKED_IN" },
        { "from": "ALIVE",      "event": "instrument.called",  "operations": [],                     "to": "KNOCKED_OUT" },
        { "from": "KNOCKED_IN", "event": "instrument.matured", "operations": ["compute_delivery_shares"], "to": "DELIVERED" }
      ]
    }
  },
  "instrument_transitions": [
    { "guard": "all legs in {CALLED}", "publish": "instrument.called" },
    { "guard": "PUT == KNOCKED_IN",    "publish": "leg.put.knocked_in" }
  ]
}
```

This answers the four questions directly: (1) allowed states per leg, (2) the
event that triggers a transition, (3) the operations run during it, (4) the
target state — plus composite instrument transitions covering split/merge-like
behavior.

### 5.b The three consumers

| Consumer | Uses the model for |
|---|---|
| **fina-risk** (§8) | one shared path cube → evaluate every leg's payoff function on the same paths; the state machine says which legs are live at each observation. Per-leg PV/Greeks fall out; note PV is the signed sum. |
| **fina-trade** (§4) | derive the subscriptions (`market.fixing.observed`, `market.corporate_action.published`), the operations to run on transition (record fixing, carry memory, reprice), and the domain events to publish (`leg.coupon.paid`, `leg.put.knocked_in`, `instrument.called`). |
| **market data** (§6) | derive, per leg, which series are required at what frequency, and therefore the publication contract subscribers wait on. |

### 5.c Who owns what

The state machine is **declarative data, not code**, split so product knowledge
never lands in the scheduler — and, crucially, so the pricing hot path never goes
through a JSON interpreter:

- **Schema/semantics (definition)**: `InstrumentModel` (structure + state machine
  + dependencies) lives in the shared `FinA/schemas/` package, versioned with
  `template.version`. The transition semantics are specified once, as a
  **restricted state DSL** (finite states, numeric guards, a fixed operation
  algebra). CI validates the model; each trade pins the version.
- **Reference runtime (oracle)**: a generic interpreter for that DSL lives in
  `fina-core`. It is used for *low-volume* work — live trade lifecycle in
  `fina-trade`, conformance testing, what-if replay. It is **not** the pricing
  path.
- **Compiled runtime (hot path)**: the same DSL is **lowered/compiled into
  `fina-risk`'s vectorized kernel** and fused with the payoff loop over the path
  cube (§5.d). This is where simulation-time state evolution happens.
- **Authority (stateful)**: `fina-trade` is the single authority that persists
  leg states per trade, applies *real* events via the oracle, runs durable
  operations, and publishes domain events.
- **Consumers (stateless)**: `fina-risk` prices; market-data services publish.
- **Transport/dispatch**: `fina-core-scheduler` routes topics to threads and
  executes operations. It is the bus + task runner, **not** the state store and
  not the product model.

The shared contract is **one semantics, two backends** (interpreter + compiled
kernel), kept honest by a conformance corpus. `fina-trade` is the only writer of
*real* state; the scheduler never needs to know what an ELIFCN is.

### 5.d Simulation-time state evolution: one spec, two backends

Pricing an ELIFCN requires evaluating the state machine **along every simulated
path and at every observation**, fused with the cashflow accumulation: whether
call/memory locks fire (which stops coupon accrual), whether the EKI gate opens
at the final fixing, and how the memory accumulator carries. A generic
`apply(state, event)` reducer over JSON is scalar, allocating, and
dynamic-dispatch-heavy — unusable at 10⁵–10⁶ paths × 10²–10³ steps. The
resolution is a compiler split:

```
   InstrumentModel (DSL, declarative)              ← control plane, authored once
            │  lower / compile
            ▼
   flat state kernel (state vector + transition table + op micro-ops)
        ├── reference interpreter   (fina-core / fina-trade / tests)   ← oracle
        └── vectorized C++/GPU kernel (fina-risk)                      ← hot path
```

What makes this compilable is that the ELIFCN state machine is almost entirely
**numeric and time-structured**:

| DSL concept | Compiles to |
|---|---|
| finite state (KO fired, KI armed, period index) | small integer **state vector**, one lane per path |
| numeric guard (`worst <= ki_barrier`, `worst in range`, `spot >= call`) | **predicated compare** over the path slice |
| time-static schedule (period end, final fixing, payment date) | **static index masks** on the observation axis |
| operations (`accrue`, `carry_memory`, `reset_memory`, `cap`, `mark_*`) | **in-place vector ops** on per-path accumulators |
| leg liveness | **branchless select** of the leg payoff into the PV accumulator |

The inner loop is then, per observation step, a handful of masked arithmetic
updates on the shared path cube — with no state-machine dispatch at all. This is
the same shape `daily_termsheet.py:78-109` already implements by hand (memory/KO
carry inside the period loop); the change is that the sequence is **generated from
the model** rather than hand-written per product.

Two compile targets, in order of pragmatism:

1. **Table-driven vectorized interpreter (C++)** — the model lowers to flat
   arrays (state-count, transition table, guard operands, op codes); the kernel
   loops over paths with predication. Flat memory, SIMD-friendly, no codegen.
   *Recommended first.*
2. **AOT/JIT codegen (C++/CUDA)** — specialize the loop per product for maximum
   throughput. Higher complexity; adopt only if (1) is too slow.

Both backends consume the **same lowered kernel**, so semantics cannot drift.
Conformance test: run the oracle and the compiled kernel over the same path cube
and assert equality — the existing Python-mirror vs. C++-native parity discipline
(reference: `lane-checkpoint.md`) extended to state.

This directly answers "how does a single simulation decompose multiple legs": the
path cube is built once; the compiled kernel advances every leg's state and
accumulates every leg's cashflow in the same pass; per-leg PV/Greeks are then
sensitivities of each leg accumulator, and the note PV is their signed sum.


## 6. Market data: dependency-driven publication

Static and dynamic market data are themselves event streams. The state machine
declares its dependencies so subscribers know *what* to wait for and *when* it is
ready:

| Class | Examples | Frequency | Topic | Consumer effect |
|---|---|---|---|---|
| static | trading calendar, holiday/fixing schedule | on change | `market.calendar.published` | schedule + next-fixing computation |
| dynamic EOD | spot, disc/est curves, dividends | end of day | `market.snapshot.published` | daily fixing observation, PV |
| dynamic intraday | spot quotes, implied vol | intraday | `market.quote.published` | reprice triggers, delta/vega refresh |
| surface/static-per-date | vol surface (`eqVol` grid), correlation, fxvol | per valuation | `market.surface.published` | vega / correlation / fx sensi |
| corporate | dividends, splits, mergers, delisting | on event | `market.corporate_action.published` | `trade_corporate_event`, term adjustment |

Envelope (same shape as scheduler events): `{topic, as_of, version, source,
payload, event_id}`. `InstrumentModel` declares the required kind/frequency and
the lifecycle state during which it is required:

```jsonc
"market_dependencies": [
  { "kind": "calendar",    "frequency": "static",  "topic": "market.calendar.published" },
  { "kind": "spot",        "frequency": "daily",   "topic": "market.snapshot.published", "required_states": ["ACCRUING"] },
  { "kind": "vol_surface", "frequency": "pricing", "topic": "market.surface.published" },
  { "kind": "correlation", "frequency": "pricing", "topic": "market.surface.published" }
]
```

Publication-timing rule: a leg in a given state only reacts to data whose `as_of`
is after that state's last transition. "When would this data publish so the
subscriber can process" then becomes a **guard in the reducer** rather than an
ad-hoc check in each consumer.

## 7. Where the ETL layer fits (concern 2)

Two speeds, by design:

| path | volume | tooling | target |
|---|---|---|---|
| street → `ProductTerms` (tiny, in-memory) | 1–N RFQs | Python projections in `etl.py` | ms |
| legacy → fina-native columnar (bulk) | 100k+ | Rust sonic-rs `run_pipelines` (`scripts/run_100k_rust_cpp.py`) | 1.28 s @100k |

The Rust ETL stays the batch engine (already streaming, no DOM). The new
`ProductTerms` layer is the *semantic* layer above it, not a replacement.
Gaps to close: real exchange **calendar normalization** (currently weekday-based
in `daily_termsheet.py:weekday_serials` — SKILL.md:280 flags missing NYSE/QuantLib
calendar), serial↔ISO conversion, dividend projection, corporate-action
adjustment, and a **stage-level SLA** table (only the 100k number exists today).

## 8. Where the pricing engine fits (concern 3)

`ProductTerms` + `MarketSnapshot` + `LifecycleState` → `PathCube` → engine → risk cells.

| lane | binding | observation | sensi/P&L | 100k wall |
|---|---|---|---|---|
| terminal parity | `run_cpp_parity` | terminal cube | FD delta/gamma + Taylor-2 | 1.5 s |
| daily EKI batch | `run_daily_termsheet_batch` | daily cube | FD daily Greeks | 47.5 s |
| numpy hybrid AAD | `run_risk_task` | shared cube | AAD + CRN fallback | 79 s |

Gaps: daily lane misses the "seconds" target (needs shared daily path reuse +
vector/tensor kernels, or AAD); AAD is absent from the native daily kernel
(XAD lives only in fina-pricer); vega/IRPV01/FX/skew/cross-Greeks are numpy-only;
the daily state cube with a real calendar does not exist yet.

## 9. Where OLAP fits (concern 4)

Strongest layer. `fina-risk/src/fina_risk/olap.py` (DuckDB-over-Parquet, SSRM),
`fina-olap` server, and `fina-olap/fina-table` React AG-Grid frontend. To make
"any version of a report" true, add an explicit versioning contract:

- every risk row keyed by `(portfolio_id, instrument_id, leg_id, risk_factor_id, greek, bucket_id)`
  plus provenance `(terms_version, terms_hash, market_version, run_id, engine_version)`;
- a compact `report_manifest` (version → partitions, counts, checksum);
- partition pruning + small-file compaction.

Open: freezing the columnar schema for the daily EKI fields (`memory_carry`,
`coupon_fixings`, `ki/ko_prob`, Taylor components) and wiring the engine to
materialize full `risk_wide`/`risk_long` at scale (today the native 100k lane
emits checksums, not risk rows).

## 10. Status vs the four concerns

| # | Concern | Have | Missing |
|---|---|---|---|
| 1 | street-facing schema → 1a/1b/1c | reference term sheet + legacy JSON; `term-sheet-conventions.schema.json`; fina-pricer `pricing_request.schema.json`; **fina-trade RFQ/Quote/Trade/Instrument/Position state machine** (`modules/fina-trade`) | UI-shaped RFQ input schema; canonical `ProductTerms`; first-class `LifecycleState`; **term-sheet emitter (A)**; **legacy emitter (B)**; reverse parser; template registry/versioning |
| 2 | ETL map in seconds | `compile_pricing_request`, `augment_termsheet`; Rust sonic-rs 100k in 1.28 s; append-only Parquet ingest | general street→terms→request graph; real calendar; serial/ISO + dividends + CA; CDC/versioning; per-stage SLA |
| 3 | engine sensi + P&L in seconds | terminal (1.5 s/100k) + daily EKI (47.5 s/100k) native kernels; numpy hybrid AAD; Taylor-2 checksums; method provenance | daily lane to seconds; native AAD; full Greek vector in native; daily state cube + real calendar; realized-vs-forecast attribution wired to OLAP |
| 4 | OLAP any-version in seconds | DuckDB/Parquet SSRM (`risk_wide`/`risk_long`); fina-olap server; fina-table UI; **fina-trade append-only version anchor** (`quote_version`, `priced_at`, lifecycle events) | explicit report-version model + manifest joining trade versions to report partitions; engine→risk-row materialization at scale; frozen daily-EKI columnar schema; prune/compaction/aggregation stats |
| — | **cross-cutting: leg model + state machine** | leg decomposition implicit in `daily_termsheet.py` / C++ kernel / `postgres.sql`; scheduler `EventBus` + subscription topics (`trade.lifecycle.*`, `quote.created`); `TradeRepository.observe`/`apply_corporate_event` (in-memory only) | shared `InstrumentModel` (structure + state machine + dependencies); generic transition reducer in `fina-core`; typed `market.*` topics; `trade_observe` / `trade_corporate_event` MCP tools |

## 11. Open convention decisions (must resolve before freezing `ProductTerms`)

1. **EKI level.** Native kernel gates on `knockInStar.KIBarrier = 0.70` → PUT
   0.01619; legacy reports ≈0.02113, which matches a no-op gate at
   `maturBarrier = strikeKI2 = 0.78`. Pick one and encode it explicitly (the
   field `knock_in.barrier_ratio` currently would carry 0.70).
2. **Coupon denominator.** Currently `N2` (`daily_termsheet.py`), numerator
   `max(observed_fixings − N1, 0) + memory`, capped at 1. Confirm the partial
   current period + five future periods rule.
3. **Terminal vs daily lane.** Keep both; label `execution.monitoring_frequency`
   on every emitted request and every stored risk row.
4. **Coupon quote scale.** `legacyCouponQuoteScale = 10.0`
   (`daily_termsheet.py:111`) — confirm this is a unit convention, not a fudge.
5. **Calendar.** Move from weekday-based to QuantLib NYSE (or vendor calendar)
   before claiming exchange-calendar parity.

## 12. Proposed first bricks (for discussion, not yet committed)

1. Resolve §11.1 (EKI level) — everything downstream depends on it.
2. Write `schema/product-terms.schema.json` (product-neutral core + `template`).
3. Write projection-1a `emit_term_sheet(terms) -> md` and diff against
   `termsheet1.md`.
4. Write projection-1b `emit_legacy_request(terms, market) -> json` and assert
   fixture round-trip against `termsheet1.md.json`.
5. Refactor `compile_pricing_request` into `emit_pricing_request(terms, market)`
   (projection-1c) and keep `parse_legacy_request` as the inverse.
6. Add round-trip + reconciliation tests to the `fina-risk` suite.
7. Draft `schema/instrument-model.schema.json` — leg decomposition + per-leg
   state machine + market dependencies (§5), validated in CI.
8. Specify the restricted **state DSL** and implement the reference interpreter
   (oracle) in `fina-core`, with `ELIFCN_KI@1` as the first fixture.
9. Lower the DSL to a **flat state kernel** and run it in the `fina-risk`
   vectorized kernel, fused with the payoff loop; add a **conformance corpus**
   asserting oracle == compiled kernel over the same path cube (§5.d).
10. Expose `trade_observe` / `trade_corporate_event` on the Postgres MCP server so
   `fina-trade` can actually apply `market.*` transitions.
11. Only then: UI-shaped RFQ input schema + RFQ form, built on `ProductTerms`.

## 13. Discussion questions (mutual update)

- Q1: Is `ProductTerms` the right altitude, or should the keystone be the
  fina-pricer `PricingRequest` itself (i.e. treat that as canonical and derive
  the street view from it)? Trade-off: less duplication vs. a street model that
  doesn't leak engine concepts (`UnwindMapRaw`, `RiskFactorKeys`).
- Q2: Should market data be *inside* `ProductTerms` (frozen at trade) or always
  external `MarketSnapshot`? (This doc assumes external; confirm.)
- Q3: For projection B, is bit-exact reproduction of `termsheet1.md.json`
  required, or is semantic equality enough?
- Q4: Which vocabulary wins for the UI — the term-sheet language
  (strike %, knock-in %) or the engine language (ratios, barriers)? The doc
  assumes term-sheet language with ratios underneath.
- Q5: Where should the template registry live — `fina-risk/skills/.../schema/`,
  or a new shared `FinA/schemas/` package both `fina-risk` and `fina-pricer`
  can consume?
- Q6: Where does `ProductTerms` physically live? Options: (a) a shared
  `FinA/schemas/` package; (b) a module inside `fina-trade` (since it owns RFQ /
  quote / trade persistence); (c) inside `fina-risk` as an importable skill
  asset. This doc assumes (a) so neither engine nor trade owns it.
- Q7: Does `fina-trade` persist projection **C** or `ProductTerms`? Today
  `quotes.pricing_request` holds an engine request. Should `rfqs.request` /
  `trades.terms` be re-typed to `ProductTerms`, with projection C derived at
  quote time and stored alongside? (This doc assumes yes.)
- Q8: Is `LifecycleState` derived on demand from `trade_lifecycle_events`, or
  snapshotted into the reprice request each run? Snapshot-in-request is
  reproducible/auditable; derive-on-demand is simpler but makes replay depend on
  event order.
- Q9: How granular should the state machine be — per-leg only, or also
  per-period coupon sub-states (UNPAID/PARTIAL/PAID per period)? Per-period is
  more faithful to memory carry but multiplies states.
- Q10: Where does the state DSL *definition + oracle* live? `fina-core` (shared,
  lets `fina-trade` and tests use one interpreter) vs. `fina-trade` (has the event
  log). **Revised:** the pricing hot path is a separate, compiled backend in
  `fina-risk` either way (§5.d), so this question is only about the oracle's
  home, not about a single shared implementation.
- Q11: Are market-data topics produced by a dedicated market-data service, or by
  `fina-core` on a schedule? Either way the `InstrumentModel.dependencies`
  contract is the same; this decides who publishes.
- Q12: Should `fina-core-scheduler` FinaProcess YAML be **generated** from an
  `InstrumentModel` (so `rfq → quote → register → observe → reprice → olap`
  threads and subscriptions are derived), or hand-authored per product family?

### Resolved (Sep 13)

- **Q6 → (a)**: `ProductTerms` lives in a shared `FinA/schemas/` package; neither
  `fina-risk` nor `fina-trade` owns it. The same package holds
  `instrument-model.schema.json` (§5).
- **Q7 → yes**: `rfqs.request` and `trades.terms` are typed `ProductTerms`;
  `quotes.pricing_request` holds projection C, derived at quote time and stored
  beside the quote result. The engine never reads `ProductTerms` directly.
- **Q8 → snapshot**: `LifecycleState` is snapshotted into each reprice request
  (reproducible); the `trade_lifecycle_events` log remains the audit trail, not
  the hot path.
- **Q10 → one spec, two backends**: the DSL definition + reference interpreter
  (oracle) are shared (schema package + `fina-core`); the pricing hot path is a
  **compiled vectorized kernel inside `fina-risk`**, not the JSON reducer. A
  conformance corpus keeps the two backends equal. See §5.d.

## Appendix A. Positioning vs. ISDA CDM / industry schema unification

Worth stating explicitly, because it bounds the plan.

**What CDM is.** ISDA's Common Domain Model is an open, machine-readable model of
financial products, trades, and lifecycle events, paired with a DSL (Rosetta) and
deterministic lifecycle functions. It is often bundled, rhetorically, with a
shared ledger/execution backend and the promise of industry-wide reconciliation
removal.

**The bundling hides two separable claims**, and FinA should take opposite
positions on them:

| Claim | Scope | FinA position |
|---|---|---|
| **Semantic unification** — one model for products, events, and lifecycle functions | shared *meaning* | **Adopt** (at firm scope) |
| **Execution unification** — one valuation engine / one ledger for all users | shared *runtime* | **Reject** |

**Why reject execution unification.**
1. **Consumers are heterogeneous and conflicting.** A flow trader wants leg-level
   replication and fast delta/vega for hedging; risk wants AAD and a full Greek
   vector; `fina-trade` wants deterministic lifecycle and reconciliation; the UI
   wants terms; quant wants what-if scenarios. No single engine is optimal for
   all. `fina-risk` already embodies this: terminal lane, daily EKI lane, numpy
   hybrid-AAD lane — plural by design.
2. **Valuation is proprietary edge.** Sharing curves, calibration, and Monte
   Carlo is commercially undesirable for a flow desk; the model is the industry
   commons, the pricer is the franchise.
3. **Reconciliation is not an engine problem.** Two parties reconcile when they
   disagree on *semantics, inputs, or lifecycle functions* — not on the
   valuation method. Agreeing those (products, observations, deterministic
   cashflows) removes most breaks while leaving valuation free.
4. **The ledger is unnecessary at firm scope.** Event-sourcing
   (`trade_lifecycle_events`) plus the shared Parquet store already gives the
   audit/replay properties a chain is usually invoked for.

**What FinA does adopt.** The *useful half* of the CDM idea:
- a **common event envelope** and typed `market.*` / `leg.*` / `instrument.*`
  topics (§6);
- **`ProductTerms` + a per-leg state machine with deterministic lifecycle
  functions** (§5) as the shared semantics;
- a **Rosetta-like split** — one DSL/model, plural runtimes (oracle interpreter +
  compiled kernel + legacy/parity lanes), kept equal by a conformance corpus.
  This is precisely CDM's own pattern (shared model, code-generated per-runtime
  functions); the plan is CDM-style *internally*, without the monoculture.

**Scope discipline (the real rejection).** CDM's cost is its universality across
all OTC derivatives. FinA should be **firm-specific and product-family-scoped**:
a small universal core (event envelope, lifecycle-function contract) plus
extensible per-family models (`ELIFCN_KI@1`, then whatever is actually traded).
Do not model the universe up front; grow the model from the book.

**Net position:** reject the "one engine to rule them all" dream; adopt
"one model, many engines." Reconciliation is reduced by shared *meaning and
events*, never by a shared *valuation kernel* — which is also why the two-backend
compiler split (§5.d) is the right expression of the idea rather than a single
shared reducer.

