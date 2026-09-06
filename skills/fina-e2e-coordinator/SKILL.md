---
name: fina-e2e-coordinator
description: Coordinate and verify FinA cross-feature FinaProcess executions through the scheduler as the single entrance, wiring real pricer, trade, event-bus, and OLAP handlers. Use for executable end-to-end RFQ-to-trade-to-lifecycle-repricing verification.
---

# FinA E2E Coordinator

Use this skill as the **single entrance** for cross-feature FinA verification. Submit one `FinaProcess` to `fina-core-scheduler`; do not call pricer, trade, or OLAP tools as independent top-level workflow steps. Register concrete adapters before submission, then inspect the process snapshot and assert every task result and event delivery.

## Core principle: every task is a lambda unit

Treat every thread as a total lambda:

```text
result = handler(full_context)
```

A task must be executable without hidden conversational state, ambient mutable variables, or an implicit lookup. Its full context is the union of:

- immutable process parameters;
- explicit task parameters;
- dependency results, addressed by dependency thread name;
- the triggering event, when event-driven;
- correlation identifiers and the task’s attempt number.

Pass this context through the scheduler’s thread parameters. A handler may use injected service capabilities, but it must not infer business inputs from global state. Return a JSON-serializable result or raise an error; never return a plausible success when a downstream tool failed.

The formal lambda-task contract is bundled at `references/lambda-task.schema.json`. Validate task declarations with `scripts/validate_lambda_task.py` before submission.

## Required execution sequence

1. **Assemble one complete RFQ context.** Include instrument, market data, risk-factor keys, lifecycle state, pricing parameters, evaluation timestamp, and correlation ID. Do not make the pricer reconstruct missing RFQ fields from process-global state.
2. **Create the service boundary.** Instantiate `SchedulerService`, a real `TradeRepository`, the pricing callable, and the OLAP callable. The pricing callable may be a direct `riskcube_mcp.sensitivity` wrapper or a client for the pricer MCP `pricing_and_sensitivity` tool.
3. **Register adapters before process creation.** Call `register_fina_handlers(...)`. This registers `fina-pricer.pricing_and_sensitivity`, `fina-trade.register`, `fina-trade.amend`, and `fina-olap.group_sensitivities`.
4. **Declare all tasks in one `FinaProcess`.** The initial path is `quote -> register_trade -> amend`; declare `reprice` as dormant with `triggered_by: trade.lifecycle.amended`; make OLAP depend on `reprice`.
5. **Subscribe lifecycle events in the process.** The subscription must name `start_thread: reprice`. The trade adapter must publish only after the repository mutation succeeds; the scheduler then injects the event payload into the reprice lambda context.
6. **Submit through the scheduler only.** Use `scheduler.create_process(definition)` or the scheduler MCP `process_submit`. Do not bypass the process by invoking pricer or trade as an additional coordinator action.
7. **Verify behavior, not only terminal state.** Require two real pricing calls, a persisted amended trade, `trade.lifecycle.amended` delivery, a finished reprice thread, and OLAP output derived from the reprice result.

## Canonical process shape

```yaml
api_version: fina/v1
kind: FinaProcess
metadata:
  name: real-fcn-rfq-to-risk
parameters:
  correlation_id: rfq-FCN-T1
  pricing_request: ${complete_rfq_context}
  trade_id: FCN-T1
threads:
  - name: quote
    handler: fina-pricer.pricing_and_sensitivity
    parameters:
      context: {task: quote, dependency_results: {}, event: null}
  - name: register_trade
    handler: fina-trade.register
    depends_on: [quote]
    parameters:
      context: {task: register_trade, dependency_names: [quote]}
  - name: amend
    handler: fina-trade.amend
    depends_on: [register_trade]
    parameters:
      context: {task: amend, dependency_names: [register_trade]}
  - name: reprice
    handler: fina-pricer.pricing_and_sensitivity
    triggered_by: trade.lifecycle.amended
    parameters:
      context: {task: reprice, dependency_names: [], event_from_subscription: true}
  - name: olap
    handler: fina-olap.group_sensitivities
    depends_on: [reprice]
    parameters:
      context: {task: olap, dependency_names: [reprice]}
subscriptions:
  - topic: trade.lifecycle.amended
    handler: fina-pricer.pricing_and_sensitivity
    start_thread: reprice
```

The process schema is `fina-core-scheduler/schema/fina-process.schema.json`. `triggered_by` marks a dormant lambda; the subscription activates it with a complete event context.

## Handler contracts

| Handler | Lambda input requirement | Output requirement |
|---|---|---|
| `fina-pricer.pricing_and_sensitivity` | Complete `PricingRequest`, correlation ID, and optional lifecycle event | PV, currency, price, and RiskCube or an explicit error |
| `fina-trade.register` | Complete trade plus the `quote` dependency result | Durable trade record and registration event |
| `fina-trade.amend` | Trade ID, mutation, reason, and `register_trade` dependency result | Durable amended trade and lifecycle event |
| `fina-olap.group_sensitivities` | Trade population and `reprice` dependency result | Grouped sensitivity rows with query inputs recorded |

The scheduler implementation exposes dependency results with `runtime.result(process_id, thread_name)`. Use that rather than reading another handler’s mutable closure. The adapter bridges `TradeRepository` event envelopes into the scheduler `EventBus`.

## MCP boundary

The scheduler is transport-neutral. The configured MCP client may not have FinA servers registered. When using MCP, start or connect the scheduler server at the single entrance and inject a pricer client into the pricing adapter. The repository-local scheduler tools are `process_submit`, `scheduler_query`, `scheduler_command`, `pubsub_subscribe`, and `pubsub_publish`; the pricer server exposes `pricing_and_sensitivity`, scenario tools, `olap_query`, and GCS tools. Do not claim MCP wiring is live unless the client registry or a real tool call proves it.

## Verification checklist

Run:

```bash
pytest -q python/tests/test_process_scheduler.py python/tests/test_process_wired.py
python examples/e2e_wired_verify.py
```

Assert all of the following:

- process state is `FINISHED`;
- initial quote thread is `FINISHED`;
- pricing call count is exactly two;
- trade status is `AMENDED`;
- `trade.lifecycle.amended` appears in the durable trade event history;
- reprice was activated by subscription and is `FINISHED`;
- OLAP is `FINISHED` and its rows derive from the reprice result;
- every task result is JSON-serializable and contains the process correlation ID where applicable.

On failure, report the failed task, its full lambda context keys (never secrets), the error, and whether the lifecycle event was published. Never replace a real tool failure with mocked pricing output in an E2E pass.
