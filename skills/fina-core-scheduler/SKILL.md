---
name: fina-core-scheduler
description: Execute and observe Fina v1 process definitions as OS-style process instances with schedulable threads, commands, notifications, and typed pub/sub lifecycle events.
---
# FinA Core Scheduler

Use this skill for orchestration. The YAML root is `kind: FinaProcess`; the submitted document creates one execution instance. A process is an execution boundary and each `threads` entry is an independently schedulable unit. This is an analogy, not an operating-system process.

## Canonical contract

The source of truth is `skills/fina-core-scheduler/schema/fina-process.schema.json`; the ETL adapter contract is next to it at `skills/fina-core-scheduler/schema/etl.schema.json`. Every process must declare `api_version: fina/v1`, `metadata.name`, `parameters`, and at least one thread. Each thread declares `name`, `handler`, optional `depends_on`, parameters, priority, and retry policy. Parameters are repeatability inputs such as `eval_datetime`, `market_data_date`, `scenario_id`, and `trade_repository_snapshot_datetime`; `${parameter}` placeholders are resolved at submission.

```yaml
api_version: fina/v1
kind: FinaProcess
metadata: {name: fcn-rfq-to-risk, version: "1"}
parameters: {eval_datetime: "2027-05-18T08:00:00Z", scenario_id: base}
threads:
  - {name: quote, handler: fina-pricer.pricing_and_sensitivity}
  - {name: register_trade, handler: fina-trade.register, depends_on: [quote]}
  - {name: olap, handler: fina-olap.group_sensitivities, depends_on: [register_trade]}
subscriptions:
  - {topic: trade.lifecycle.amended, handler: fina-pricer.pricing_and_sensitivity, start_thread: reprice}
```

## Handler wiring: important limitation

`SchedulerService` resolves a handler from its in-process `register_handler(name, callable)` registry. The canonical wiring helper is `fina_core.integrations.register_fina_handlers`; it registers `fina-pricer.pricing_and_sensitivity`, `fina-trade.register`, `fina-trade.amend`, and `fina-olap.group_sensitivities`. The pricing callable may invoke the pricer MCP tool or direct `riskcube_mcp.sensitivity`; the scheduler itself is transport-neutral.

`fina-trade.amend` publishes a repository lifecycle envelope into the scheduler `EventBus` only after a successful mutation. A process subscription resolves `start_thread: reprice`, injects the event payload into that thread, and executes the registered pricing handler. The `olap` thread depends on `reprice`, so it runs after the event-triggered quote.

The separately executed fixture in `fina-pricer/data/attachment_sample.json` produced this real quote through `riskcube_mcp.sensitivity`: PV `824303.9345074219 USD`, PV standard error `353.61628746253473 USD`, notional `770000 USD`, price `107.05245902693792%`, and four RiskCube cells. The executable wired verification is `examples/e2e_wired_verify.py`; it calls the real pricer implementation twice, persists a real `TradeRepository` trade, publishes `trade.lifecycle.amended`, triggers the reprice thread, and performs grouped OLAP.

## MCP operations

The optional MCP server is `fina_core.scheduler_mcp`. It supports stdio (`python -m fina_core.scheduler_mcp`) and streamable HTTP (`uvicorn fina_core.scheduler_mcp:app`). Use `process_submit`, `scheduler_query`, `scheduler_command`, `pubsub_subscribe`, and `pubsub_publish`. Commands are formally shaped as `{command: pause|resume|cancel|publish, thread_id?, topic?, payload?}`. Events are `{topic, payload, event_id, process_id?, thread_id?}` and must use stable dot-separated topics, for example `trade.lifecycle.amended`.

Run one long-lived scheduler daemon for shared process/thread state. The module-level `SchedulerService` is created once per daemon and is reused by every HTTP request or stdio tool call handled by that process. For a shared remote endpoint, run `FINA_SCHEDULER_TRANSPORT=streamable-http fina-core-scheduler` behind a persistent host; do not use stateless serverless instances for scheduler state. A stdio deployment must keep its process alive for the full client session. If stdio and HTTP clients must share state, route both through the same long-lived daemon or add a durable scheduler state backend; separate short-lived processes do not share the in-memory service.

## Lifecycle and error rules

A dependent thread cannot run until every `depends_on` thread is `FINISHED`. A handler error is recorded as `FAILED` and is never silently converted to a successful quote. Lifecycle handlers should publish domain events after a durable trade mutation; a subscription may then spawn a re-pricing thread with the event payload. Keep event payloads JSON-serializable and never put credentials in parameters, events, logs, or results.

## Verification

Run the scheduler contract tests from the repository root with `pytest -q python/tests/test_process_scheduler.py python/tests/test_process_wired.py`. Run `examples/e2e_wired_verify.py` in an environment containing `fina-core`, `fina-trade`, and `fina-pricer`; it registers the real adapters, calls pricing twice, persists a trade, publishes an amendment event, triggers re-pricing, and groups the resulting sensitivities. Use the native ETL scheduler for existing `pipelines:` documents; use `FinaProcess` for cross-feature orchestration.

## MCP client evidence

The configured MCP client in this environment reports three servers: `manus-tools`, `slides`, and `workflow`. Their tools are built-in Manus/browser/media/WebDev tools, Slides authoring tools, and `workflow.run`, respectively. There is no configured `fina-core-scheduler` or `fina-pricer` MCP server in the client registry. The repository code contains a local optional scheduler MCP module and a separate pricer FastMCP server, but those are not the same as being registered in the current MCP client.
