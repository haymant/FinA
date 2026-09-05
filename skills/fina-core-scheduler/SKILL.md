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

`handler: fina-pricer.pricing_and_sensitivity` is currently a **symbolic handler name**, not an automatic cross-server invocation. `SchedulerService` resolves a handler only from its in-process `register_handler(name, callable)` registry. The scheduler MCP façade does not discover or invoke another MCP server. To make this thread live, an adapter must register that name and call the pricer MCP tool with a complete `PricingRequest`; the adapter then returns the tool response as the thread result. The real pricer server currently exposes `pricing_and_sensitivity(request)` plus scenario and OLAP tools in `fina-pricer/src/riskcube_mcp/server.py`.

The separately executed fixture in `fina-pricer/data/attachment_sample.json` produced this real quote through `riskcube_mcp.sensitivity`: PV `824303.9345074219 USD`, PV standard error `353.61628746253473 USD`, notional `770000 USD`, price `107.05245902693792%`, and four RiskCube cells. This is evidence of the pricer result, not evidence that the scheduler currently called it.

## MCP operations

The optional MCP server is `fina_core.scheduler_mcp`. It supports stdio (`python -m fina_core.scheduler_mcp`) and streamable HTTP (`uvicorn fina_core.scheduler_mcp:app`). Use `process_submit`, `scheduler_query`, `scheduler_command`, `pubsub_subscribe`, and `pubsub_publish`. Commands are formally shaped as `{command: pause|resume|cancel|publish, thread_id?, topic?, payload?}`. Events are `{topic, payload, event_id, process_id?, thread_id?}` and must use stable dot-separated topics, for example `trade.lifecycle.amended`.

## Lifecycle and error rules

A dependent thread cannot run until every `depends_on` thread is `FINISHED`. A handler error is recorded as `FAILED` and is never silently converted to a successful quote. Lifecycle handlers should publish domain events after a durable trade mutation; a subscription may then spawn a re-pricing thread with the event payload. Keep event payloads JSON-serializable and never put credentials in parameters, events, logs, or results.

## Verification

Run the scheduler contract tests from the repository root with `pytest -q python/tests/test_process_scheduler.py`. For an end-to-end run, register explicit adapters for `fina-pricer.pricing_and_sensitivity`, `fina-trade.register`, and `fina-olap.group_sensitivities`; submit a process containing quote, register, amend-event reprice, and OLAP threads; then assert all expected states/results and event delivery count. Use the native ETL scheduler for existing `pipelines:` documents; use `FinaProcess` for cross-feature orchestration.

## MCP client evidence

The configured MCP client in this environment reports three servers: `manus-tools`, `slides`, and `workflow`. Their tools are built-in Manus/browser/media/WebDev tools, Slides authoring tools, and `workflow.run`, respectively. There is no configured `fina-core-scheduler` or `fina-pricer` MCP server in the client registry. The repository code contains a local optional scheduler MCP module and a separate pricer FastMCP server, but those are not the same as being registered in the current MCP client.
