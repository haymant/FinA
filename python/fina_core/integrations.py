"""Concrete handler wiring used by the single-entry Fina coordinator.

Adapters are dependency-injected: the scheduler does not import a particular
MCP client or trade database. Pass a callable that invokes the real pricer tool,
a TradeRepository-compatible object, and an OLAP callable.
"""
from __future__ import annotations

from typing import Any, Callable, Dict

from .process_scheduler import Event, SchedulerService, ThreadInstance


def register_fina_handlers(
    scheduler: SchedulerService,
    *,
    pricing_callable: Callable[[Dict[str, Any]], Dict[str, Any]],
    trade_repository: Any,
    olap_callable: Callable[[Dict[str, Any]], Dict[str, Any]],
    risk_callable: Callable[[Dict[str, Any]], Dict[str, Any]] | None = None,
) -> None:
    """Register the canonical handler names used by FinaProcess YAML.

    ``risk_callable`` wires the fina-risk compute handlers
    (``fina-risk.pricing_and_sensitivity`` and ``fina-risk.risk_batch``); pass a
    callable that invokes the fina-risk MCP tool (``run_risk_task``) or the
    direct adapter. When omitted those handlers are not registered.
    """

    def price(thread: ThreadInstance, runtime: SchedulerService) -> Dict[str, Any]:
        request = dict(thread.parameters.get("pricing_request", {}))
        event = thread.parameters.get("event")
        if event:
            request["lifecycle_event"] = event
        quote = pricing_callable(request)
        return {"trade_id": thread.parameters.get("trade_id"), "quote": quote, "trigger": event}

    def register_trade(thread: ThreadInstance, runtime: SchedulerService) -> Dict[str, Any]:
        quote_result = runtime.result(thread.process_id, "quote")
        quote = quote_result.get("quote", quote_result)
        trade = dict(thread.parameters["trade"])
        trade["quote"] = quote
        return trade_repository.register(trade)

    def amend_trade(thread: ThreadInstance, runtime: SchedulerService) -> Dict[str, Any]:
        changes = dict(thread.parameters.get("changes", {}))
        return trade_repository.amend(thread.parameters["trade_id"], changes, thread.parameters.get("reason", "lifecycle event"))

    def grouped_olap(thread: ThreadInstance, runtime: SchedulerService) -> Dict[str, Any]:
        return olap_callable({"trades": list(trade_repository.trades.values()), "latest_reprice": runtime.result(thread.process_id, "reprice"), "parameters": thread.parameters})

    def olap_query(thread: ThreadInstance, runtime: SchedulerService) -> Dict[str, Any]:
        """Query the shared store, passing the latest upstream compute result when present."""
        payload: Dict[str, Any] = {"query": dict(thread.parameters.get("olap_query", {})), "parameters": thread.parameters}
        for upstream in ("risk", "reprice", "quote"):
            try:
                payload[upstream] = runtime.result(thread.process_id, upstream)
                break
            except KeyError:
                continue
        return olap_callable(payload)

    scheduler.register_handler("fina-pricer.pricing_and_sensitivity", price)
    scheduler.register_handler("fina-trade.register", register_trade)
    scheduler.register_handler("fina-trade.amend", amend_trade)
    scheduler.register_handler("fina-olap.group_sensitivities", grouped_olap)
    scheduler.register_handler("fina-olap.olap_query", olap_query)

    if risk_callable is not None:

        def risk_single(thread: ThreadInstance, runtime: SchedulerService) -> Dict[str, Any]:
            request = dict(thread.parameters.get("pricing_request", {}))
            event = thread.parameters.get("event")
            if event:
                request["lifecycle_event"] = event
            risk_request = dict(thread.parameters.get("risk_request", {}))
            risk_request.update(request)
            return risk_callable(risk_request)

        def risk_batch(thread: ThreadInstance, runtime: SchedulerService) -> Dict[str, Any]:
            request = dict(thread.parameters.get("risk_request", thread.parameters))
            request.setdefault("mode", "batch")
            if request.pop("use_compiled", False):
                compiled = runtime.result(thread.process_id, "compile")
                request["requests"] = compiled.get("requests", [])
            return risk_callable(request)

        def etl_augment(thread: ThreadInstance, runtime: SchedulerService) -> Dict[str, Any]:
            return risk_callable(
                {
                    "mode": "augment",
                    "termsheet": thread.parameters.get("termsheet"),
                    "count": int(thread.parameters.get("count", 10)),
                    "seed": int(thread.parameters.get("seed", 20260909)),
                }
            )

        def etl_compile(thread: ThreadInstance, runtime: SchedulerService) -> Dict[str, Any]:
            return risk_callable(
                {
                    "mode": "compile",
                    "termsheet": thread.parameters.get("termsheet"),
                    "count": int(thread.parameters.get("count", 10)),
                    "seed": int(thread.parameters.get("seed", 20260909)),
                }
            )

        scheduler.register_handler("fina-risk.pricing_and_sensitivity", risk_single)
        scheduler.register_handler("fina-risk.risk_batch", risk_batch)
        scheduler.register_handler("fina-etl.augment_termsheet", etl_augment)
        scheduler.register_handler("fina-etl.compile_pricing_requests", etl_compile)

    if hasattr(trade_repository, "publish"):
        trade_repository.publish = lambda event: scheduler.bus.publish(
            Event(event["topic"], event.get("payload", {}), event_id=event.get("event_id", ""), thread_id=event.get("trade_id"))
        )
