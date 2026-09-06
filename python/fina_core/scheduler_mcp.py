"""MCP façade for the FinA process scheduler.

Run ``python -m fina_core.scheduler_mcp`` for stdio. For streamable HTTP,
import ``app`` into an ASGI server (for example ``uvicorn fina_core.scheduler_mcp:app``).
The MCP dependency is optional so the core package remains lightweight.
"""
from __future__ import annotations

import os
from typing import Any, Dict, Optional
from .process_scheduler import Event, SchedulerService, render_parameters

try:
    from mcp.server.fastmcp import FastMCP
except ImportError:  # pragma: no cover
    FastMCP = None  # type: ignore

service = SchedulerService()


def get_service() -> SchedulerService:
    """Return the process-wide scheduler state for this long-running runtime."""
    return service

if FastMCP is not None:
    mcp = FastMCP("fina-core-scheduler")

    @mcp.tool()
    def process_submit(definition: Dict[str, Any], parameters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Submit a FinaProcess definition and return its execution instance."""
        return {"process": service.snapshot(service.create_process(render_parameters(definition, parameters or {})).id)[0]}

    @mcp.tool()
    def scheduler_query(process_id: Optional[str] = None) -> Dict[str, Any]:
        """Return process and thread execution state."""
        return {"processes": service.snapshot(process_id)}

    @mcp.tool()
    def scheduler_command(command: Dict[str, Any]) -> Dict[str, Any]:
        """Send pause, resume, cancel, or publish command."""
        return service.command(command)

    @mcp.tool()
    def pubsub_subscribe(topic: str, subscriber_id: str) -> Dict[str, Any]:
        """Register a named no-op subscription; runtime handlers use the same bus API."""
        service.bus.subscribe(topic, subscriber_id, lambda _event: None)
        return {"topic": topic, "subscriber_id": subscriber_id, "subscribed": True}

    @mcp.tool()
    def pubsub_publish(topic: str, payload: Dict[str, Any], process_id: Optional[str] = None, thread_id: Optional[str] = None) -> Dict[str, Any]:
        """Publish a typed event to matching subscribers."""
        return service.command({"command": "publish", "topic": topic, "payload": payload, "process_id": process_id, "thread_id": thread_id})

    @mcp.prompt()
    def fina_core_scheduler_guidance() -> str:
        return "Use process_submit for a FinaProcess, scheduler_query for immutable execution snapshots, scheduler_command for control, and pubsub_publish for lifecycle-driven work. A process is the execution instance; each thread is independently schedulable."

    app = mcp.streamable_http_app()
else:
    mcp = None
    app = None


def main() -> None:
    if mcp is None:
        raise SystemExit("Install the optional MCP dependency: pip install 'fina-core[mcp]'")
    # Keep one service object alive for the lifetime of the daemon. Use
    # FINA_SCHEDULER_TRANSPORT=streamable-http for the shared HTTP instance;
    # stdio remains a single-session daemon with the same process-wide state.
    mcp.run(transport=os.environ.get("FINA_SCHEDULER_TRANSPORT", "stdio"))


if __name__ == "__main__":
    main()
