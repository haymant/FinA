"""Process/thread scheduler contract for FinA v1.

This module is deliberately independent of the native ETL scheduler: it provides
an executable orchestration façade for YAML-defined processes, dynamic thread
spawning, commands, notifications, and typed pub/sub events.
"""
from __future__ import annotations

import copy
import re
import threading
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional


@dataclass
class Event:
    topic: str
    payload: Dict[str, Any]
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    process_id: Optional[str] = None
    thread_id: Optional[str] = None


class EventBus:
    def __init__(self) -> None:
        self._handlers: Dict[str, Dict[str, Callable[[Event], None]]] = {}
        self._lock = threading.RLock()

    def subscribe(self, topic: str, subscriber_id: str, handler: Callable[[Event], None]) -> None:
        with self._lock:
            self._handlers.setdefault(topic, {})[subscriber_id] = handler

    def unsubscribe(self, topic: str, subscriber_id: str) -> None:
        with self._lock:
            self._handlers.get(topic, {}).pop(subscriber_id, None)

    def publish(self, event: Event) -> int:
        with self._lock:
            handlers = list(self._handlers.get(event.topic, {}).values()) + list(self._handlers.get("*", {}).values())
        for handler in handlers:
            handler(copy.deepcopy(event))
        return len(handlers)


@dataclass
class ThreadInstance:
    id: str
    process_id: str
    name: str
    handler: str
    parameters: Dict[str, Any]
    depends_on: List[str] = field(default_factory=list)
    state: str = "PENDING"
    result: Any = None
    error: Optional[str] = None
    attempts: int = 0


@dataclass
class ProcessInstance:
    id: str
    name: str
    parameters: Dict[str, Any]
    state: str = "RUNNING"
    threads: Dict[str, ThreadInstance] = field(default_factory=dict)


Handler = Callable[[ThreadInstance, "SchedulerService"], Any]


class SchedulerService:
    """Small deterministic scheduler used by skills, tests, and the MCP façade."""
    def __init__(self, handlers: Optional[Dict[str, Handler]] = None) -> None:
        self.bus = EventBus()
        self.handlers = handlers or {}
        self.processes: Dict[str, ProcessInstance] = {}
        self.commands: List[Dict[str, Any]] = []
        self._lock = threading.RLock()

    def register_handler(self, name: str, handler: Handler) -> None:
        self.handlers[name] = handler

    def create_process(self, definition: Dict[str, Any], parameters: Optional[Dict[str, Any]] = None) -> ProcessInstance:
        params = dict(definition.get("parameters", {}))
        params.update(parameters or {})
        process_id = str(uuid.uuid4())
        metadata = definition.get("metadata", {})
        process = ProcessInstance(process_id, metadata.get("name", "process"), params)
        for spec in definition.get("threads", []):
            tid = process_id + "/" + spec["name"]
            merged = dict(spec.get("parameters", {}))
            merged.update(params)
            state = "WAITING" if spec.get("triggered_by") else "PENDING"
            process.threads[tid] = ThreadInstance(tid, process_id, spec["name"], spec["handler"], merged, list(spec.get("depends_on", [])), state=state)
        with self._lock:
            self.processes[process_id] = process
        for subscription in definition.get("subscriptions", []):
            topic = subscription["topic"]
            subscriber_id = process_id + "/subscription/" + topic
            self.bus.subscribe(topic, subscriber_id, self._subscription_handler(process, subscription))
        self._run_ready(process)
        return process

    def _subscription_handler(self, process: ProcessInstance, subscription: Dict[str, Any]) -> Callable[[Event], None]:
        def receive(event: Event) -> None:
            start_thread = subscription.get("start_thread")
            if not start_thread:
                return
            template = next((item for item in process.threads.values() if item.name == start_thread), None)
            if template is None:
                raise KeyError("subscription start_thread is not declared: " + start_thread)
            self.spawn_thread(
                process.id,
                start_thread,
                template.handler,
                parameters={"event": event.payload, "event_id": event.event_id},
                depends_on=[],
            )
        return receive

    def spawn_thread(
        self,
        process_id: str,
        name: str,
        handler: str,
        *,
        parameters: Optional[Dict[str, Any]] = None,
        depends_on: Optional[List[str]] = None,
    ) -> ThreadInstance:
        process = self.processes[process_id]
        base_id = process_id + "/" + name
        dormant = process.threads.get(base_id)
        if dormant is not None and dormant.state == "WAITING":
            dormant.parameters.update(parameters or {})
            dormant.state = "PENDING"
            self._run_ready(process)
            return dormant
        thread_id = base_id
        suffix = 2
        while thread_id in process.threads:
            thread_id = base_id + "#" + str(suffix)
            suffix += 1
        merged = dict(process.parameters)
        merged.update(parameters or {})
        thread = ThreadInstance(thread_id, process_id, name, handler, merged, list(depends_on or []))
        process.threads[thread_id] = thread
        self._run_ready(process)
        return thread

    def command(self, command: Dict[str, Any]) -> Dict[str, Any]:
        self.commands.append(copy.deepcopy(command))
        action = command.get("command") or command.get("cmd")
        if action == "publish":
            event = Event(command["topic"], command.get("payload", {}), process_id=command.get("process_id"), thread_id=command.get("thread_id"))
            return {"delivered": self.bus.publish(event), "event_id": event.event_id}
        if action in ("pause", "resume", "cancel"):
            thread = self._thread(command["thread_id"])
            thread.state = {"pause": "PAUSED", "resume": "PENDING", "cancel": "CANCELLED"}[action]
            if action == "resume": self._run_ready(self.processes[thread.process_id])
            return {"thread_id": thread.id, "state": thread.state}
        raise ValueError("unsupported command: " + str(action))

    def _thread(self, tid: str) -> ThreadInstance:
        for p in self.processes.values():
            if tid in p.threads: return p.threads[tid]
        raise KeyError(tid)

    def result(self, process_id: str, name: str) -> Any:
        """Return the latest result for a named thread in a process."""
        process = self.processes[process_id]
        matches = [thread for thread in process.threads.values() if thread.name == name and thread.state == "FINISHED"]
        if not matches:
            raise KeyError(process_id + "/" + name)
        return matches[-1].result

    def _run_ready(self, process: ProcessInstance) -> None:
        changed = True
        while changed:
            changed = False
            for thread in list(process.threads.values()):
                if thread.state != "PENDING" or any(process.threads.get(process.id + "/" + dep, ThreadInstance("", "", "", "", {})).state != "FINISHED" for dep in thread.depends_on):
                    continue
                handler = self.handlers.get(thread.handler)
                if handler is None:
                    thread.state, thread.result = "FINISHED", {"handler": thread.handler, "parameters": thread.parameters}
                else:
                    thread.state, thread.attempts = "RUNNING", thread.attempts + 1
                    try:
                        thread.result = handler(thread, self)
                        thread.state = "FINISHED"
                    except Exception as exc:  # deterministic terminal state for orchestration errors
                        thread.state, thread.error = "FAILED", str(exc)
                changed = True
                self.bus.publish(Event("fina.thread." + thread.state.lower(), {"thread_id": thread.id, "result": thread.result}, process_id=process.id, thread_id=thread.id))
        if all(t.state in ("FINISHED", "CANCELLED") for t in process.threads.values()): process.state = "FINISHED"

    def snapshot(self, process_id: Optional[str] = None) -> List[Dict[str, Any]]:
        processes = [self.processes[process_id]] if process_id else list(self.processes.values())
        return [{"id": p.id, "name": p.name, "state": p.state, "parameters": copy.deepcopy(p.parameters), "threads": [{"id": t.id, "name": t.name, "handler": t.handler, "state": t.state, "result": t.result, "error": t.error, "attempts": t.attempts} for t in p.threads.values()]} for p in processes]


def render_parameters(value: Any, parameters: Dict[str, Any]) -> Any:
    if isinstance(value, dict): return {k: render_parameters(v, parameters) for k, v in value.items()}
    if isinstance(value, list): return [render_parameters(v, parameters) for v in value]
    if isinstance(value, str):
        full = re.fullmatch(r"\$\{([A-Za-z0-9_.-]+)\}", value)
        if full: return parameters.get(full.group(1), value)
        return re.sub(r"\$\{([A-Za-z0-9_.-]+)\}", lambda m: str(parameters.get(m.group(1), m.group(0))), value)
    return value
