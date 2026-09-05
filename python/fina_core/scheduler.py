"""Scheduler bindings (docs/scheduler.md).

The Rust core exposes a small, direct bridge:

* ``_core.scheduler_new(workers, hook=None)``  -> integer handle
* ``_core.scheduler_cmd(handle, command_json)``-> fire a JSON command
* ``_core.scheduler_query(handle)``            -> JSON snapshot of all tasks
* ``_core.scheduler_close(handle)``            -> stop & release the scheduler
* ``_core.etl_scheduler_run(yaml, workers, retries, poll_ms)`` -> one-shot run
* ``_core.expand_etl_config(yaml, retries)``   -> scheduler task plan (JSON)

This module wraps those into objects and helpers:

* :class:`Scheduler`  — a live, stateful scheduler (like ``orjson`` on the JSON
  side, here the kernel is an actix actor; see the Rust side). ``cmd`` accepts
  either a JSON ``str`` or a Python object.
* :class:`TaskHook`   — base class for Python hooks. Override the lifecycle
  callbacks; they receive a task ``dict``. A Python hook thread can transition a
  task by calling ``scheduler.cmd(...)`` (the id is in the task dict).
* :func:`run_pipelines_scheduled` — convenience one-shot runner over an ETL
  config ``execution:`` block.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Union

from . import _core

__all__ = [
    "Scheduler",
    "TaskHook",
    "TaskInfo",
    "run_pipelines_scheduled",
    "TaskState",
]


class TaskState:
    """Lifecycle states of a scheduler task (mirrors the Rust ``TaskState``)."""

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    PAUSED = "PAUSED"
    FINISHED = "FINISHED"
    KILLED = "KILLED"

    _TERMINAL = {FINISHED, KILLED}

    @classmethod
    def is_terminal(cls, state: str) -> bool:
        return state in cls._TERMINAL


@dataclass
class TaskInfo:
    """A snapshot row returned by :meth:`Scheduler.query`."""

    id: str
    state: str
    priority: int = 0
    info: Any = None
    checkpoint: Any = None
    result: Any = None
    error: Optional[str] = None
    attempts: int = 0
    max_attempts: int = 0

    #: human timestamps, epoch millis
    created_ms: int = 0
    last_started_ms: Optional[int] = None
    finished_ms: Optional[int] = None

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "TaskInfo":
        d = dict(d)
        return cls(
            id=d.pop("id", ""),
            state=d.pop("state", ""),
            priority=d.pop("priority", 0),
            info=d.pop("info", None),
            checkpoint=d.pop("checkpoint", None),
            result=d.pop("result", None),
            error=d.pop("error", None),
            attempts=d.pop("attempts", 0),
            max_attempts=d.pop("max_attempts", 0),
            created_ms=d.pop("created_ms", 0),
            last_started_ms=d.pop("last_started_ms", None),
            finished_ms=d.pop("finished_ms", None),
        )

    @property
    def terminal(self) -> bool:
        return TaskState.is_terminal(self.state)


class TaskHook:
    """Base class for Python scheduler hooks.

    Subclass and override whichever lifecycle callbacks you care about. Each
    receives a task ``dict`` snapshot (see ``TaskInfo.from_dict``). The kernel
    invokes hooks on the task's own OS thread; a Python hook that needs to
    resolve a task can call ``scheduler.cmd(...)`` (e.g. ``kill`` or
    ``reschedule``) using the ``id`` from the snapshot — the Python-side handle
    is reachable via closure.
    """

    def on_start(self, task: Dict[str, Any]) -> None:
        """First dispatch of the task."""

    def on_pause(self, task: Dict[str, Any], checkpoint: Any) -> None:
        """Task was paused; ``checkpoint`` may carry state."""

    def on_resume(self, task: Dict[str, Any], checkpoint: Any) -> None:
        """Task resumed from pause / restore."""

    def on_finish(self, task: Dict[str, Any]) -> None:
        """Task finished successfully; ``task`` carries ``result``."""

    def on_kill(self, task: Dict[str, Any], reason: Optional[str]) -> None:
        """Task was killed; ``reason`` may explain."""

    def on_reschedule(self, task: Dict[str, Any], reason: str) -> None:
        """Task was soft-failed and retried; ``reason`` explains."""


class Scheduler:
    """A live scheduler over the Rust actix kernel.

    Parameters
    ----------
    workers:
        Concurrency slots (tasks running at the same time), >= 1.
    hook:
        Optional ``TaskHook`` instance. Without one the built-in
        ``AutoFinishHook`` runs tasks to completion instantly (state-machine
        smoke testing).
    """

    def __init__(self, workers: int = 1, hook: Optional[TaskHook] = None):
        self._handle: Optional[int] = None
        self.workers = max(1, int(workers))
        self._hook = hook
        self._handle = _core.scheduler_new(self.workers, hook)

    # -- lifecycle ---------------------------------------------------------
    def close(self) -> None:
        """Stop the scheduler and release its native handle."""
        if self._handle is not None:
            _core.scheduler_close(self._handle)
            self._handle = None

    def __enter__(self) -> "Scheduler":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def __del__(self) -> None:  # pragma: no cover - destructor fallback
        try:
            self.close()
        except Exception:  # noqa: BLE001 - interpreter is shutting down
            pass

    # -- commands ------------------------------------------------------------
    def cmd(self, command: Union[str, Dict[str, Any]]) -> None:
        """Fire a scheduler command (fire-and-forget).

        Either a JSON string or a Python object::

            sched.cmd({"cmd": "start", "id": "t1", "priority": 1,
                       "info": {"x": 1}, "max_attempts": 3})
            sched.cmd({"cmd": "reschedule", "id": "t1", "reason": "network"})
            sched.cmd({"cmd": "restore", "tasks": [...]})
            sched.cmd({"cmd": "checkpoint", "id": "t1", "value": {"o": 42}})
            sched.cmd({"cmd": "set_slots", "slots": 4})
            sched.cmd({"cmd": "kill", "id": "t1", "reason": "hard fail"})
        """
        if self._handle is None:
            raise RuntimeError("scheduler is closed")
        if not isinstance(command, str):
            command = json.dumps(command)
        _core.scheduler_cmd(self._handle, command)

    def start(
        self,
        task_id: str,
        *,
        info: Any = None,
        priority: int = 0,
        max_attempts: int = 0,
    ) -> None:
        """Start a task."""
        self.cmd(
            {
                "cmd": "start",
                "id": task_id,
                "priority": priority,
                "info": info,
                "max_attempts": max_attempts,
            }
        )

    # -- queries -------------------------------------------------------------
    def query(self) -> List[TaskInfo]:
        """Snapshot of every task (submission order)."""
        if self._handle is None:
            raise RuntimeError("scheduler is closed")
        raw = _core.scheduler_query(self._handle)
        if isinstance(raw, str):
            raw = json.loads(raw)
        return [TaskInfo.from_dict(d) for d in raw]

    def counts(self) -> Dict[str, int]:
        """Cheap per-state counters ``{pending, running, paused, finished,
        killed}`` (scans the native snapshot without materializing task JSON).
        """
        if self._handle is None:
            raise RuntimeError("scheduler is closed")
        raw = _core.scheduler_count(self._handle)
        if isinstance(raw, str):
            raw = json.loads(raw)
        return {k: int(v) for k, v in raw.items()}

    def state(self, task_id: str) -> Optional[TaskInfo]:
        """Snapshot of one task or ``None`` if the id is unknown."""
        for t in self.query():
            if t.id == task_id:
                return t
        return None

    def wait(
        self,
        task_ids: List[str],
        *,
        timeout_ms: float = 60_000.0,
        poll_ms: float = 25.0,
    ) -> List[TaskInfo]:
        """Block until every ``task_ids`` reaches a terminal state.

        Raises ``RuntimeError`` if any task is KILLED or the deadline passes.
        """
        import time

        deadline = time.monotonic() + timeout_ms / 1000.0
        wanted = set(task_ids)
        while True:
            snap = {t.id: t for t in self.query()}
            missing = wanted - set(snap)
            done = [snap[i] for i in wanted if i in snap and snap[i].terminal]
            killed = [t for t in done if t.state == TaskState.KILLED]
            if killed:
                raise RuntimeError(
                    f"task(s) killed: {[(t.id, t.error) for t in killed]}"
                )
            if not missing and len(done) == len(wanted):
                return done
            if time.monotonic() >= deadline:
                raise RuntimeError(f"wait() timed out; missing={sorted(missing)}")
            time.sleep(poll_ms / 1000.0)


@dataclass
class ScheduledRun:
    """Aggregated outcome of :func:`run_pipelines_scheduled`."""

    ok: bool
    elapsed_ms: int
    slots: int
    timing: List[Dict[str, Any]]
    datasets: Dict[str, int]
    tasks: List[TaskInfo]
    errors: List[Any] = None  # type: ignore[assignment]

    #: ``str`` -> row counts, same shape as ``EtlResult.rows``.
    @property
    def rows(self) -> Dict[str, int]:
        return self.datasets


def run_pipelines_scheduled(
    config: Union[str, bytes],
    *,
    workers: int = 1,
    retries: int = 2,
    poll_ms: int = 100,
) -> ScheduledRun:
    """Run a whole ETL config through an in-process scheduler (one shot).

    ``config`` is the YAML text (or bytes) of the official ETL schema with an
    ``execution:`` block (see docs/scheduler.md). Stages run serially, groups
    inside a stage in parallel, fan-out nodes split into one task per partition.
    Soft failures are retried up to ``retries`` times; the first hard failure
    kills the run. ``workers`` bounds concurrent tasks.
    """
    if isinstance(config, bytes):
        config = config.decode("utf-8")
    raw = _core.etl_scheduler_run(config, workers, retries, poll_ms)
    if isinstance(raw, str):
        raw = json.loads(raw)
    return ScheduledRun(
        ok=bool(raw.get("ok")),
        elapsed_ms=int(raw.get("elapsed_ms", 0)),
        slots=int(raw.get("slots", workers)),
        timing=list(raw.get("timing", [])),
        datasets=dict(raw.get("datasets", {})),
        tasks=[TaskInfo.from_dict(d) for d in raw.get("tasks", [])],
        errors=list(raw.get("errors", [])),
    )