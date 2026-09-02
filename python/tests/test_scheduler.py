"""Scheduler Python-bindings tests (docs/scheduler.md).

Run::

    cd python
    uvx maturin develop                # build _core into the active env
    python -m pytest tests -q
"""

from __future__ import annotations

import os
import pathlib
import time

import pytest

import fina
from fina.scheduler import Scheduler, TaskHook, TaskState

REPO = pathlib.Path(__file__).resolve().parents[2]
EXAMPLE = REPO / "examples" / "scheduler"


class Recorder(TaskHook):
    def __init__(self) -> None:
        self.events: list[tuple[str, str]] = []

    def on_start(self, task: dict) -> None:
        self.events.append(("start", task["id"]))

    def on_finish(self, task: dict) -> None:
        self.events.append(("finish", task["id"]))

    def on_kill(self, task: dict, reason) -> None:
        self.events.append(("kill", task["id"]))

    def on_reschedule(self, task: dict, reason: str) -> None:
        self.events.append(("resched", task["id"]))

    def on_pause(self, task: dict, checkpoint) -> None:
        self.events.append(("pause", task["id"]))

    def on_resume(self, task: dict, checkpoint) -> None:
        self.events.append(("resume", task["id"]))


def _wait_state(s: "Scheduler", task_id: str, *states: str, deadline: float = 5.0) -> None:
    t0 = time.monotonic()
    while time.monotonic() - t0 < deadline:
        t = s.state(task_id)
        if t is not None and t.state in states:
            return
        time.sleep(0.01)
    raise AssertionError(f"task {task_id} never reached {states}")


def test_scheduler_state_machine() -> None:
    rec = Recorder()
    with Scheduler(workers=2, hook=rec) as s:
        s.start("t1", info={"job": "x"}, max_attempts=2)
        _wait_state(s, "t1", TaskState.PENDING, TaskState.RUNNING)
        assert s.state("t1").state == TaskState.RUNNING  # type: ignore[union-attr]

        # checkpoint / update are no-ops on state but must be accepted
        s.cmd({"cmd": "checkpoint", "id": "t1", "value": {"offset": 1}})
        s.cmd({"cmd": "update", "id": "t1", "info": {"progress": 0.5}})
        time.sleep(0.05)

        s.cmd({"cmd": "kill", "id": "t1", "reason": "boom"})
        _wait_state(s, "t1", TaskState.KILLED)
        t = s.state("t1")
        assert t is not None and t.state == TaskState.KILLED
        assert t.error == "boom"

        # terminal tasks are not restarted by resume / restore
        s.cmd({"cmd": "resume", "id": "t1"})
        time.sleep(0.05)
        assert s.state("t1").state == TaskState.KILLED  # type: ignore[union-attr]

        kills = [e for e in rec.events if e[0] == "kill"]
        assert ("kill", "t1") in kills


def test_scheduler_auto_finish_and_slots() -> None:
    # AutoFinishHook finishes each task the moment it starts.
    with Scheduler(workers=1) as s:
        s.start("a")
        s.start("b")
        done = s.wait(["a", "b"])
        assert all(t.state == TaskState.FINISHED for t in done)
        s.cmd({"cmd": "set_slots", "slots": 4})
        time.sleep(0.05)


def test_expand_etl_config() -> None:
    from fina import _core

    plan = _core.expand_etl_config(_example_yaml(), retries=2)
    plan = fina.loads(plan)
    stages = plan["stages"]
    # 3 pipelines in the execution plan, regardless of fan-out expansion
    assert len(stages) == 3
    # last stage fans out by instrument name into <=10 partitions
    fanout = stages[2]
    assert 1 <= len(fanout) <= 10
    assert all(t["id"].startswith("2-instrumentsFanout/x/") for t in fanout)


def test_etl_scheduler_run() -> None:
    res = fina.run_pipelines_scheduled(_example_yaml(), workers=10, retries=2, poll_ms=5)
    assert res.ok, res.errors
    assert res.rows["market"] == 8
    assert res.rows["options"] == 9  # 7 instruments unwound into 9 legs
    assert res.rows["fanout"] == 9  # summed across partitions
    fanout_files = list((EXAMPLE / "out" / "fanout").glob("*.parquet")) if (
        EXAMPLE / "out" / "fanout"
    ).is_dir() else []
    assert len(fanout_files) == 7


def _example_yaml() -> str:
    text = (EXAMPLE / "pipelines.yml").read_text(encoding="utf-8")
    base = EXAMPLE.as_posix()
    text = text.replace("file://market.json", f"file://{base}/market.json")
    text = text.replace("file://instruments.json", f"file://{base}/instruments.json")
    text = text.replace("file://out/", f"file://{base}/out/")
    return text


@pytest.fixture(autouse=True)
def _clean_out():
    yield
    import shutil

    shutil.rmtree(EXAMPLE / "out", ignore_errors=True)


if __name__ == "__main__":
    test_scheduler_state_machine()
    test_scheduler_auto_finish_and_slots()
    test_expand_etl_config()
    test_etl_scheduler_run()
    print("ok")