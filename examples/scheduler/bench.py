#!/usr/bin/env python3
"""Scheduler maximum-throughtput benchmark.

Measures how many tasks the fina scheduler can take to a terminal state in
a fixed wall-clock window (default 60 s). Three submission strategies answer
slightly different questions:

  bounded  (default)  keep ``--max-inflight`` tasks outstanding. The queue is
                      always non-empty, so completed/sec during the window is
                      the kernel's steady-state task consumption rate with a
                      bounded registry.
  stream              submit as fast as the driver can for the whole window and
                      report completed/sec at the deadline. The registry grows
                      without bound, so the number also absorbs the snapshot
                      growth cost (see BENCHMARK.md, "known bottlenecks").
  burst               submit ``--count`` tasks as fast as possible, then drain;
                      tasks/sec covers submit + drain (a single finite batch).

Two submission transports and two hooks let you attribute where time goes:

  --submit batch   (default) inject tasks with the ``restore`` command, 2000
                   (or ``--batch-size``) per JSON command — the kernel path.
  --submit fanout  one ``start`` PyO3 call per task — the per-task Python path.
  --hook auto      (default) native AutoFinishHook: no Python in the hot loop.
  --hook python    a Python ``TaskHook`` that finishes every task from
                   ``on_start`` via ``scheduler.cmd`` (GIL-bound worker path).

Run from the repo root after installing the package:

    pip install -e .
    python examples/scheduler/bench.py --duration 60
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from typing import Any, Dict, List, Optional

import fina
from fina.scheduler import Scheduler, TaskHook


class FinishOnStart(TaskHook):
    """Python worker that completes every task from inside ``on_start``.

    The kernel's ``PyHook`` gives a Python hook no notifier handle, so a
    Python worker drives its task through ``scheduler.cmd`` — exactly the
    documented pattern. This measures the realistic per-task cost of the
    GIL-bound hook + a second command round-trip.
    """

    def __init__(self) -> None:
        self._sched: Optional[Scheduler] = None

    def set_scheduler(self, sched: Scheduler) -> None:
        self._sched = sched

    def on_start(self, task: Dict[str, Any]) -> None:
        assert self._sched is not None
        self._sched.cmd({"cmd": "finish", "id": task["id"]})


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--workers", type=int, default=256, help="concurrency slots")
    p.add_argument("--duration", type=float, default=60.0, help="measurement window (s)")
    p.add_argument(
        "--mode",
        choices=["bounded", "stream", "burst"],
        default="bounded",
        help="submission strategy (see module docstring)",
    )
    p.add_argument("--max-inflight", type=int, default=10_000, help="tasks outstanding (bounded)")
    p.add_argument(
        "--max-total",
        type=int,
        default=1_000_000,
        help="safety cap on total submissions (keeps registry + snapshots from OOM-ing the box)",
    )
    p.add_argument("--count", type=int, default=200_000, help="total tasks (burst)")
    p.add_argument("--batch-size", type=int, default=2_000, help="tasks per restore command")
    p.add_argument(
        "--submit",
        choices=["batch", "fanout"],
        default="batch",
        help="batch=restore commands, fanout=one start() call per task",
    )
    p.add_argument("--hook", choices=["auto", "python"], default="auto")
    p.add_argument("--drain", action="store_true", help="wait for terminal after the window")
    p.add_argument("--sample-every", type=float, default=5.0, help="progress sample interval (s)")
    p.add_argument("--drain-timeout", type=float, default=120.0, help="max drain wait (s)")
    p.add_argument("--quiet", action="store_true", help="only print the final summary table")
    return p.parse_args()


# ---------------------------------------------------------------------------
# Submission helpers
# ---------------------------------------------------------------------------

def submit_batch(sched: Scheduler, submit: str, start_id: int, n: int) -> None:
    """Fire ``n`` tasks beginning at ``start_id`` via the chosen transport."""
    if submit == "fanout":
        for i in range(start_id, start_id + n):
            sched.start(f"bench/{i}", info={"i": i})
        return
    tasks = [{"id": f"bench/{i}", "priority": 0, "info": {"i": i}} for i in range(start_id, start_id + n)]
    sched.cmd({"cmd": "restore", "tasks": tasks})


class Counter:
    """TTL-cached per-state counters from the cheap native ``scheduler_count``."""

    def __init__(self, sched: Scheduler, ttl: float = 0.05):
        self.sched = sched
        self.ttl = ttl
        self._at = 0.0
        self._c: Dict[str, int] = {}

    def _fresh(self) -> None:
        if time.monotonic() - self._at >= self.ttl:
            self._c = self.sched.counts()
            self._at = time.monotonic()

    def __getitem__(self, key: str) -> int:
        self._fresh()
        return self._c[key]

    def refresh(self) -> Dict[str, int]:
        self._c = self.sched.counts()
        self._at = time.monotonic()
        return self._c

    def terminal(self) -> int:
        return self["finished"] + self["killed"]

    def inflight(self) -> int:
        return self["pending"] + self["running"]


class Stopwatch:
    def __init__(self):
        self.t0 = time.monotonic()

    def elapsed(self) -> float:
        return time.monotonic() - self.t0


def terminal_count(sched: Scheduler) -> int:
    c = sched.counts()
    return c["finished"] + c["killed"]


# ---------------------------------------------------------------------------
# The three modes
# ---------------------------------------------------------------------------

def run_bounded(
    sched: Scheduler,
    args: argparse.Namespace,
    sw: Stopwatch,
    counter: Counter,
) -> Dict[str, int]:
    """Maintain in-flight at ``--max-inflight``; report completed/sec at the end.

    Before each decision the driver first syncs with the actor: it waits until
    every task it has *submitted* is *registered* in the snapshot (mailbox
    caught up), so the in-flight window is an exact bound rather than a stale
    overshoot — otherwise a flood of queued commands measures mailbox backlog,
    not scheduler throughput.
    """
    submitted = 0
    next_id = 0
    sampled_until = 0.0
    while sw.elapsed() < args.duration and submitted < args.max_total:
        c = counter.refresh()
        while submitted > (c["pending"] + c["running"] + c["finished"] + c["killed"]):
            if sw.elapsed() >= args.duration or submitted >= args.max_total:
                break
            time.sleep(0.001)
            c = counter.refresh()
        if sw.elapsed() >= sampled_until:
            rate = c["finished"] / max(sw.elapsed(), 1e-9)
            if not args.quiet:
                print(
                    f"  [t={sw.elapsed():5.1f}s] submitted={submitted:>10,}"
                    f"  finished={c['finished']:>10,}  in_flight={c['pending'] + c['running']:>7,}"
                    f"  rate={rate:,.0f}/s",
                    flush=True,
                )
            sampled_until = sw.elapsed() + args.sample_every
        headroom = args.max_inflight - (c["pending"] + c["running"])
        if headroom > 0:
            batch = min(headroom, args.batch_size, args.max_total - submitted)
            submit_batch(sched, args.submit, next_id, batch)
            submitted += batch
            next_id += batch
        else:
            time.sleep(0.001 if args.hook == "auto" else 0.005)
    return {"submitted": submitted}


def run_stream(
    sched: Scheduler,
    args: argparse.Namespace,
    sw: Stopwatch,
    counter: Counter,
) -> Dict[str, int]:
    """Submit as fast as possible for the whole window (registry grows).

    The registry also accumulates *finished* tasks, and the snapshot rebuild is
    O(registry) per event, so an unbounded ``stream`` run eventually wedges the
    actor and can OOM the box. ``--max-total`` (default 1e6) caps total
    submissions; the reported rate then absorbs the growth cost up to that cap.
    """
    submitted = 0
    next_id = 0
    sampled_until = 0.0
    while sw.elapsed() < args.duration:
        if sw.elapsed() >= sampled_until:
            c = counter.refresh()
            if not args.quiet:
                print(
                    f"  [t={sw.elapsed():5.1f}s] submitted={submitted:>10,}"
                    f"  finished={c['finished']:>10,}  in_flight={c['pending'] + c['running']:>7,}"
                    f"  rate={c['finished'] / max(sw.elapsed(), 1e-9):,.0f}/s",
                    flush=True,
                )
            sampled_until = sw.elapsed() + args.sample_every
        if submitted < args.max_total:
            batch = min(args.batch_size, args.max_total - submitted)
            submit_batch(sched, args.submit, next_id, batch)
            submitted += batch
            next_id += batch
        else:
            time.sleep(0.002)
    return {"submitted": submitted}


def run_burst(
    sched: Scheduler,
    args: argparse.Namespace,
    sw: Stopwatch,
    counter: Counter,
) -> Dict[str, int]:
    """Submit ``--count`` tasks, then drain; report tasks/sec over submit+drain."""
    submitted = 0
    next_id = 0
    while submitted < args.count:
        batch = min(args.batch_size, args.count - submitted)
        submit_batch(sched, args.submit, next_id, batch)
        submitted += batch
        next_id += batch
    if not args.quiet:
        print(f"  submitted {submitted:,} tasks in {sw.elapsed():.2f}s; draining…", flush=True)
    return {"submitted": submitted}


# ---------------------------------------------------------------------------
# Drain + report
# ---------------------------------------------------------------------------

def drain(sched: Scheduler, submitted: int, timeout_s: float) -> float:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if terminal_count(sched) >= submitted:
            return time.monotonic()
        time.sleep(0.05)
    return time.monotonic()


def main() -> int:
    args = parse_args()
    hook: Optional[TaskHook] = FinishOnStart() if args.hook == "python" else None

    header: Dict[str, object] = {
        "mode": args.mode,
        "workers": args.workers,
        "duration_s": args.duration,
        "max_inflight": args.max_inflight,
        "submit": args.submit,
        "hook": args.hook,
    }

    sw = Stopwatch()
    with Scheduler(workers=args.workers, hook=hook) as sched:
        if isinstance(hook, FinishOnStart):
            hook.set_scheduler(sched)
        counter = Counter(sched)
        if not args.quiet:
            print(f"fina scheduler benchmark  {json.dumps(header)}", flush=True)

        if args.mode == "bounded":
            info = run_bounded(sched, args, sw, counter)
        elif args.mode == "stream":
            info = run_stream(sched, args, sw, counter)
        else:
            info = run_burst(sched, args, sw, counter)

        submitted = info["submitted"]
        window = sw.elapsed()
        c = counter.refresh()
        finished_at_window = c["finished"]
        killed_at_window = c["killed"]
        if not args.quiet:
            print(
                f"  [t={window:5.1f}s] window end: submitted={submitted:>10,}"
                f" finished={finished_at_window:>10,} killed={killed_at_window:>7,}",
                flush=True,
            )

        drained = 0.0
        if args.drain or args.mode == "burst":
            drained = drain(sched, submitted, args.drain_timeout)
            if not args.quiet:
                print(f"  drained in {drained - sw.t0:.2f}s", flush=True)

        if args.hook == "python" and not (args.drain or args.mode == "burst"):
            # give GIL-bound worker threads a chance to finish before the
            # scheduler is closed: with a hook still holding the GIL at
            # interpreter shutdown the process can hang (see docs).
            drain(sched, submitted, args.drain_timeout)
        if args.hook == "python":
            time.sleep(0.5)

        c = counter.refresh()
        finished_total = c["finished"]

    elapsed = sw.elapsed()
    window_rate = finished_at_window / max(window, 1e-9)
    total_rate = submitted / max(elapsed, 1e-9)  # submit+drain wall

    print("\n=== scheduler throughput summary ===")
    print(f"  mode            : {args.mode}  (workers={args.workers}, submit={args.submit}, hook={args.hook})")
    print(f"  window          : {window:.2f}s")
    print(f"  submitted       : {submitted:,}")
    print(f"  finished @window: {finished_at_window:,}  ({window_rate:,.0f} tasks/sec)")
    print(f"  in_flight @win  : {c['pending'] + c['running']:,} (pending {c['pending']:,}, running {c['running']:,})")
    if args.drain or args.mode == "burst":
        print(f"  finished (all)  : {finished_total:,} in {elapsed:.2f}s  ({total_rate:,.0f} tasks/sec incl. drain)")
    if killed_at_window:
        print(f"  killed          : {killed_at_window:,}")
    print("-" * 52)

    result = dict(header)
    result.update(
        {
            "window_s": round(window, 3),
            "submitted": submitted,
            "finished_at_window": finished_at_window,
            "tasks_per_sec": round(window_rate),
            "in_flight_pending": c["pending"],
            "in_flight_running": c["running"],
        }
    )
    if args.drain or args.mode == "burst":
        result["tasks_per_sec_total"] = round(total_rate)
    print("summary_json: " + json.dumps(result))

    # a fully-bottlenecked run (nothing completed) is a failed benchmark
    return 0 if finished_at_window > 0 else 2


if __name__ == "__main__":
    sys.exit(main())