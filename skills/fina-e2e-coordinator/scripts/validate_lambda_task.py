#!/usr/bin/env python3
"""Validate one JSON lambda-task declaration using only the standard library."""
from __future__ import annotations

import json
import sys
from pathlib import Path


def validate(value: object) -> list[str]:
    errors: list[str] = []
    if not isinstance(value, dict):
        return ["task must be an object"]
    for key in ("task", "handler", "context"):
        if key not in value:
            errors.append(f"missing required property: {key}")
    context = value.get("context")
    if not isinstance(context, dict):
        return errors + ["context must be an object"]
    for key in ("correlation_id", "inputs", "dependency_results", "event", "attempt"):
        if key not in context:
            errors.append(f"context missing required property: {key}")
    if not isinstance(context.get("correlation_id"), str):
        errors.append("context.correlation_id must be a string")
    if not isinstance(context.get("inputs"), dict):
        errors.append("context.inputs must be an object")
    if not isinstance(context.get("dependency_results"), dict):
        errors.append("context.dependency_results must be an object")
    if context.get("event") is not None and not isinstance(context.get("event"), dict):
        errors.append("context.event must be an object or null")
    if not isinstance(context.get("attempt"), int) or context.get("attempt", 0) < 1:
        errors.append("context.attempt must be an integer >= 1")
    return errors


def main() -> int:
    if len(sys.argv) != 2:
        print(f"usage: {Path(sys.argv[0]).name} TASK.json", file=sys.stderr)
        return 2
    try:
        value = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        print(f"invalid JSON: {exc}", file=sys.stderr)
        return 2
    errors = validate(value)
    if errors:
        for error in errors:
            print(error, file=sys.stderr)
        return 1
    print("valid lambda task")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
