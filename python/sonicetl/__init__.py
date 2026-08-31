"""sonicetl — a native whole-ETL Python library backed by a Rust core (sonic-rs).

sonicetl is designed like orjson: the heavy lifting happens in a compiled Rust
extension (``sonicetl._core``) while the Python layer exposes a clean, small API.

Public surface
--------------
* ``loads`` / ``dumps`` — orjson-style JSON codecs powered by sonic-rs.
* ``run`` / ``run_etl`` — run a whole ETL described by the official ETL YAML
  schema; each record is parsed natively with sonic-rs and the extracted fields
  are streamed to Parquet (one ``<dataset>.parquet`` per dataset).
* ``ETLConfig`` / field / dataset builder helpers to construct the YAML schema
  programmatically.

Run an ETL::

    import sonicetl
    result = sonicetl.run("examples/etl.yml", "data/uni.json", out_dir="dist")
    print(result.rows)   # {"instrument_raw": 150000, "instrument_master": ..., ...}
    print(result.timing) # [{step, ms}, ...]
"""

from __future__ import annotations

import os
import pathlib
import sys
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Sequence, Union

from . import _core
from ._core import __version__  # noqa: F401

__all__ = [
    "__version__",
    "loads",
    "dumps",
    "run",
    "run_etl",
    "ETLConfig",
    "Dataset",
    "FieldSpec",
    "UnwindRule",
    "EtlResult",
    "ETL_SCHEMA",
]


# ---------------------------------------------------------------------------
# Orjson-style codecs (option arg kept for signature compatibility)
# ---------------------------------------------------------------------------
def _as_bytes(data: Union[str, bytes, bytearray, memoryview]) -> bytes:
    if isinstance(data, bytes):
        return data
    if isinstance(data, (bytearray, memoryview)):
        return bytes(data)
    if isinstance(data, str):
        return data.encode("utf-8")
    raise TypeError("data must be bytes, str, bytearray or memoryview")


def loads(data: Union[str, bytes, bytearray, memoryview]) -> Any:
    """Parse JSON into Python objects using the native sonic-rs core.

    Accepts ``str`` or ``bytes`` (like orjson, which only takes ``bytes``, we
    additionally accept ``str`` for convenience).
    """
    return _core.loads(_as_bytes(data))


def dumps(obj: Any, option: int = 0) -> bytes:
    """Serialize a Python object to JSON ``bytes`` using the sonic-rs core."""
    if not isinstance(option, int):
        raise TypeError("option must be an int")
    return _core.dumps(obj, option)


#: Matches orjson.OPT_* envelope position (we only support the default for now).
OPT_SORT_KEYS = 1
OPT_OMIT_MICROSECONDS = 2
OPT_INDENT_2 = 4
OPT_NAIVE_UTC = 8
OPT_UTC_Z = 16
OPT_PASSTHROUGH_DATETIME = 24
OPT_PASSTHROUGH_SUBCLASS = 128
OPT_APPEND_NEWLINE = 256
OPT_NON_STR_KEYS = 1024
OPT_SERIALIZE_NUMPY = 2048
OPT_SERIALIZE_UUID = 4096
OPT_STRICT_INTEGER = 512


# ---------------------------------------------------------------------------
# ETL schema objects (official ETL YAML schema)
# ---------------------------------------------------------------------------
@dataclass
class UnwindRule:
    """A single unwind rule for an ``unwound`` dataset.

    When ``condition`` holds, the array selected by ``unwind_path`` is "unwound":
    one output row is produced per array element, and each element is exposed to
    field expressions under ``output_alias`` (e.g. ``$.symbol``).
    """

    name: str
    condition: str
    unwind_path: str
    output_alias: str

    def to_dict(self) -> Dict[str, str]:
        return {
            "name": self.name,
            "condition": self.condition,
            "unwind_path": self.unwind_path,
            "output_alias": self.output_alias,
        }


@dataclass
class FieldSpec:
    """A single output column: ``name`` + a field ``expression``.

    Expression syntax (see README):
    * ``$`` or ``$a.b[0]``          JSON-path addressing.
    * ``coalesce(a, b, 'DEF')``     first non-null.
    * ``cast(x as double|integer|string)``  typed coercion.
    * ``to_json_string(x)``         re-serialize a sub-node.
    * literals: ``'str'``, ``true``, ``false``, ``null``, numbers.
    """

    name: str
    expression: str

    def to_dict(self) -> Dict[str, str]:
        return {"name": self.name, "expression": self.expression}


@dataclass
class Dataset:
    """An output dataset. ``type`` is one of ``raw``, ``master`` or ``unwound``.

    * ``raw``:  one output row per input record.
    * ``master``: one output row per input record (typically the canonical,
      normalized view; same per-record semantics as ``raw``).
    * ``unwound``: zero-or-more output rows per input record, driven by
      ``unwind_rules`` (e.g. one row per underlying symbol).
    """

    name: str
    type: str
    fields: Sequence[FieldSpec] = field(default_factory=list)
    unwind_rules: Sequence[UnwindRule] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        d: Dict[str, Any] = {"name": self.name, "type": self.type}
        if self.fields:
            d["fields"] = [f.to_dict() for f in self.fields]
        if self.unwind_rules:
            d["unwind_rules"] = [r.to_dict() for r in self.unwind_rules]
        return d


@dataclass
class ETLConfig:
    """Programmatic builder for the official ETL YAML schema.

    Optionally ``output`` can carry ``format`` / ``output_directory`` /
    ``partition_by`` (accepted for forward compatibility; the native engine
    currently writes a single Parquet file per dataset).
    """

    pipeline_name: str
    source_file_path: str
    source_json_path: Optional[str] = None
    datasets: List[Dataset] = field(default_factory=list)
    output_format: str = "parquet"
    output_directory: str = ""
    partition_by: Sequence[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        d: Dict[str, Any] = {
            "pipeline_name": self.pipeline_name,
            "source": {"file_path": self.source_file_path},
        }
        if self.source_json_path:
            d["source"]["json_path"] = self.source_json_path
        d["datasets"] = [ds.to_dict() for ds in self.datasets]
        d["output"] = {
            "format": self.output_format,
            "output_directory": self.output_directory,
            "partition_by": list(self.partition_by),
        }
        return d

    def to_yaml(self) -> str:
        import yaml

        return yaml.safe_dump(self.to_dict(), sort_keys=False)

    # convenience helpers ------------------------------------------------
    def raw(self, name: str, fields: Sequence[tuple]) -> "ETLConfig":
        self.datasets.append(Dataset(name, "raw", _fields(fields)))
        return self

    def master(self, name: str, fields: Sequence[tuple]) -> "ETLConfig":
        self.datasets.append(Dataset(name, "master", _fields(fields)))
        return self

    def unwound(
        self,
        name: str,
        fields: Sequence[tuple],
        rules: Sequence[UnwindRule],
    ) -> "ETLConfig":
        self.datasets.append(Dataset(name, "unwound", _fields(fields), list(rules)))
        return self


def _fields(specs: Sequence[tuple]) -> List[FieldSpec]:
    out: List[FieldSpec] = []
    for item in specs:
        if isinstance(item, FieldSpec):
            out.append(item)
        else:
            name, expression = item  # (name, expression)
            out.append(FieldSpec(str(name), str(expression)))
    return out


# ---------------------------------------------------------------------------
# Result object
# ---------------------------------------------------------------------------
@dataclass
class EtlResult:
    """Outcome of a whole ETL run."""

    timing: List[Dict[str, Any]]
    datasets: Dict[str, int]
    records: int
    out_dir: str

    @property
    def rows(self) -> Dict[str, int]:
        """Alias for ``datasets`` (name -> row count)."""
        return self.datasets

    def total_etl_ms(self) -> float:
        """Sum of parse + extract + write timings (excludes disk read)."""
        return float(sum(t["ms"] for t in self.timing))

    def breakdown(self) -> str:
        labels = {
            "json parsing": "parse",
            "lazy extract": "extract",
            "parquet write": "write",
        }
        grouped: Dict[str, float] = {}
        for t in self.timing:
            step = t["step"]
            for label, short in labels.items():
                if label in step:
                    grouped[short] = grouped.get(short, 0.0) + float(t["ms"])
                    break
            else:  # noqa: PLW0120
                grouped[step] = grouped.get(step, 0.0) + float(t["ms"])
        return "  ".join(f"{k}={v:.1f} ms" for k, v in grouped.items())


# ---------------------------------------------------------------------------
# High-level runner
# ---------------------------------------------------------------------------
def run(
    config: Union[ETLConfig, Dict[str, Any], str, os.PathLike],
    input: Union[bytes, str, os.PathLike, None] = None,
    out_dir: Union[str, os.PathLike] = ".",
    *,
    config_yaml: Union[str, bytes, None] = None,
    input_bytes: Union[bytes, bytearray, memoryview, None] = None,
) -> EtlResult:
    """Run a whole ETL and return an :class:`EtlResult`.

    Parameters
    ----------
    config:
        An :class:`ETLConfig`, a config ``dict``, a path to an ETL YAML file,
        or a YAML string. As an alternative pass ``config_yaml=...``.
    input:
        The JSON document as ``bytes``, or a path to a JSON file. As an
        alternative pass ``input_bytes=...``.
    out_dir:
        Directory that will receive ``<dataset>.parquet`` files.

    Examples
    --------
    >>> r = sonicetl.run("examples/etl.yml", "data/uni.json", out_dir="dist")
    >>> r.rows
    {'instrument_raw': 150000, 'instrument_master': 150000, ...}
    """
    if config_yaml is not None:
        yaml_text = config_yaml.decode() if isinstance(config_yaml, bytes) else config_yaml
    elif isinstance(config, ETLConfig):
        yaml_text = config.to_yaml()
    elif isinstance(config, dict):
        import yaml

        yaml_text = yaml.safe_dump(config, sort_keys=False)
    elif isinstance(config, (str, os.PathLike)) and _is_path(config):
        with open(config, "r", encoding="utf-8") as fh:  # noqa: PTH123
            yaml_text = fh.read()
    else:
        yaml_text = str(config)

    if input_bytes is not None:
        data = bytes(input_bytes)
    elif input is None:
        raise ValueError("run() requires `input` (bytes) or `input_bytes`")
    elif isinstance(input, (bytes, bytearray, memoryview)):
        data = bytes(input)
    else:
        with open(input, "rb") as fh:  # noqa: PTH123
            data = fh.read()

    out = os.fspath(out_dir)
    raw = _core.run_etl(yaml_text, data, out)
    return EtlResult(
        timing=raw["timing"],
        datasets=dict(raw["datasets"]),
        records=int(raw["records"]),
        out_dir=str(raw["out_dir"]),
    )


def run_etl(config_yaml: str, input_bytes: bytes, out_dir: str) -> Dict[str, Any]:
    """Low-level ETL entrypoint mirroring the Rust core.

    ``config_yaml`` must be a YAML string, ``input_bytes`` the JSON document as
    ``bytes``. Returns the raw result dict (``timing``, ``datasets``, ...).
    Prefer :func:`run` for the higher-level API.
    """
    return dict(_core.run_etl(config_yaml, bytes(input_bytes), out_dir))


def _is_path(value: Union[str, os.PathLike]) -> bool:
    s = os.fspath(value)
    if isinstance(s, bytes):
        s = s.decode()
    return s.endswith((".yml", ".yaml")) or os.path.exists(s)


# ---------------------------------------------------------------------------
# Documented ETL YAML schema (kept as data so it can be consumed programmatically)
# ---------------------------------------------------------------------------
DOC = """
pipeline_name: string                         # human-readable pipeline name
source:
  file_path: string                           # input JSON path (records array)
  json_path: string?                          # optional root path into the JSON
datasets:
  - name: string                              # output dataset name -> <name>.parquet
    type: "raw" | "master" | "unwound"        # one output row semantics
    fields:
      - name: string                          # output column name
        expression: string                    # field expression (see README)
    unwind_rules:                             # only for type=unwound
      - name: string
        condition: string                     # e.g. "$.instrumentName CONTAINS 'FCN'"
        unwind_path: string                   # e.g. "$.KIKOSelect.underlying"
        output_alias: string                  # exposed as e.g. "$.symbol"
output:
  format: "parquet"                           # (informational; parquet is implicit)
  output_directory: string
  partition_by: [string]
"""  # noqa: E501

ETL_SCHEMA = DOC


def main() -> None:  # pragma: no cover - convenience CLI
    if len(sys.argv) < 2:
        print("usage: python -m sonicetl <etl.yml> <input.json> [out_dir]")
        return
    config, src = sys.argv[1], sys.argv[2]
    out_dir = sys.argv[3] if len(sys.argv) > 3 else "dist"
    r = run(config, src, out_dir=out_dir)
    print(r.breakdown())
    print("rows:", r.rows)
