"""sonicetl — a native whole-ETL Python library backed by a Rust core (sonic-rs).

sonicetl is designed like orjson: the heavy lifting happens in a compiled Rust
extension (``sonicetl._core``) while the Python layer exposes a clean, small API.

Public surface
--------------
* ``loads`` / ``dumps`` — orjson-style JSON codecs powered by sonic-rs.
* ``run_pipelines`` — run a set of ETL pipelines described by the official ETL
  YAML schema (see ``docs/schema.md``). The root document is
  ``pipelines: [ ... ]``; every pipeline may declare any number of *sources*
  (inputs) and *datasets* (outputs). Datasets may target any store — parquet
  file, in-memory duckdb table, or duckdb file — via a duckdb-style URI, and
  may LEFT JOIN against tables produced earlier in the same call.
* ``PipelinesConfig`` / ``Pipeline`` / ``Source`` / ``Dataset`` / ``Field`` /
  ``UnwindRule`` / ``Join`` — thin builder helpers to construct the YAML schema
  programmatically.

Run a set of pipelines::

    import sonicetl
    result = sonicetl.run_pipelines("examples/demo/pipelines.yml")
    print(result.rows)    # {"spot": 12, "products": 36}
    print(result.timing)  # [{step, ms}, ...]
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
    "run_pipelines",
    "PipelinesConfig",
    "Pipeline",
    "Source",
    "Output",
    "Dataset",
    "Field",
    "UnwindRule",
    "Join",
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
# ETL schema objects (official ETL YAML schema, see docs/schema.md)
# ---------------------------------------------------------------------------
@dataclass
class Field:
    """A single output column: ``name`` + a field ``expression``.

    Expression syntax (see README / docs/schema.md):
    * ``$`` or ``$a.b[0]``           JSON-path addressing.
    * ``{sourceName.}path``          cross-source projection (bare path = default source).
    * ``coalesce(a, b, 'DEF')``      first non-null.
    * ``cast(x as double|integer|string)``  typed coercion.
    * ``alias.col`` / ``cast(alias.col as TYPE)``  joined-column access.
    * ``to_json_string(x)``          re-serialize a sub-node.
    * literals: ``'str'``, ``true``, ``false``, ``null``, numbers.
    """

    name: str
    expression: str

    def to_dict(self) -> Dict[str, str]:
        return {"name": self.name, "expression": self.expression}


@dataclass
class UnwindRule:
    """A single unwind rule for an ``unwound`` dataset.

    When ``condition`` holds, the array selected by ``unwind_path`` is "unwound":
    one output row is produced per array element, and each element is exposed to
    field expressions under ``output_alias`` (e.g. ``$.u``). If ``output_alias``
    is empty, ``name`` is used as the alias.
    """

    name: str
    condition: str
    unwind_path: str
    output_alias: str = ""

    def to_dict(self) -> Dict[str, str]:
        return {
            "name": self.name,
            "condition": self.condition,
            "unwind_path": self.unwind_path,
            "output_alias": self.output_alias,
        }


@dataclass
class Join:
    """A LEFT JOIN of this dataset's rows against a table produced earlier.

    ``target`` is a store URI (``memory://<table>`` or ``duckdb://<file>?table=<t>``).
    ``left_key`` is an expression evaluated per row against the default source
    (e.g. ``$.u``); ``right_key`` is the target table's column name. The selected
    ``columns`` are exposed to field expressions under ``alias``.
    """

    alias: str
    target: str
    left_key: str
    right_key: str
    columns: Sequence[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "alias": self.alias,
            "target": self.target,
            "left_key": self.left_key,
            "right_key": self.right_key,
            "columns": list(self.columns),
        }


@dataclass
class Output:
    """A store target. ``uri`` is a duckdb-style URI:

    * ``file:///...`` or a bare path  -> parquet file (or directory when ``partition_by`` is set)
    * ``memory://<table>``            -> in-memory duckdb table (shared across the call)
    * ``duckdb://<file>?table=<t>`    -> duckdb file-backed table

    ``format`` is informational for file targets (``parquet`` is the default).
    """

    uri: str = ""
    format: str = "parquet"
    partition_by: Sequence[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        d: Dict[str, Any] = {"uri": self.uri}
        if self.format:
            d["format"] = self.format
        if self.partition_by:
            d["partition_by"] = list(self.partition_by)
        return d


@dataclass
class Source:
    """An input. ``uri`` is a duckdb-style store URI:

    * ``file:///path`` (or a bare path) reads a JSON document (``format: json``,
      optionally narrowed via ``json_path``) or a parquet file (``format: parquet``).
    * ``memory://<table>`` / ``duckdb://<file>?table=<t>`` read an existing table
      from the shared store (e.g. one written by an earlier pipeline).
    """

    name: str
    uri: str
    format: Optional[str] = None
    json_path: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        d: Dict[str, Any] = {"name": self.name, "uri": self.uri}
        if self.format:
            d["format"] = self.format
        if self.json_path:
            d["json_path"] = self.json_path
        return d


@dataclass
class Dataset:
    """An output dataset. ``type`` is one of ``raw``, ``master`` or ``unwound``.

    * ``raw``:  one output row per input record.
    * ``master``: one output row per input record (canonical, normalized view).
    * ``unwound``: zero-or-more output rows per input record, driven by
      ``unwind_rules`` (e.g. one row per underlying symbol).

    ``source`` names the default input (a ``Source.name``); ``$`` resolves to
    this source's records. ``to`` is the output store; ``join`` optionally
    LEFT-JOINs against a previously produced table.
    """

    name: str
    type: str
    source: str = ""
    to: Optional[Output] = None
    fields: Sequence[Field] = field(default_factory=list)
    unwind_rules: Sequence[UnwindRule] = field(default_factory=list)
    join: Optional[Join] = None

    def to_dict(self) -> Dict[str, Any]:
        d: Dict[str, Any] = {"name": self.name, "type": self.type}
        if self.source:
            d["source"] = self.source
        if self.to is not None:
            d["to"] = self.to.to_dict()
        if self.fields:
            d["fields"] = [f.to_dict() for f in self.fields]
        if self.unwind_rules:
            d["unwind_rules"] = [r.to_dict() for r in self.unwind_rules]
        if self.join is not None:
            d["join"] = self.join.to_dict()
        return d


@dataclass
class Pipeline:
    """A named ETL pipeline: any number of sources + any number of datasets."""

    name: str = ""
    sources: Sequence[Source] = field(default_factory=list)
    datasets: Sequence[Dataset] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        d: Dict[str, Any] = {}
        if self.name:
            d["name"] = self.name
        if self.sources:
            d["sources"] = [s.to_dict() for s in self.sources]
        if self.datasets:
            d["datasets"] = [ds.to_dict() for ds in self.datasets]
        return d


@dataclass
class PipelinesConfig:
    """Programmatic builder for the official ETL YAML schema.

    Example::

        cfg = PipelinesConfig([
            Pipeline(name="mktDataETL", sources=[Source("spot", "file://spot.json")],
                     datasets=[Dataset("spot", "raw", to=Output("memory://spot"),
                                       fields=[Field("name", "$._id")])]),
        ])
        result = sonicetl.run_pipelines(cfg)
    """

    pipelines: Sequence[Pipeline] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {"pipelines": [p.to_dict() for p in self.pipelines]}

    def to_yaml(self) -> str:
        import yaml

        return yaml.safe_dump(self.to_dict(), sort_keys=False)


# ---------------------------------------------------------------------------
# Result object
# ---------------------------------------------------------------------------
@dataclass
class EtlResult:
    """Outcome of a ``run_pipelines`` call."""

    timing: List[Dict[str, Any]]
    datasets: Dict[str, int]

    @property
    def rows(self) -> Dict[str, int]:
        """Alias for ``datasets`` (dataset name -> row count)."""
        return self.datasets

    def total_etl_ms(self) -> float:
        """Sum of per-dataset timings (excludes the ``pipeline ... total`` rows)."""
        return float(
            sum(t["ms"] for t in self.timing if not str(t["step"]).endswith(" total"))
        )

    def breakdown(self) -> str:
        return "  ".join(f"{t['step']}={t['ms']:.1f} ms" for t in self.timing)


# ---------------------------------------------------------------------------
# High-level runner
# ---------------------------------------------------------------------------
def run_pipelines(
    config: Union[PipelinesConfig, Dict[str, Any], str, os.PathLike],
    *,
    config_yaml: Union[str, bytes, None] = None,
) -> EtlResult:
    """Run a set of ETL pipelines and return an :class:`EtlResult`.

    Parameters
    ----------
    config:
        A :class:`PipelinesConfig`, a config ``dict``, a path to a pipelines YAML
        file, or a YAML string. As an alternative pass ``config_yaml=...``.

    Examples
    --------
    >>> r = sonicetl.run_pipelines("examples/demo/pipelines.yml")
    >>> r.rows
    {'spot': 12, 'products': 36}
    """
    if config_yaml is not None:
        yaml_text = config_yaml.decode() if isinstance(config_yaml, bytes) else config_yaml
    elif isinstance(config, PipelinesConfig):
        yaml_text = config.to_yaml()
    elif isinstance(config, dict):
        import yaml

        yaml_text = yaml.safe_dump(config, sort_keys=False)
    elif isinstance(config, (str, os.PathLike)) and _is_path(config):
        with open(config, "r", encoding="utf-8") as fh:  # noqa: PTH123
            yaml_text = fh.read()
    else:
        yaml_text = str(config)

    raw = _core.run_pipelines(yaml_text)
    return EtlResult(
        timing=list(raw["timing"]),
        datasets=dict(raw["datasets"]),
    )


def _is_path(value: Union[str, os.PathLike]) -> bool:
    s = os.fspath(value)
    if isinstance(s, bytes):
        s = s.decode()
    return s.endswith((".yml", ".yaml")) or os.path.exists(s)


# ---------------------------------------------------------------------------
# Documented ETL YAML schema (kept as data so it can be consumed programmatically)
# ---------------------------------------------------------------------------
DOC = """
pipelines:
  - name: string                             # human-readable pipeline name
    sources:
      - name: string                         # referenced by datasets (`source:`)
        uri: string                          # file:///memory://duckdb:// store URI
        format: "json" | "parquet"?          # file sources (default json)
        json_path: string?                   # optional sub-path of a JSON document
    datasets:
      - name: string                         # output dataset name / table name
        type: "raw" | "master" | "unwound"
        source: string?                      # default source; `$` = dataset's source
        to:
          uri: string                        # file:///memory://duckdb:// target
          format: string?                    # informational; parquet is default
          partition_by: [string]?            # Hive partition columns
        fields:
          - name: string                     # output column name
            expression: string               # field expression (see docs/schema.md)
        unwind_rules:                        # only for type=unwound
          - name: string
            condition: string                # e.g. "$.underlyings[0] != ''"
            unwind_path: string              # e.g. "$.underlyings"
            output_alias: string             # exposed as e.g. "$.u"
        join:                                # optional LEFT JOIN
          alias: string                      # column prefix, e.g. "mkt"
          target: string                     # store URI of the table to join
          left_key: string                   # e.g. "$.u" (this dataset's key)
          right_key: string                  # target table's key column
          columns: [string]                  # right columns exposed under alias
"""  # noqa: E501

ETL_SCHEMA = DOC


def main() -> None:  # pragma: no cover - convenience CLI
    if len(sys.argv) < 2:
        print("usage: python -m sonicetl <pipelines.yml>")
        return
    config = sys.argv[1]
    r = run_pipelines(config)
    print(r.breakdown())
    print("rows:", r.rows)