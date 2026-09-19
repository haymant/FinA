"""Canonical payoff-graph compilation and lowering for fina-core.

The payoff graph is the canonical *compiled pricing* representation: an
instrument's legs, payoff nodes, gates and schedule expressed as a typed DAG,
with the fina-risk C++ kernel bindings that evaluate each node. It is emitted
from semantic terms (``ProductTerms`` / the ``fcn-terms-projection`` stand-in)
plus an optional ``MarketSnapshot`` and ``LifecycleState``, and is lowered to
the two engine-facing views:

* ``lower_payoff_graph_to_fcn_terms`` — the flattened ``FcnTerms`` struct the
  native ``fina_risk_cpp`` kernel consumes (``fina_risk/terms.hpp``).
* ``lower_payoff_graph_to_pricing_request`` — the compiled ``legs``/``market_data``
  pricing-request shape (``pricing-request.schema.json``).
* ``enrich_pricing_request`` — the risk-store view carrying
  ``InstrumentKey``/``UnwindMapRaw``/``RiskFactorKeys``/``MarketDataSnapshot``/
  ``UpdatedLifecycle``/``CommonEconomics`` for sensi/P&L storage.

Everything here is stdlib-only so the schedule/ETL stages can run it without a
numpy/native dependency.
"""

from __future__ import annotations

import hashlib
import json
from datetime import date, timedelta
from typing import Any, Dict, List, Optional

SCHEMA_VERSION = "fina/payoff-graph/v1"
ENGINE_MARKER = "cpp_daily_termsheet_eki"
CPP_MODULE = "fina_risk_cpp"
CPP_HEADER_ROOT = "modules/fina-risk/cpp/include/fina_risk/"

_EPOCH = date(1899, 12, 30)


def excel_serial_to_date(serial: int) -> date:
    """Excel/WPS serial number to :class:`date` (1899-12-30 epoch)."""
    return _EPOCH + timedelta(days=int(serial))


def date_to_excel_serial(value: Any) -> int:
    """Date/ISO string to Excel serial number."""
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        try:
            parsed = date.fromisoformat(value)
        except ValueError as exc:  # pragma: no cover - defensive
            raise ValueError(f"not an ISO date: {value!r}") from exc
        return (parsed - _EPOCH).days
    raise TypeError(f"cannot interpret date: {value!r}")


def _first_nonzero(values: Any, default: float = 0.0) -> float:
    if isinstance(values, (int, float)):
        return float(values)
    if isinstance(values, list):
        for value in values:
            if isinstance(value, (int, float)) and value:
                return float(value)
    return default


def _as_list(values: Any) -> List[Any]:
    return list(values) if isinstance(values, list) else []


def graph_hash(graph: Dict[str, Any]) -> str:
    """Stable content hash over the canonical graph document."""
    payload = json.dumps(graph, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Kernel bindings: node kind -> fina-risk C++ function organization
# ---------------------------------------------------------------------------
FUNCTION_BINDINGS: List[Dict[str, str]] = [
    {"node_kind": "fixing_schedule", "module": CPP_MODULE, "symbol": "fina::risk::fcn::observation_on_or_before", "header": f"{CPP_HEADER_ROOT}schedule.hpp", "engine_marker": ENGINE_MARKER},
    {"node_kind": "worst_of_performance", "module": CPP_MODULE, "symbol": "fina::risk::fcn::price", "header": f"{CPP_HEADER_ROOT}engine.hpp", "engine_marker": ENGINE_MARKER},
    {"node_kind": "knock_in_gate", "module": CPP_MODULE, "symbol": "fina::risk::fcn::update_knock_in", "header": f"{CPP_HEADER_ROOT}terminal_option.hpp", "engine_marker": ENGINE_MARKER},
    {"node_kind": "down_and_in_put", "module": CPP_MODULE, "symbol": "fina::risk::fcn::terminal_option_payoff", "header": f"{CPP_HEADER_ROOT}terminal_option.hpp", "engine_marker": ENGINE_MARKER},
    {"node_kind": "global_ko_gate", "module": CPP_MODULE, "symbol": "fina::risk::fcn::resolve_ko", "header": f"{CPP_HEADER_ROOT}barriers.hpp", "engine_marker": ENGINE_MARKER},
    {"node_kind": "local_ko_gate", "module": CPP_MODULE, "symbol": "fina::risk::fcn::resolve_ko", "header": f"{CPP_HEADER_ROOT}barriers.hpp", "engine_marker": ENGINE_MARKER},
    {"node_kind": "range_accrual", "module": CPP_MODULE, "symbol": "fina::risk::fcn::in_range", "header": f"{CPP_HEADER_ROOT}coupon.hpp", "engine_marker": ENGINE_MARKER},
    {"node_kind": "coupon_strip", "module": CPP_MODULE, "symbol": "fina::risk::fcn::period_rate", "header": f"{CPP_HEADER_ROOT}coupon.hpp", "engine_marker": ENGINE_MARKER},
    {"node_kind": "memory_carry", "module": CPP_MODULE, "symbol": "fina::risk::fcn::next_memory", "header": f"{CPP_HEADER_ROOT}coupon.hpp", "engine_marker": ENGINE_MARKER},
    {"node_kind": "notional_return", "module": CPP_MODULE, "symbol": "fina::risk::fcn::funding_amount", "header": f"{CPP_HEADER_ROOT}funding.hpp", "engine_marker": ENGINE_MARKER},
    {"node_kind": "cash_settlement", "module": CPP_MODULE, "symbol": "fina::risk::fcn::settlement_for", "header": f"{CPP_HEADER_ROOT}settlement.hpp", "engine_marker": ENGINE_MARKER},
    {"node_kind": "physical_delivery_settlement", "module": CPP_MODULE, "symbol": "fina::risk::fcn::settlement_for", "header": f"{CPP_HEADER_ROOT}settlement.hpp", "engine_marker": ENGINE_MARKER},
    {"node_kind": "payment_discount", "module": CPP_MODULE, "symbol": "fina::risk::fcn::price", "header": f"{CPP_HEADER_ROOT}engine.hpp", "engine_marker": ENGINE_MARKER},
]

_KERNEL_BY_KIND = {entry["node_kind"]: entry for entry in FUNCTION_BINDINGS}


# ---------------------------------------------------------------------------
# Legacy deal-data readers (tolerant of both legacy block shapes)
# ---------------------------------------------------------------------------
def _legacy_reader(legacy: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Normalize a legacy term-sheet payload into RGACCLKO/knockInStar/KIKO facts."""
    if not legacy:
        return {}
    root = legacy
    if isinstance(root.get("Chunk"), dict) and isinstance(root["Chunk"].get("Jobs"), list):
        jobs = root["Chunk"]["Jobs"]
        if jobs:
            root = jobs[-1]["commonData"]
    deal = root.get("dealData", {}) if isinstance(root, dict) else {}
    market = root.get("marketData", {}) if isinstance(root, dict) else {}
    deal_access = deal if isinstance(deal, dict) else {}
    result: Dict[str, Any] = {}
    kiko = deal_access.get("KIKOSelect") or {}
    result["kiko"] = kiko if isinstance(kiko, dict) else {}
    result["knock_in_star"] = deal_access.get("knockInStar") or {}
    rgacc = deal_access.get("RGACCLKO") or {}
    result["rgacc"] = rgacc if isinstance(rgacc, dict) else {}
    result["deal_expiry_date"] = deal_access.get("expiryDate")
    result["deal_maturity_date"] = deal_access.get("maturityDate")
    result["legacy_coupon_quote_scale"] = deal_access.get("legacyCouponQuoteScale")
    result["reference_prices"] = _as_list(result["kiko"].get("referencePrice"))
    mcpara = market.get("MCPara") or {}
    result["mcp_market"] = market
    result["mcpara"] = mcpara if isinstance(mcpara, dict) else {}
    return result


def _coupon_periods(terms: Dict[str, Any], legacy: Optional[Dict[str, Any]], evaluation_serial: int) -> List[Dict[str, Any]]:
    """Per-period coupon detail: explicit ``coupon`` terms when present, else RGACCLKO.

    Periods that ended at or before the *pricing* evaluation date are already
    behind the curve and are filtered out (they are carried in ``N1`` of the
    memory bookkeeping instead), matching the fina-risk compilation behavior.
    """
    coupon = terms.get("coupon", {})
    periods: List[Dict[str, Any]] = []
    if isinstance(coupon.get("periods"), list) and coupon["periods"]:
        evaluation = evaluation_serial
        return [
            dict(period) for period in coupon["periods"]
            if date_to_excel_serial(period["end_date"]) > evaluation
        ]
    if not legacy:
        return periods
    rgacc = legacy.get("rgacc", {})
    ends = _as_list(rgacc.get("endDate"))
    payments = _as_list(rgacc.get("paymentDate"))
    starts = _as_list(rgacc.get("startDate"))
    rates = _as_list(rgacc.get("accruRate"))
    lows = _as_list(rgacc.get("lowRange"))
    ups = _as_list(rgacc.get("upRange"))
    n1 = _as_list(rgacc.get("N1"))
    n2 = _as_list(rgacc.get("N2"))
    previous = evaluation_serial
    for index, end in enumerate(ends):
        end_serial = date_to_excel_serial(end)
        if end_serial <= evaluation_serial:
            previous = end_serial
            continue
        start_serial = date_to_excel_serial(starts[index]) if index < len(starts) else previous
        periods.append(
            {
                "period": index + 1,
                "start_date": max(int(start_serial), evaluation_serial),
                "end_date": int(end_serial),
                "payment_date": int(date_to_excel_serial(payments[index])) if index < len(payments) else int(end_serial),
                "range_rate": float(rates[index]) if index < len(rates) else 0.0,
                "fixed_coupon": 0.0,
                "lower_bound": float(lows[index]) if index < len(lows) else 0.0,
                "upper_bound": float(ups[index]) if index < len(ups) else 1.0e12,
                "already_paid_fixings": int(n1[index]) if index < len(n1) else 0,
                "total_fixings": int(n2[index]) if index < len(n2) else 0,
            }
        )
        previous = int(end_serial)
    return periods


# ---------------------------------------------------------------------------
# Compile: semantic terms -> canonical payoff graph
# ---------------------------------------------------------------------------
def compile_payoff_graph(
    terms: Dict[str, Any],
    *,
    market: Optional[Dict[str, Any]] = None,
    lifecycle: Optional[Dict[str, Any]] = None,
    legacy: Optional[Dict[str, Any]] = None,
    parameters: Optional[Dict[str, Any]] = None,
    product: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Compile semantic terms (``fcn-terms-projection`` shape) into the canonical payoff graph.

    ``market`` is a pricing-request ``market_data`` snapshot (injected at pricing
    time). ``legacy`` optionally supplies the provenance facts (coupon periods,
    serial dates, per-name barriers) that the semantic levels intentionally leave
    abstract. ``lifecycle`` is the mutable state fingerprint.
    """
    economics = terms.get("economics", {})
    schedule = terms.get("schedule", {})
    features = terms.get("features", {})
    settlement = terms.get("settlement", {})
    provenance = terms.get("provenance", {})
    legacy_facts = _legacy_reader(legacy)

    currency = economics.get("currency") or settlement.get("payment_currency") or "USD"
    notional = float(economics.get("notional", 0.0))
    strike = float(economics.get("strike", 0.0))
    ki_barrier = float(economics.get("knock_in_barrier", 0.0))
    coupon_rate = float(economics.get("coupon_rate", 0.0))
    return_ratio = float(economics.get("return_ratio", 1.0))

    evaluation_iso = schedule.get("evaluation_date", "2026-01-01")
    if market and market.get("evaluation_date") is not None:
        evaluation_serial = date_to_excel_serial(market["evaluation_date"])
    else:
        evaluation_serial = date_to_excel_serial(evaluation_iso)
    legacy_expiry = legacy_facts.get("deal_expiry_date")
    legacy_maturity = legacy_facts.get("deal_maturity_date")
    final_fixing_serial = (
        date_to_excel_serial(legacy_expiry) if legacy_expiry is not None else date_to_excel_serial(economics.get("maturity_date", evaluation_iso))
    )
    maturity_serial = (
        date_to_excel_serial(legacy_maturity) if legacy_maturity is not None else date_to_excel_serial(economics.get("maturity_date", final_fixing_serial))
    )

    reference_prices = _as_list(provenance.get("reference_prices")) or legacy_facts.get("reference_prices") or []
    underlyings = _as_list(provenance.get("underlyings")) or _as_list(economics.get("underlyings")) or []
    reference_spots = [float(value) for value in reference_prices if value]

    periods = _coupon_periods(terms, legacy_facts, evaluation_serial)
    global_barrier = float(features.get("ko_enabled") or economics.get("knock_out_barrier") or 0.0) or 0.0
    if not global_barrier and legacy_facts.get("rgacc", {}).get("gblBarPrice"):
        global_barrier = _first_nonzero(legacy_facts["rgacc"].get("gblBarPrice"))
    if not global_barrier:
        global_barrier = 1.10

    payment_lag_days = [
        max(int(date_to_excel_serial(period["payment_date"])) - int(date_to_excel_serial(period["end_date"])), 0)
        for period in periods
        if int(date_to_excel_serial(period["payment_date"])) != int(date_to_excel_serial(period["end_date"]))
    ] or [2]

    nodes: List[Dict[str, Any]] = [
        {
            "node_id": "fixing_schedule",
            "kind": "fixing_schedule",
            "label": "NYSE fixing schedule",
            "config": {"calendar": schedule.get("calendar", "NYSE"), "observation_frequency": schedule.get("observation_frequency", "daily")},
            "kernel": _KERNEL_BY_KIND["fixing_schedule"],
        },
        {
            "node_id": "worst_of_performance",
            "kind": "worst_of_performance",
            "label": "worst-of close / initial spot across basket",
            "config": {"indicator": features.get("performance_indicator", "worst_of"), "basis": "close_over_initial_spot"},
            "kernel": _KERNEL_BY_KIND["worst_of_performance"],
        },
        {
            "node_id": "knock_in_gate",
            "kind": "knock_in_gate",
            "label": "European knock-in at final fixing",
            "config": {
                "barrier": ki_barrier,
                "operator": features.get("ki_operator", "<="),
                "monitoring": "EKI",
                "scope": "final_fixing",
                "final_fixing_eki": bool(features.get("final_fixing_eki", True)),
            },
            "kernel": _KERNEL_BY_KIND["knock_in_gate"],
        },
        {
            "node_id": "global_ko_gate",
            "kind": "global_ko_gate",
            "label": "global call/KO at all-underlyings >= barrier",
            "config": {
                "barrier": global_barrier,
                "operator": features.get("ko_operator", ">="),
                "style": features.get("ko_type", "american"),
                "memory_ko": bool(features.get("memory_ko", False)),
                "enabled": bool(features.get("ko_enabled", True)),
            },
            "kernel": _KERNEL_BY_KIND["global_ko_gate"],
        },
        {
            "node_id": "coupon_strip",
            "kind": "coupon_strip",
            "label": "range-accrual coupon strip with memory carry",
            "config": {
                "indicator": terms.get("coupon", {}).get("indicator") or "WPS",
                "rate": coupon_rate,
                "memory_carry": bool(features.get("memory_coupon", True)),
                "unpaid_period_rule": terms.get("coupon", {}).get("unpaid_period_rule") or "N2-N1",
                "periods": periods,
            },
            "kernel": _KERNEL_BY_KIND["coupon_strip"],
        },
    ]
    if features.get("memory_coupon", True):
        nodes.append(
            {
                "node_id": "memory_carry",
                "kind": "memory_carry",
                "label": "unpaid fixing memory carried into later periods",
                "config": {"denominator": "N2", "per_period": True},
                "kernel": _KERNEL_BY_KIND["memory_carry"],
            }
        )
    nodes.extend(
        [
            {
                "node_id": "down_and_in_put",
                "kind": "down_and_in_put",
                "label": "worst-of down-and-in put at final fixing",
                "config": {"strike": strike, "ki_enabled": True, "settlement": "physical_delivery" if bool(features.get("physical_delivery", False)) else "cash"},
                "kernel": _KERNEL_BY_KIND["down_and_in_put"],
            },
            {
                "node_id": "notional_return",
                "kind": "notional_return",
                "label": "notional redemption at par",
                "config": {"return_ratio": return_ratio, "enabled": True},
                "kernel": _KERNEL_BY_KIND["notional_return"],
            },
            {
                "node_id": "physical_delivery_settlement",
                "kind": "physical_delivery_settlement",
                "label": "worst-asset physical delivery after knock-in",
                "config": {
                    "delivery_type": settlement.get("delivery_type", "physical"),
                    "itm_payment": settlement.get("itm_payment", "delivery"),
                    "enabled": bool(features.get("physical_delivery", False)),
                },
                "kernel": _KERNEL_BY_KIND["physical_delivery_settlement"],
            },
            {
                "node_id": "payment_discount",
                "kind": "payment_discount",
                "label": "payment-date discounting",
                "config": {"curve": "USD Std Curve", "day_count": "Actual/365"},
                "kernel": _KERNEL_BY_KIND["payment_discount"],
            },
        ]
    )

    edges: List[Dict[str, str]] = [
        {"from": "fixing_schedule", "to": "worst_of_performance", "role": "feeds"},
        {"from": "worst_of_performance", "to": "knock_in_gate", "role": "feeds"},
        {"from": "worst_of_performance", "to": "global_ko_gate", "role": "feeds"},
        {"from": "worst_of_performance", "to": "coupon_strip", "role": "feeds"},
        {"from": "knock_in_gate", "to": "down_and_in_put", "role": "activates"},
        {"from": "global_ko_gate", "to": "down_and_in_put", "role": "terminates"},
        {"from": "global_ko_gate", "to": "coupon_strip", "role": "terminates"},
        {"from": "coupon_strip", "to": "memory_carry", "role": "feeds"},
        {"from": "down_and_in_put", "to": "physical_delivery_settlement", "role": "activates"},
        {"from": "down_and_in_put", "to": "payment_discount", "role": "discounts"},
        {"from": "notional_return", "to": "payment_discount", "role": "discounts"},
        {"from": "coupon_strip", "to": "payment_discount", "role": "discounts"},
    ]

    leg_nodes = {"ki_put": "down_and_in_put", "funding": "notional_return", "coupon": "coupon_strip"}
    legs: List[Dict[str, Any]] = []
    for leg in terms.get("legs", []):
        role = leg.get("role")
        legs.append(
            {
                "leg_id": leg.get("leg_id"),
                "role": role,
                "name": leg.get("name", role or "leg"),
                "multiplier": float(leg.get("multiplier", 1.0)),
                "notional": float(leg.get("notional", notional)),
                "payoff_node": leg_nodes.get(role, "custom"),
            }
        )

    lifecycle_state = lifecycle or {}
    locked = lifecycle_state.get("locked", [])
    graph: Dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "graph_id": f"{provenance.get('legacy_product_name', 'FCN')}-{terms.get('product_type', 'ELIFCN_KI')}".upper(),
        "graph_type": "PAYOFF",
        "graph_hash": None,  # set after assembly
        "product": {
            "family": terms.get("product_family", "fcn"),
            "type": terms.get("product_type", "ELIFCN_KI"),
            "template": provenance.get("legacy_product_template", "ELIFCN_KI"),
            "version": 1,
            "model_id": provenance.get("model_id", "memraki"),
            "payoff_script_name": provenance.get("payoff_script", "ELIFCN_KI"),
        },
        "source_ref": {
            "product_terms_schema": terms.get("schema_version", "fcn-terms-projection.schema.json"),
            "terms_version": "pending-freeze",
            "market_snapshot_ref": None if market is None else f"market@{evaluation_serial}",
            "lifecycle_ref": "lifecycle-state" if lifecycle_state else None,
            "legacy_refs": terms.get("source_ref", {}).get("fixture"),
        },
        "currency": currency,
        "notional": notional,
        "coupon_quote_scale": float(legacy_facts.get("legacy_coupon_quote_scale") or terms.get("coupon", {}).get("coupon_quote_scale") or 10.0),
        "performance": {"indicator": "worst_of", "basis": "close_over_initial_spot"},
        "dates": {
            "evaluation_date": evaluation_serial,
            "final_fixing_date": final_fixing_serial,
            "maturity_date": maturity_serial,
        },
        "reference_spots": reference_spots,
        "schedule": {
            "calendar": schedule.get("calendar", "NYSE"),
            "observation_frequency": schedule.get("observation_frequency", "daily"),
            "evaluation_date": evaluation_iso,
            "last_fixing_date": schedule.get("last_fixing_date", evaluation_iso),
        },
        "lifecycle": {
            "already_knock_in": bool(lifecycle_state.get("already_knock_in", False)),
            "memory_carry": bool(features.get("memory_coupon", True)),
            "locked": locked or None,
        },
        "coupon": {
            "type": "range_accrual_strip",
            "indicator": "WPS",
            "rate": coupon_rate,
            "memory_carry": bool(features.get("memory_coupon", True)),
            "unpaid_period_rule": "N2-N1",
            "payment_lag_days": payment_lag_days,
        },
        "legs": legs,
        "nodes": nodes,
        "edges": edges,
        "payoff_script": "",  # rendered by render_payoff_script()
        "function_bindings": FUNCTION_BINDINGS,
    }
    graph["payoff_script"] = render_payoff_script(graph)
    if market is not None:
        graph["market_data"] = market
    graph["parameters"] = {**DEFAULT_PARAMETERS, **(parameters or {})}
    graph["provenance"] = {**provenance, "underlyings": underlyings}
    period_rate = next((float(p.get("range_rate", 0.0)) for p in periods if p.get("range_rate")), 0.0)
    if legacy and coupon_rate and period_rate and abs(period_rate - coupon_rate) > 1.0e-9:
        graph["provenance"]["coupon_rate_note"] = {
            "semantic_coupon_rate": coupon_rate,
            "period_range_rate": period_rate,
            "detail": "semantic/compiled rate diverges from legacy RGACCLKO accruRate; engine periods carry the legacy per-period rate",
        }
    graph["graph_hash"] = None
    if product:
        graph["product"].update(product)
    graph["graph_hash"] = graph_hash(graph)
    return graph


def render_payoff_script(graph: Dict[str, Any]) -> str:
    """Human-readable payoff explanation generated from the graph nodes/edges."""
    legs = graph.get("legs", [])
    terms_segments: List[str] = []
    for leg in legs:
        node = graph["legs"][0]["payoff_node"] if leg["payoff_node"] == "down_and_in_put" else leg["payoff_node"]
        kind = next((n["kind"] for n in graph.get("nodes", []) if n["node_id"] == node), None)
        multiplier = leg.get("multiplier", 1.0)
        sign = "-" if multiplier < 0 else "+"
        if kind == "down_and_in_put":
            cfg = next(n["config"] for n in graph.get("nodes", []) if n["node_id"] == "down_and_in_put")
            ki_cfg = next(n["config"] for n in graph.get("nodes", []) if n["kind"] == "knock_in_gate")
            terms_segments.append(f"{sign} put(EKI {ki_cfg['barrier']}, strike {cfg['strike']}, {cfg['settlement']})")
        elif kind == "notional_return":
            cfg = next(n["config"] for n in graph.get("nodes", []) if n["node_id"] == "notional_return")
            terms_segments.append(f"{sign} notional_return({cfg['return_ratio']:g}x)")
        elif kind == "coupon_strip":
            coupon = graph.get("coupon", {})
            terms_segments.append(f"{sign} coupon_strip({coupon['rate']:g}, {coupon['indicator']}, {coupon['unpaid_period_rule']}{', memory' if coupon['memory_carry'] else ''})")
        else:
            terms_segments.append(f"{sign} {kind}")
    ki = graph.get("nodes", [])
    ki_cfg = next((n["config"] for n in ki if n["kind"] == "knock_in_gate"), {})
    ko_cfg = next((n["config"] for n in ki if n["kind"] == "global_ko_gate"), {})
    gates = []
    if ki_cfg:
        gates.append(f"EKI({ki_cfg.get('barrier')}, {ki_cfg.get('scope')})")
    if ko_cfg and ko_cfg.get("enabled"):
        gates.append(f"global_ko({ko_cfg.get('barrier')}, {ko_cfg.get('operator')}) terminates coupon+put")
    header = "worst-of { "
    body = "; ".join(t for t in terms_segments)
    trailer = " }"
    gate_part = f"  gates: {' + '.join(gates)}" if gates else ""
    return f"{header}{body}{trailer}  {gate_part}".rstrip()


# ---------------------------------------------------------------------------
# Lowering: payoff graph -> engine-facing views
# ---------------------------------------------------------------------------
def _graph_node(graph: Dict[str, Any], node_id: str) -> Dict[str, Any]:
    return next(node for node in graph.get("nodes", []) if node["node_id"] == node_id)


def _serials(graph: Dict[str, Any], name: str) -> int:
    return date_to_excel_serial(graph["dates"][name])


def lower_payoff_graph_to_fcn_terms(graph: Dict[str, Any]) -> Dict[str, Any]:
    """Lower the payoff graph to the flattened ``FcnTerms`` struct (``terms.hpp``)."""
    coupon_periods = []
    coupon_node = _graph_node(graph, "coupon_strip")
    for period in coupon_node.get("config", {}).get("periods", []):
        coupon_periods.append(
            {
                "id": f"coupon-{period.get('period', len(coupon_periods) + 1)}",
                "start_date": int(period["start_date"]),
                "end_date": int(period["end_date"]),
                "payment_date": int(period["payment_date"]),
                "range_rate": float(period.get("range_rate", 0.0)),
                "fixed_coupon": float(period.get("fixed_coupon", 0.0)),
                "lower_bound": float(period.get("lower_bound", 0.0)),
                "upper_bound": float(period.get("upper_bound", 1.0e12)),
                "already_paid_fixings": int(period.get("already_paid_fixings", 0)),
                "total_fixings": int(period.get("total_fixings", 0)),
                "coupon_barrier": float(period.get("coupon_barrier", 0.0)),
                "local_ko_barrier": float(period.get("local_ko_barrier", 0.0)),
                "local_ko_coupon": float(period.get("local_ko_coupon", 0.0)),
                "global_ko_coupon": float(period.get("global_ko_coupon", 0.0)),
            }
        )
    global_ko = _graph_node(graph, "global_ko_gate")["config"]
    terminal_cfg = _graph_node(graph, "down_and_in_put")["config"]
    funding_cfg = _graph_node(graph, "notional_return")["config"]
    settlement_kind = "physical" if terminal_cfg.get("settlement") == "physical_delivery" else "cash"
    return {
        "fcn_terms": {
            "instrument_key": graph.get("source_ref", {}).get("market_snapshot_ref") or graph["graph_id"],
            "request_id": "",
            "process_id": "",
            "currency": graph["currency"],
            "notional": float(graph["notional"]),
            "coupon_quote_scale": float(graph.get("coupon_quote_scale", 10.0)),
            "evaluation_date": _serials(graph, "evaluation_date"),
            "final_fixing_date": _serials(graph, "final_fixing_date"),
            "maturity_date": _serials(graph, "maturity_date"),
            "rate": float(graph["coupon"]["rate"]),
            "reference_spots": [float(value) for value in graph.get("reference_spots", [])],
            "coupon_periods": coupon_periods,
            "barriers": {
                "local_enabled": any(n["kind"] == "local_ko_gate" for n in graph.get("nodes", [])),
                "global_enabled": bool(global_ko.get("enabled", True)),
                "memory_ko": bool(global_ko.get("memory_ko", False)),
                "local_barrier": next((n["config"].get("barrier", 0.0) for n in graph.get("nodes", []) if n["kind"] == "local_ko_gate"), 0.0),
                "global_barrier": float(global_ko.get("barrier", 0.0)),
                "local_operator": ">=",
                "global_operator": str(global_ko.get("operator", ">=")),
                "same_day_ko_precedence": "global",
            },
            "terminal": {
                "ki_enabled": True,
                "ki_barrier": float(_graph_node(graph, "knock_in_gate")["config"].get("barrier", 0.0)),
                "ki_operator": str(_graph_node(graph, "knock_in_gate")["config"].get("operator", "<=")),
                "strike": float(terminal_cfg.get("strike", 0.0)),
                "settlement": settlement_kind,
            },
            "funding": {
                "enabled": bool(funding_cfg.get("enabled", True)),
                "return_ratio": float(funding_cfg.get("return_ratio", 1.0)),
            },
            "use_worst_of": True,
            "range_lower_inclusive": True,
            "range_upper_inclusive": True,
            "coupon_memory": bool(graph["coupon"]["memory_carry"]),
            "physical_delivery": terminal_cfg.get("settlement") == "physical_delivery",
            "source_revision": graph.get("provenance", {}).get("source_revision", ""),
        }
    }


def lower_payoff_graph_to_pricing_request(graph: Dict[str, Any]) -> Dict[str, Any]:
    """Lower the payoff graph to the ``pricing-request`` schema shape."""
    underlyings = [underlying["id"] for underlying in graph.get("market_data", {}).get("underlyings", [])]
    if not underlyings:
        underlyings = list(graph.get("reference_spots", []))
    coupon_cfg = _graph_node(graph, "coupon_strip")["config"]
    parameters = graph.get("parameters") or DEFAULT_PARAMETERS
    legs = []
    for leg in graph.get("legs", []):
        node = _graph_node(graph, leg["payoff_node"])
        payoff: Dict[str, Any] = {}
        if node["kind"] == "down_and_in_put":
            payoff = {
                "basket": "worst_of",
                "strike": float(node["config"]["strike"]),
                "knock_in": {
                    "monitoring": "EKI",
                    "barrier": float(_graph_node(graph, "knock_in_gate")["config"]["barrier"]),
                },
                "settlement": "physical_delivery" if node["config"].get("settlement") == "physical_delivery" else "cash",
            }
        elif node["kind"] == "notional_return":
            payoff = {"notional_return": True, "return_ratio": float(node["config"].get("return_ratio", 1.0))}
        elif node["kind"] == "coupon_strip":
            payoff = {
                "type": "range_accrual_strip",
                "indicator": coupon_cfg.get("indicator", "WPS"),
                "rate": float(coupon_cfg.get("rate", 0.0)),
                "unpaid_period_rule": coupon_cfg.get("unpaid_period_rule", "N2-N1"),
                "payment_lag_days": graph.get("coupon", {}).get("payment_lag_days", [2]),
            }
        legs.append(
            {
                "leg_id": leg["leg_id"],
                "leg_type": {"ki_put": "intrinsic_option", "funding": "funding", "coupon": "coupon"}.get(leg["role"], "custom"),
                "leg_name": leg["name"],
                "multiplier": float(leg["multiplier"]),
                "notional": float(graph["notional"]),
                "payoff": payoff,
            }
        )
    return {
        "instrument_key": graph["graph_id"],
        "market_data": graph.get("market_data") or {
            "evaluation_date": _serials(graph, "evaluation_date"),
            "underlyings": [],
            "curves": [],
            "vol_surfaces": [],
            "correlations": [],
        },
        "parameters": dict(parameters),
        "common_economics": {
            "notional": float(graph["notional"]),
            "payment_currency": graph["currency"],
            "underlyings": [str(value) for value in underlyings],
            "product_type": graph.get("product", {}).get("type", "ELIFCN"),
        },
        "legs": legs,
    }


def enrich_pricing_request(request: Dict[str, Any]) -> Dict[str, Any]:
    """Enrich a pricing-request with the risk-store fields for sensi/P&L storage."""
    from itertools import count as _count

    underlyings = request.get("common_economics", {}).get("underlyings", [])
    market = request.get("market_data", {})
    instrument_key = str(request.get("instrument_key", "instrument"))
    risk_keys: List[Dict[str, Any]] = []
    counter = _count(1)
    for underlying in underlyings:
        risk_keys.append({"rfk": f"EQ_SPOT_{underlying}", "risk_factor_type": "EQ_SPOT", "code": underlying, "currency": market.get("underlyings", [{}])[next(counter, 0) - 1].get("currency", "USD") if market.get("underlyings") else "USD", "path": f"equity/{underlying}/spot"})
    risk_keys.append({"rfk": "DISC_USD", "risk_factor_type": "DISC_CURVE", "code": "USD Std Curve", "currency": "USD", "path": "curves/discount/USD"})
    risk_keys.append({"rfk": "EQEQ_CORR", "risk_factor_type": "CORRELATION", "code": "+".join(str(u) for u in underlyings), "currency": "USD", "path": "correlation/equity"})
    lifecycle = request.get("updated_lifecycle", {})
    snapshot = {
        "evaluation_date": market.get("evaluation_date"),
        "as_of": market.get("evaluation_date"),
        "spot_count": len(market.get("underlyings", [])),
    }
    return {
        "InstrumentKey": {"instrument_id": instrument_key, "timestamp": None, "series": None},
        "UnwindMapRaw": {
            "instrument": {"name": instrument_key},
            "basket": [{"id": str(u), "weight": 1.0} for u in underlyings],
            "fx_pair": "USD.USD",
            "time_zone": "United States",
        },
        "RiskFactorKeys": risk_keys,
        "MarketDataSnapshot": snapshot,
        "UpdatedLifecycle": {
            "already_knock_in": lifecycle.get("already_knock_in", False),
            "memory_locked": lifecycle.get("memory_locked", []),
            "memory_dates": lifecycle.get("memory_dates", []),
            "coupon_fixings": lifecycle.get("coupon_fixings", []),
            "as_of": lifecycle.get("as_of") or market.get("evaluation_date"),
            "status": lifecycle.get("status", "LIVE"),
        },
        "CommonEconomics": {
            "payment_currency": request.get("common_economics", {}).get("payment_currency", "USD"),
            "notional": request.get("common_economics", {}).get("notional", 1.0),
            "underlyings": underlyings,
            "product_type": request.get("common_economics", {}).get("product_type", "ELIFCN"),
        },
        "parameters": request.get("parameters", {}),
    }


def compile_fcn_graph_and_lower(
    terms: Dict[str, Any],
    *,
    market: Optional[Dict[str, Any]] = None,
    lifecycle: Optional[Dict[str, Any]] = None,
    legacy: Optional[Dict[str, Any]] = None,
    parameters: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """ETL transform stage: emit the payoff graph, then lower it to all engine views."""
    graph = compile_payoff_graph(terms, market=market, lifecycle=lifecycle, legacy=legacy, parameters=parameters, product=None)
    pricing_request = lower_payoff_graph_to_pricing_request(graph)
    return {
        "payoff_graph": graph,
        "fcn_terms": lower_payoff_graph_to_fcn_terms(graph),
        "pricing_request": pricing_request,
        "enriched_request": enrich_pricing_request(pricing_request),
    }


DEFAULT_PARAMETERS: Dict[str, Any] = {
    "bump_size": 0.01,
    "bump_mode": "relative",
    "method_priority": ["AAD", "PATHWISE", "LRM", "FD"],
    "seed": 1729,
    "common_random_numbers": True,
    "paths": 30000,
    "steps": 252,
    "monitoring_frequency": "daily",
}


__all__ = [
    "DEFAULT_PARAMETERS",
    "ENGINE_MARKER",
    "FUNCTION_BINDINGS",
    "SCHEMA_VERSION",
    "compile_fcn_graph_and_lower",
    "compile_payoff_graph",
    "date_to_excel_serial",
    "enrich_pricing_request",
    "excel_serial_to_date",
    "graph_hash",
    "lower_payoff_graph_to_fcn_terms",
    "lower_payoff_graph_to_pricing_request",
    "render_payoff_script",
]