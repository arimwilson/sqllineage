# api/lineage.py
# Vercel picks up a Flask/Werkzeug WSGI app named `app`.
# Endpoints:
#   - POST /api/lineage        -> analyze lineage
#   - GET  /api/lineage        -> health
#
# Payloads supported:
# 1) Full:
#    {
#      "initial_catalog": { "tables": ["schema.table", {"schema":"ext","name":"s3://..."}] },
#      "statements": [ {"sql":"...", "enabled":true, "session_id":"...", "timestamp_ms":1700000000000} ],
#      "config": { "search_path":["public"], "name_mode":"FINAL_NAME", ... }
#    }
# 2) Convenience:
#    { "sql": "CREATE TABLE ...; INSERT INTO ... SELECT ..." }

from __future__ import annotations

import json
import re
from dataclasses import asdict, is_dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple, Set

from flask import Flask, request, Response

# ---- Import your real API & types from your module ----
# Ensure vertica_lineage.py is in the root or available on PYTHONPATH in Vercel.
from vertica_lineage import (
    analyze_lineage,
    InitialCatalog,
    Statement,
    AnalyzerConfig,
    NameResolutionMode,
    QualifiedName,
    Edge,
    EdgeOp,
    LineageResult,
    RenameEvent,
)

app = Flask(__name__)

# ---------- Helpers: parsing input ----------

def _parse_qualified_name(obj: Any, default_schema: str = "public") -> QualifiedName:
    """
    Accepts either:
      - {"schema": "...", "name": "..."}
      - "schema.name"
      - "ext://resource" or "kafka://topic"
      - "name" (falls back to default_schema)
    """
    if isinstance(obj, dict) and "schema" in obj and "name" in obj:
        return QualifiedName(schema=obj["schema"], name=obj["name"])

    if not isinstance(obj, str):
        raise ValueError(f"QualifiedName must be dict or string, got: {type(obj)}")

    # ext:// and kafka:// pass through as special schemas
    if obj.startswith("ext://"):
        return QualifiedName(schema="ext", name=obj[len("ext://"):])
    if obj.startswith("kafka://"):
        return QualifiedName(schema="kafka", name=obj[len("kafka://"):])

    if "." in obj:
        schema, name = obj.split(".", 1)
        return QualifiedName(schema=schema, name=name)
    else:
        # Bare name -> assume default schema
        return QualifiedName(schema=default_schema, name=obj)

def _parse_initial_catalog(data: Optional[Dict[str, Any]], default_schema: str) -> InitialCatalog:
    tables: Set[QualifiedName] = set()
    if data and isinstance(data.get("tables"), list):
        for t in data["tables"]:
            tables.add(_parse_qualified_name(t, default_schema))
    return InitialCatalog(tables=tables)

def _parse_statements(items: Any) -> List[Statement]:
    """
    Accepts either:
      - [{"sql": "...", "enabled": true, "session_id": "...", "timestamp_ms": 1700}, ...]
      - ["SQL TEXT", "ANOTHER SQL TEXT", ...]
    """
    if not items:
        return []

    out: List[Statement] = []
    for it in items:
        if isinstance(it, str):
            out.append(Statement(sql=it))
        elif isinstance(it, dict):
            out.append(
                Statement(
                    sql=it.get("sql", ""),
                    enabled=bool(it.get("enabled", True)),
                    session_id=it.get("session_id"),
                    timestamp_ms=it.get("timestamp_ms"),
                )
            )
        else:
            raise ValueError(f"Unsupported statement item: {type(it)}")
    return out

def _coerce_name_mode(v: Any) -> NameResolutionMode:
    if isinstance(v, NameResolutionMode):
        return v
    if isinstance(v, str):
        return NameResolutionMode[v]
    if isinstance(v, int):
        # Allow integer enum index (1-based in auto()), map defensively
        for m in NameResolutionMode:
            if m.value == v:
                return m
    raise ValueError(f"Invalid name_mode: {v}")

def _parse_config(cfg: Optional[Dict[str, Any]]) -> AnalyzerConfig:
    if not cfg:
        return AnalyzerConfig()

    # Start with defaults, patch known fields if present
    c = AnalyzerConfig()
    for field_name in [
        "search_path", "default_schema", "include_view_edges", "expand_views_to_bases",
        "include_temp_tables", "compress_temps_in_output", "include_negative_edges",
        "include_external_sources", "ignore_disabled_statements", "tolerate_parse_errors",
        "case_sensitive_quoted", "track_partitions_on_edges",
    ]:
        if field_name in cfg:
            setattr(c, field_name, cfg[field_name])

    if "name_mode" in cfg:
        setattr(c, "name_mode", _coerce_name_mode(cfg["name_mode"]))

    return c

# ---------- Helpers: serializing output ----------

def _qn_to_str(qn: QualifiedName) -> str:
    # Use the module's render() to respect quoting and special schemas.
    return qn.render()

def _edge_to_dict(e: Edge) -> Dict[str, Any]:
    return {
        "src": _qn_to_str(e.src),
        "dst": _qn_to_str(e.dst),
        "op": e.op.name if isinstance(e.op, EdgeOp) else str(e.op),
        "stmt_index": e.stmt_index,
        "session_id": e.session_id,
        "timestamp_ms": e.timestamp_ms,
        "meta": dict(e.meta or {}),
    }

def _rename_to_dict(r: RenameEvent) -> Dict[str, Any]:
    return {
        "old": _qn_to_str(r.old),
        "new": _qn_to_str(r.new),
        "stmt_index": r.stmt_index,
        "session_id": r.session_id,
        "timestamp_ms": r.timestamp_ms,
    }

def _lineage_to_json(result: LineageResult) -> Dict[str, Any]:
    # Ensure JSON-safe types (sets -> lists; enums -> names; dataclasses -> dicts)
    return {
        "edges": [ [a, b] for (a, b) in (result.edges or []) ],
        "detailed_edges": [ _edge_to_dict(e) for e in (result.detailed_edges or []) ],
        "ephemeral_tables": sorted(list(result.ephemeral_tables or [])),
        "live_tables": sorted(list(result.live_tables or [])),
        "renames": [ _rename_to_dict(r) for r in (result.renames or []) ],
        # JSON requires string keys; use str(index)
        "warnings": { str(k): list(v) for k, v in (result.warnings or {}).items() },
    }

# ---------- CORS ----------

def _corsify(resp: Response) -> Response:
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.headers["Access-Control-Allow-Methods"] = "GET,POST,OPTIONS"
    resp.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
    return resp

@app.after_request
def add_cors_headers(resp: Response):
    return _corsify(resp)

@app.route("/", methods=["OPTIONS"])
def options_root():
    return _corsify(Response(status=204))

# ---------- Routes ----------

@app.route("/", methods=["GET"])
def health() -> Response:
    return _corsify(Response(response=json.dumps({"ok": True}), mimetype="application/json"))

@app.route("/", methods=["POST"])
def analyze() -> Response:
    try:
        payload = request.get_json(force=True, silent=False)
    except Exception:
        return _corsify(Response(json.dumps({"error": "Invalid JSON"}), status=400, mimetype="application/json"))

    if not isinstance(payload, dict):
        return _corsify(Response(json.dumps({"error": "JSON object expected"}), status=400, mimetype="application/json"))

    # Convenience mode: { "sql": "..." }
    if "sql" in payload and isinstance(payload["sql"], str):
        statements = [Statement(sql=payload["sql"])]
        initial_catalog = InitialCatalog(tables=set())
        config = AnalyzerConfig()
    else:
        # Full mode
        cfg_raw = payload.get("config") or {}
        config = _parse_config(cfg_raw)

        initial_catalog = _parse_initial_catalog(payload.get("initial_catalog"), default_schema=config.default_schema)

        statements_raw = payload.get("statements")
        statements = _parse_statements(statements_raw)

        if not statements:
            return _corsify(
                Response(json.dumps({"error": "No statements provided (use `statements` or `sql`)"}),
                         status=400, mimetype="application/json")
            )

    try:
        result = analyze_lineage(initial_catalog=initial_catalog, statements=statements, config=config)
    except Exception as e:
        # Return a concise error to the caller; avoid leaking internals
        return _corsify(
            Response(json.dumps({"error": "analyze_lineage failed", "detail": str(e)}),
                     status=500, mimetype="application/json")
        )

    out = _lineage_to_json(result)
    return _corsify(Response(response=json.dumps(out), mimetype="application/json"))

# Map the function path too (Vercel passes the full path to the app)
@app.route("/api/lineage", methods=["OPTIONS"])
def options_lineage():
    return _corsify(Response(status=204))

@app.route("/api/lineage", methods=["GET"])
def health_alias():
    return health()

@app.route("/api/lineage", methods=["POST"])
def analyze_alias():
    return analyze()

