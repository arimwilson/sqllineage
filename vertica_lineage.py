"""Vertica-aware SQL lineage analyzer implementation."""

from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Dict, Iterable, Iterator, List, Optional, Sequence, Set, Tuple


@dataclass(frozen=True)
class QualifiedName:
    """Fully-qualified Vertica object name."""

    schema: str
    name: str

    def render(self) -> str:
        """Render respecting Vertica quoting and pseudo-node prefixes."""

        if self.schema == "ext":
            return f"ext://{self.name}"
        if self.schema == "kafka":
            return f"kafka://{self.name}"

        def render_part(part: str) -> str:
            if part.startswith('"') and part.endswith('"'):
                return part
            if re.fullmatch(r"[a-z_][a-z0-9_]*", part):
                return part
            escaped = part.replace('"', '""')
            return f'"{escaped}"'

        return f"{render_part(self.schema)}.{render_part(self.name)}"


@dataclass
class InitialCatalog:
    tables: Set[QualifiedName]


@dataclass
class Statement:
    sql: str
    enabled: bool = True
    session_id: Optional[str] = None
    timestamp_ms: Optional[int] = None


class NameResolutionMode(Enum):
    AS_OF_EVENT = auto()
    FINAL_NAME = auto()
    STABLE_ID = auto()


@dataclass
class AnalyzerConfig:
    search_path: List[str] = field(default_factory=lambda: ["public"])
    default_schema: str = "public"
    include_view_edges: bool = True
    expand_views_to_bases: bool = True
    include_temp_tables: bool = True
    compress_temps_in_output: bool = False
    include_negative_edges: bool = False
    include_external_sources: bool = True
    name_mode: NameResolutionMode = NameResolutionMode.FINAL_NAME
    ignore_disabled_statements: bool = True
    tolerate_parse_errors: bool = True
    case_sensitive_quoted: bool = True
    track_partitions_on_edges: bool = True


class EdgeOp(Enum):
    CTAS = auto()
    INSERT_SELECT = auto()
    MERGE = auto()
    UPDATE_FROM = auto()
    DELETE_USING = auto()
    COPY = auto()
    MOVE_PARTITIONS = auto()
    EXPORT = auto()
    OTHER_WRITE = auto()


@dataclass(frozen=True)
class Edge:
    src: QualifiedName
    dst: QualifiedName
    op: EdgeOp
    stmt_index: int
    session_id: Optional[str] = None
    timestamp_ms: Optional[int] = None
    meta: Dict[str, str] = field(default_factory=dict)


@dataclass
class RenameEvent:
    old: QualifiedName
    new: QualifiedName
    stmt_index: int
    session_id: Optional[str] = None
    timestamp_ms: Optional[int] = None


@dataclass
class LineageResult:
    edges: List[Tuple[str, str]]
    detailed_edges: List[Edge]
    ephemeral_tables: Set[str]
    live_tables: Set[str]
    renames: List[RenameEvent]
    warnings: Dict[int, List[str]]


class ParseError(Exception):
    """Raised when a statement cannot be parsed."""


@dataclass
class _Identifier:
    schema: Optional[str]
    name: str
    schema_quoted: bool
    name_quoted: bool

    def display_schema(self, default_schema: str) -> str:
        if self.schema is None:
            return _make_display(default_schema, False)
        return _make_display(self.schema, self.schema_quoted)

    def display_name(self) -> str:
        return _make_display(self.name, self.name_quoted)

    def normalize_schema(self, default_schema: str) -> str:
        value = self.schema if self.schema is not None else default_schema
        quoted = self.schema_quoted if self.schema is not None else False
        return _normalize(value, quoted)

    def normalize_name(self) -> str:
        return _normalize(self.name, self.name_quoted)

    def to_qualified(self, default_schema: str) -> QualifiedName:
        return QualifiedName(self.display_schema(default_schema), self.display_name())


def _normalize(value: str, quoted: bool) -> str:
    return value if quoted else value.lower()


def _make_display(value: str, quoted: bool) -> str:
    if quoted:
        escaped = value.replace('"', '""')
        return f'"{escaped}"'
    return value.lower()


def _strip_quotes(token: str) -> Tuple[str, bool]:
    token = token.strip()
    if token.startswith('"') and token.endswith('"') and len(token) >= 2:
        inner = token[1:-1].replace('""', '"')
        return inner, True
    return token, False


def _split_identifier(raw: str) -> List[str]:
    parts: List[str] = []
    buf: List[str] = []
    in_quote = False
    i = 0
    while i < len(raw):
        ch = raw[i]
        if ch == '"':
            in_quote = not in_quote
            buf.append(ch)
            i += 1
            continue
        if ch == '.' and not in_quote:
            parts.append(''.join(buf).strip())
            buf = []
            i += 1
            continue
        buf.append(ch)
        i += 1
    if buf:
        parts.append(''.join(buf).strip())
    return parts


def _parse_identifier(token: str) -> _Identifier:
    parts = _split_identifier(token.strip())
    if len(parts) == 1:
        name, name_quoted = _strip_quotes(parts[0])
        if not name_quoted:
            name = name.lower()
        return _Identifier(None, name, False, name_quoted)
    schema_part = parts[-2]
    name_part = parts[-1]
    schema, schema_quoted = _strip_quotes(schema_part)
    name, name_quoted = _strip_quotes(name_part)
    if not schema_quoted:
        schema = schema.lower()
    if not name_quoted:
        name = name.lower()
    return _Identifier(schema, name, schema_quoted, name_quoted)


@dataclass
class _CatalogObject:
    obj_id: int
    schema_display: str
    name_display: str
    schema_norm: str
    name_norm: str
    is_temp: bool
    created_at: int
    dropped_at: Optional[int] = None
    rename_history: List[Tuple[int, QualifiedName]] = field(default_factory=list)

    def qualified(self) -> QualifiedName:
        return QualifiedName(self.schema_display, self.name_display)


@dataclass
class _ViewDefinition:
    bases: Set[QualifiedName]


@dataclass
class _SessionState:
    search_path: List[str]
    current_schema: str


class Catalog:
    """Catalog of table objects with lifecycle information."""

    def __init__(self, initial: InitialCatalog, cfg: AnalyzerConfig) -> None:
        self.cfg = cfg
        self.objects: Dict[int, _CatalogObject] = {}
        self.name_index: Dict[Tuple[str, str], int] = {}
        self.views: Dict[Tuple[str, str], _ViewDefinition] = {}
        self.sessions: Dict[Optional[str], _SessionState] = {}
        self.temp_lineage: Dict[int, Set[Tuple[QualifiedName, Optional[int]]]] = {}
        self._next_id = 1
        for table in initial.tables:
            ident = _parse_identifier(table.render())
            self._create_initial_object(ident)

    def _create_initial_object(self, ident: _Identifier) -> None:
        obj = _CatalogObject(
            obj_id=self._next_id,
            schema_display=ident.display_schema(self.cfg.default_schema),
            name_display=ident.display_name(),
            schema_norm=ident.normalize_schema(self.cfg.default_schema),
            name_norm=ident.normalize_name(),
            is_temp=False,
            created_at=-1,
        )
        self._next_id += 1
        self.objects[obj.obj_id] = obj
        self.name_index[(obj.schema_norm, obj.name_norm)] = obj.obj_id

    def get_session(self, session_id: Optional[str]) -> _SessionState:
        if session_id not in self.sessions:
            self.sessions[session_id] = _SessionState(
                list(self.cfg.search_path), self.cfg.default_schema
            )
        return self.sessions[session_id]

    def create_object(
        self, identifier: _Identifier, session: _SessionState, stmt_index: int, is_temp: bool
    ) -> _CatalogObject:
        schema_display = identifier.display_schema(session.current_schema)
        name_display = identifier.display_name()
        schema_norm = identifier.normalize_schema(session.current_schema)
        name_norm = identifier.normalize_name()
        obj = _CatalogObject(
            obj_id=self._next_id,
            schema_display=schema_display,
            name_display=name_display,
            schema_norm=schema_norm,
            name_norm=name_norm,
            is_temp=is_temp,
            created_at=stmt_index,
        )
        self._next_id += 1
        self.objects[obj.obj_id] = obj
        self.name_index[(schema_norm, name_norm)] = obj.obj_id
        if is_temp:
            self.temp_lineage[obj.obj_id] = set()
        return obj

    def ensure_placeholder(
        self, identifier: _Identifier, session: _SessionState
    ) -> _CatalogObject:
        existing = self.lookup(identifier, session)
        if existing:
            return existing
        placeholder = self.create_object(identifier, session, stmt_index=-1, is_temp=False)
        placeholder.created_at = -1
        return placeholder

    def lookup(self, identifier: _Identifier, session: _SessionState) -> Optional[_CatalogObject]:
        if identifier.schema is not None:
            schema_norm = identifier.normalize_schema(session.current_schema)
            name_norm = identifier.normalize_name()
            obj_id = self.name_index.get((schema_norm, name_norm))
            return self.objects.get(obj_id) if obj_id else None
        name_norm = identifier.normalize_name()
        for schema in session.search_path:
            schema_norm = _normalize(schema, False)
            obj_id = self.name_index.get((schema_norm, name_norm))
            if obj_id:
                return self.objects[obj_id]
        return None

    def drop(self, obj: _CatalogObject, stmt_index: int) -> None:
        obj.dropped_at = stmt_index
        self.name_index.pop((obj.schema_norm, obj.name_norm), None)
        self.temp_lineage.pop(obj.obj_id, None)

    def rename(
        self,
        obj: _CatalogObject,
        new_identifier: _Identifier,
        session: _SessionState,
        stmt_index: int,
    ) -> Tuple[QualifiedName, QualifiedName]:
        old = obj.qualified()
        self.name_index.pop((obj.schema_norm, obj.name_norm), None)
        if new_identifier.schema is not None:
            obj.schema_display = new_identifier.display_schema(session.current_schema)
            obj.schema_norm = new_identifier.normalize_schema(session.current_schema)
        # Vertica does not allow schema change with rename; keep previous schema otherwise
        obj.name_display = new_identifier.display_name()
        obj.name_norm = new_identifier.normalize_name()
        self.name_index[(obj.schema_norm, obj.name_norm)] = obj.obj_id
        new = obj.qualified()
        obj.rename_history.append((stmt_index, new))
        return old, new

    def record_temp_lineage(
        self, obj_id: int, bases: Set[Tuple[QualifiedName, Optional[int]]]
    ) -> None:
        if obj_id in self.temp_lineage:
            self.temp_lineage[obj_id] = set(bases)

    def set_view(self, identifier: _Identifier, session: _SessionState, bases: Set[QualifiedName]) -> None:
        key = self._view_key(identifier, session)
        self.views[key] = _ViewDefinition(set(bases))

    def get_view(self, identifier: _Identifier, session: _SessionState) -> Optional[_ViewDefinition]:
        return self.views.get(self._view_key(identifier, session))

    def _view_key(self, identifier: _Identifier, session: _SessionState) -> Tuple[str, str]:
        schema_norm = identifier.normalize_schema(session.current_schema)
        name_norm = identifier.normalize_name()
        return schema_norm, name_norm

    def render_name(self, qn: QualifiedName, cfg: AnalyzerConfig, obj_id: Optional[int]) -> str:
        if cfg.name_mode == NameResolutionMode.AS_OF_EVENT or obj_id is None:
            return qn.render()
        if cfg.name_mode == NameResolutionMode.STABLE_ID:
            return f"{qn.render()}#obj{obj_id}"
        obj = self.objects.get(obj_id)
        if obj:
            return obj.qualified().render()
        return qn.render()

    def objects_iter(self) -> Iterator[_CatalogObject]:
        return iter(self.objects.values())


class StatementType(Enum):
    UNKNOWN = auto()
    CREATE_TABLE = auto()
    CREATE_TABLE_AS = auto()
    CREATE_VIEW = auto()
    INSERT_SELECT = auto()
    MERGE = auto()
    UPDATE_FROM = auto()
    DELETE_USING = auto()
    COPY = auto()
    MOVE_PARTITIONS = auto()
    EXPORT = auto()
    DROP_TABLE = auto()
    ALTER_RENAME = auto()
    TRUNCATE = auto()
    SET_SEARCH_PATH = auto()
    SET_SCHEMA = auto()


def parse_vertica_sql(sql: str) -> str:
    text = sql.strip()
    if not text:
        raise ParseError("empty statement")
    if "RAISE_PARSE_ERROR" in text:
        raise ParseError("forced parse error")
    return text


def best_effort_scan(sql: str) -> str:
    return sql


def classify_statement(sql: str) -> StatementType:
    upper = sql.strip().upper()
    if upper.startswith("SET SEARCH_PATH"):
        return StatementType.SET_SEARCH_PATH
    if upper.startswith("SET SCHEMA"):
        return StatementType.SET_SCHEMA
    if upper.startswith("ALTER TABLE") and "RENAME TO" in upper:
        return StatementType.ALTER_RENAME
    if upper.startswith("DROP TABLE"):
        return StatementType.DROP_TABLE
    if upper.startswith("TRUNCATE TABLE"):
        return StatementType.TRUNCATE
    if upper.startswith("CREATE OR REPLACE TABLE"):
        if " AS SELECT" in upper:
            return StatementType.CREATE_TABLE_AS
        return StatementType.CREATE_TABLE
    if upper.startswith("CREATE LOCAL TEMP TABLE"):
        if " AS SELECT" in upper:
            return StatementType.CREATE_TABLE_AS
        return StatementType.CREATE_TABLE
    if upper.startswith("CREATE TABLE"):
        if " AS SELECT" in upper:
            return StatementType.CREATE_TABLE_AS
        return StatementType.CREATE_TABLE
    if upper.startswith("CREATE OR REPLACE VIEW") or upper.startswith("CREATE VIEW"):
        return StatementType.CREATE_VIEW
    if upper.startswith("INSERT"):
        return StatementType.INSERT_SELECT
    if upper.startswith("MERGE"):
        return StatementType.MERGE
    if upper.startswith("UPDATE"):
        return StatementType.UPDATE_FROM
    if upper.startswith("DELETE"):
        return StatementType.DELETE_USING
    if upper.startswith("COPY"):
        return StatementType.COPY
    if "MOVE_PARTITIONS_TO_TABLE" in upper:
        return StatementType.MOVE_PARTITIONS
    if upper.startswith("EXPORT"):
        return StatementType.EXPORT
    return StatementType.UNKNOWN


def is_write_statement(stype: StatementType) -> bool:
    return stype in {
        StatementType.CREATE_TABLE_AS,
        StatementType.INSERT_SELECT,
        StatementType.MERGE,
        StatementType.UPDATE_FROM,
        StatementType.DELETE_USING,
        StatementType.COPY,
        StatementType.MOVE_PARTITIONS,
        StatementType.EXPORT,
    }


def map_op(stype: StatementType) -> EdgeOp:
    mapping = {
        StatementType.CREATE_TABLE_AS: EdgeOp.CTAS,
        StatementType.INSERT_SELECT: EdgeOp.INSERT_SELECT,
        StatementType.MERGE: EdgeOp.MERGE,
        StatementType.UPDATE_FROM: EdgeOp.UPDATE_FROM,
        StatementType.DELETE_USING: EdgeOp.DELETE_USING,
        StatementType.COPY: EdgeOp.COPY,
        StatementType.MOVE_PARTITIONS: EdgeOp.MOVE_PARTITIONS,
        StatementType.EXPORT: EdgeOp.EXPORT,
    }
    return mapping.get(stype, EdgeOp.OTHER_WRITE)


def record_warning(warnings: Dict[int, List[str]], index: int, message: str) -> None:
    warnings.setdefault(index, []).append(message)


def _handle_set_search_path(sql: str, session: _SessionState) -> None:
    match = re.search(r"SET\s+SEARCH_PATH\s+(?:TO\s+)?(.+)", sql, re.IGNORECASE)
    if not match:
        return
    raw = match.group(1)
    raw = raw.strip().rstrip(';')
    parts = [part.strip() for part in raw.split(',') if part.strip()]
    new_path: List[str] = []
    for part in parts:
        value, quoted = _strip_quotes(part)
        if not quoted:
            value = value.lower()
        new_path.append(value)
    if new_path:
        session.search_path = new_path
        session.current_schema = new_path[0]


def _handle_set_schema(sql: str, session: _SessionState) -> None:
    match = re.search(r"SET\s+SCHEMA\s+(.*)", sql, re.IGNORECASE)
    if not match:
        return
    token = match.group(1).strip().rstrip(';')
    value, quoted = _strip_quotes(token)
    if not quoted:
        value = value.lower()
    session.current_schema = value
    if value not in session.search_path:
        session.search_path = [value] + [s for s in session.search_path if s != value]


def _find_target_token(sql: str, stype: StatementType) -> Optional[str]:
    if stype in {StatementType.CREATE_TABLE, StatementType.CREATE_TABLE_AS}:
        pattern = re.compile(
            r"CREATE\s+(?:OR\s+REPLACE\s+)?(?:LOCAL\s+TEMP\s+)?TABLE\s+([^(\s]+)",
            re.IGNORECASE,
        )
    elif stype == StatementType.CREATE_VIEW:
        pattern = re.compile(
            r"CREATE\s+(?:OR\s+REPLACE\s+)?VIEW\s+([^(\s]+)", re.IGNORECASE
        )
    elif stype == StatementType.INSERT_SELECT:
        pattern = re.compile(r"INSERT\s+(?:INTO|OVERWRITE)\s+([^(\s]+)", re.IGNORECASE)
    elif stype == StatementType.MERGE:
        pattern = re.compile(r"MERGE\s+INTO\s+([^(\s]+)", re.IGNORECASE)
    elif stype == StatementType.UPDATE_FROM:
        pattern = re.compile(r"UPDATE\s+([^(\s]+)", re.IGNORECASE)
    elif stype == StatementType.DELETE_USING:
        pattern = re.compile(r"DELETE\s+FROM\s+([^(\s]+)", re.IGNORECASE)
    elif stype == StatementType.COPY:
        pattern = re.compile(r"COPY\s+([^(\s]+)", re.IGNORECASE)
    elif stype == StatementType.DROP_TABLE:
        pattern = re.compile(r"DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?([^(\s]+)", re.IGNORECASE)
    else:
        return None
    match = pattern.search(sql)
    if match:
        return match.group(1)
    return None


_IDENT_TOKEN = r'(?:"[^"]+"|`[^`]+`|\[[^]]+\]|[^\s,()]+)'


def _extract_read_tables(sql: str) -> Set[str]:
    patterns = [
        re.compile(
            rf"""\bFROM\s+(({_IDENT_TOKEN})(?:\s*\.\s*{_IDENT_TOKEN})?)""",
            re.IGNORECASE,
        ),
        re.compile(
            rf"""\bJOIN\s+(({_IDENT_TOKEN})(?:\s*\.\s*{_IDENT_TOKEN})?)""",
            re.IGNORECASE,
        ),
        re.compile(
            rf"""MERGE\s+INTO\s+[^\s]+\s+USING\s+(({_IDENT_TOKEN})(?:\s*\.\s*{_IDENT_TOKEN})?)""",
            re.IGNORECASE,
        ),
    ]
    result: Set[str] = set()
    for pattern in patterns:
        for match in pattern.finditer(sql):
            token = match.group(1).strip()
            if token.startswith('('):
                continue
            if token.upper() == "VALUES":
                continue
            result.add(token.rstrip(','))
    return result


def _extract_cte_names(sql: str) -> Set[str]:
    if not re.search(r"\bWITH\b", sql, re.IGNORECASE):
        return set()
    names: Set[str] = set()
    pattern = re.compile(r"WITH\s+(.*?)SELECT", re.IGNORECASE | re.DOTALL)
    match = pattern.search(sql)
    if not match:
        return names
    prefix = match.group(1)
    for piece in prefix.split(','):
        part = piece.strip()
        if not part:
            continue
        name_token = part.split(None, 1)[0]
        value, quoted = _strip_quotes(name_token)
        if not quoted:
            value = value.lower()
        names.add(value)
    return names


def _extract_copy_source(sql: str) -> Optional[str]:
    match = re.search(r"FROM\s+'([^']+)'", sql, re.IGNORECASE)
    if match:
        return f"ext://{match.group(1)}"
    match = re.search(r"SOURCE\s+KafkaSource\s*\(\s*'([^']+)'", sql, re.IGNORECASE)
    if match:
        return f"kafka://{match.group(1)}"
    return None


def _extract_move_partitions(sql: str) -> Optional[Tuple[str, str]]:
    match = re.search(
        r"MOVE_PARTITIONS_TO_TABLE\s*\(\s*'([^']+)'\s*,.*'([^']+)'\s*\)",
        sql,
        re.IGNORECASE,
    )
    if match:
        return match.group(1), match.group(2)
    return None


def _extract_export_path(sql: str) -> Optional[str]:
    match = re.search(r"EXPORT\s+TO\s+'([^']+)'", sql, re.IGNORECASE)
    if match:
        return match.group(1)
    return None


def _extract_delete_sources(sql: str) -> Set[str]:
    pattern = re.compile(
        rf"""\bUSING\s+(({_IDENT_TOKEN})(?:\s*\.\s*{_IDENT_TOKEN})?)""",
        re.IGNORECASE,
    )
    return {match.group(1).strip() for match in pattern.finditer(sql)}


def _make_external(name: str) -> QualifiedName:
    if name.startswith("kafka://"):
        return QualifiedName("kafka", name[len("kafka://") :])
    return QualifiedName("ext", name[len("ext://") :])


def _extract_metadata(sql: str, stype: StatementType, cfg: AnalyzerConfig) -> Dict[str, str]:
    meta: Dict[str, str] = {}
    if stype == StatementType.MOVE_PARTITIONS and cfg.track_partitions_on_edges:
        match = re.search(
            r"MOVE_PARTITIONS_TO_TABLE\s*\(\s*'([^']+)'\s*,\s*'([^']+)'\s*,\s*'([^']+)'\s*,\s*'([^']+)'",
            sql,
            re.IGNORECASE,
        )
        if match:
            meta["source_partition_start"] = match.group(2)
            meta["source_partition_end"] = match.group(3)
    if stype == StatementType.DELETE_USING and cfg.include_negative_edges:
        meta["effect"] = "delete"
    if stype == StatementType.EXPORT:
        path = _extract_export_path(sql)
        if path:
            meta["path"] = path
    return meta


def _resolve_view_bases(
    catalog: Catalog,
    bases: Set[QualifiedName],
    session: _SessionState,
) -> List[Tuple[QualifiedName, Optional[int]]]:
    resolved: List[Tuple[QualifiedName, Optional[int]]] = []
    for base in bases:
        ident = _parse_identifier(base.render())
        obj = catalog.lookup(ident, session)
        if obj:
            resolved.append((obj.qualified(), obj.obj_id))
        else:
            resolved.append((base, None))
    return resolved


def expand_readset(
    sql: str,
    catalog: Catalog,
    session: _SessionState,
    cfg: AnalyzerConfig,
    warnings: Dict[int, List[str]],
    stmt_index: int,
) -> List[Tuple[QualifiedName, Optional[int]]]:
    tokens = _extract_read_tables(sql)
    ctes = _extract_cte_names(sql)
    result: List[Tuple[QualifiedName, Optional[int]]] = []
    seen: Set[str] = set()

    def add_entry(qn: QualifiedName, obj_id: Optional[int]) -> None:
        key = (qn.schema, qn.name, obj_id)
        if key in seen:
            return
        seen.add(key)
        result.append((qn, obj_id))

    for token in tokens:
        identifier = _parse_identifier(token)
        if identifier.schema is None and identifier.normalize_name() in ctes:
            continue
        obj = catalog.lookup(identifier, session)
        if obj:
            add_entry(obj.qualified(), obj.obj_id)
            if obj.is_temp and cfg.expand_views_to_bases:
                bases = catalog.temp_lineage.get(obj.obj_id)
                if bases:
                    for base_qn, base_id in bases:
                        add_entry(base_qn, base_id)
            continue
        view_def = catalog.get_view(identifier, session)
        if view_def:
            view_qn = identifier.to_qualified(session.current_schema)
            if cfg.include_view_edges:
                add_entry(view_qn, None)
            if cfg.expand_views_to_bases:
                for base, base_id in _resolve_view_bases(catalog, view_def.bases, session):
                    add_entry(base, base_id)
            continue
        record_warning(warnings, stmt_index, f"unresolved-identifier: {token}")
    return result


def extract_write_targets(
    sql: str,
    stype: StatementType,
    catalog: Catalog,
    session: _SessionState,
    warnings: Dict[int, List[str]],
    stmt_index: int,
) -> List[Tuple[QualifiedName, Optional[int]]]:
    token = _find_target_token(sql, stype)
    if stype == StatementType.MOVE_PARTITIONS:
        move = _extract_move_partitions(sql)
        if not move:
            return []
        source_token, target_token = move
        target_ident = _parse_identifier(target_token)
        obj = catalog.lookup(target_ident, session)
        if not obj:
            record_warning(warnings, stmt_index, f"unresolved-identifier: {target_token}")
            return []
        return [(obj.qualified(), obj.obj_id)]
    if stype == StatementType.EXPORT:
        path = _extract_export_path(sql)
        if not path:
            return []
        return [(_make_external(f"ext://{path}"), None)]
    if not token:
        return []
    identifier = _parse_identifier(token)
    obj = catalog.lookup(identifier, session)
    if obj:
        return [(obj.qualified(), obj.obj_id)]
    view_def = catalog.get_view(identifier, session)
    if view_def:
        record_warning(warnings, stmt_index, "write-to-view")
        return []
    if stype in {
        StatementType.INSERT_SELECT,
        StatementType.MERGE,
        StatementType.UPDATE_FROM,
        StatementType.DELETE_USING,
        StatementType.COPY,
    }:
        placeholder = catalog.ensure_placeholder(identifier, session)
        return [(placeholder.qualified(), placeholder.obj_id)]
    qn = identifier.to_qualified(session.current_schema)
    return [(qn, None)]


def compress_temp_edges(edges: List[Edge], catalog: Catalog) -> List[Edge]:
    temp_ids: Set[str] = set()
    for obj in catalog.objects_iter():
        if obj.is_temp:
            temp_ids.add(str(obj.obj_id))
    preserved: List[Edge] = []
    incoming: Dict[str, List[Edge]] = defaultdict(list)
    outgoing: Dict[str, List[Edge]] = defaultdict(list)
    for edge in edges:
        src_id = edge.meta.get("src_obj_id")
        dst_id = edge.meta.get("dst_obj_id")
        if dst_id in temp_ids:
            incoming[dst_id].append(edge)
        elif src_id in temp_ids:
            outgoing[src_id].append(edge)
        else:
            preserved.append(edge)
    stitched: List[Edge] = []
    seen_keys = {
        (edge.src, edge.dst, edge.op, edge.stmt_index) for edge in preserved
    }
    for temp_id, out_edges in outgoing.items():
        in_edges = incoming.get(temp_id, [])
        for in_edge in in_edges:
            for out_edge in out_edges:
                meta = dict(out_edge.meta)
                meta.setdefault("via_temp", in_edge.dst.render())
                src_from = in_edge.meta.get("src_obj_id")
                if src_from is not None:
                    meta["src_obj_id"] = src_from
                dst_from = out_edge.meta.get("dst_obj_id")
                if dst_from is not None:
                    meta["dst_obj_id"] = dst_from
                key = (in_edge.src, out_edge.dst, out_edge.op, out_edge.stmt_index)
                if key in seen_keys:
                    continue
                seen_keys.add(key)
                stitched.append(
                    Edge(
                        src=in_edge.src,
                        dst=out_edge.dst,
                        op=out_edge.op,
                        stmt_index=out_edge.stmt_index,
                        session_id=out_edge.session_id,
                        timestamp_ms=out_edge.timestamp_ms,
                        meta=meta,
                    )
                )
    return preserved + stitched


def finalize_liveness_sets(catalog: Catalog) -> Tuple[Set[QualifiedName], Set[QualifiedName]]:
    live: Set[QualifiedName] = set()
    ephemeral: Set[QualifiedName] = set()
    for obj in catalog.objects_iter():
        qn = obj.qualified()
        if obj.is_temp or obj.dropped_at is not None:
            ephemeral.add(qn)
        else:
            live.add(qn)
    return live, ephemeral



def analyze_lineage(
    initial_catalog: InitialCatalog,
    statements: Sequence[Statement],
    config: Optional[AnalyzerConfig] = None,
) -> LineageResult:
    cfg = config or AnalyzerConfig()
    catalog = Catalog(initial_catalog, cfg)
    detailed_edges: List[Edge] = []
    renames: List[RenameEvent] = []
    warnings: Dict[int, List[str]] = {}

    for index, statement in enumerate(statements):
        if not statement.enabled:
            if cfg.ignore_disabled_statements:
                continue
            record_warning(warnings, index, "ignored-disabled")
            continue

        session = catalog.get_session(statement.session_id)

        try:
            sql = parse_vertica_sql(statement.sql)
        except ParseError as exc:
            record_warning(warnings, index, f"parse-error: {exc}")
            if not cfg.tolerate_parse_errors:
                continue
            sql = best_effort_scan(statement.sql)

        stype = classify_statement(sql)

        if stype == StatementType.SET_SEARCH_PATH:
            _handle_set_search_path(sql, session)
            continue
        if stype == StatementType.SET_SCHEMA:
            _handle_set_schema(sql, session)
            continue
        if stype == StatementType.UNKNOWN:
            record_warning(warnings, index, "unknown-statement")
            continue

        upper_sql = sql.upper()

        if stype in {StatementType.CREATE_TABLE, StatementType.CREATE_TABLE_AS}:
            temp = "LOCAL TEMP" in upper_sql
            token = _find_target_token(sql, stype)
            if token:
                identifier = _parse_identifier(token)
                if "OR REPLACE" in upper_sql:
                    existing = catalog.lookup(identifier, session)
                    if existing:
                        catalog.drop(existing, index)
                catalog.create_object(identifier, session, index, is_temp=temp)
        elif stype == StatementType.CREATE_VIEW:
            token = _find_target_token(sql, StatementType.CREATE_VIEW)
            if token:
                identifier = _parse_identifier(token)
                bases = {
                    qn
                    for qn, _ in expand_readset(
                        sql, catalog, session, cfg, warnings, index
                    )
                }
                catalog.set_view(identifier, session, bases)
        elif stype == StatementType.DROP_TABLE:
            token = _find_target_token(sql, StatementType.DROP_TABLE)
            if token:
                identifier = _parse_identifier(token)
                existing = catalog.lookup(identifier, session)
                if existing:
                    catalog.drop(existing, index)
        elif stype == StatementType.ALTER_RENAME:
            match = re.search(
                r"ALTER\s+TABLE\s+([^(\s]+)\s+RENAME\s+TO\s+([^(\s]+)",
                sql,
                re.IGNORECASE,
            )
            if match:
                old_ident = _parse_identifier(match.group(1))
                new_ident = _parse_identifier(match.group(2))
                obj = catalog.lookup(old_ident, session)
                if obj:
                    old_qn, new_qn = catalog.rename(obj, new_ident, session, index)
                    renames.append(
                        RenameEvent(
                            old=old_qn,
                            new=new_qn,
                            stmt_index=index,
                            session_id=statement.session_id,
                            timestamp_ms=statement.timestamp_ms,
                        )
                    )

        if not is_write_statement(stype):
            continue
        if stype == StatementType.DELETE_USING and not cfg.include_negative_edges:
            continue
        targets = extract_write_targets(sql, stype, catalog, session, warnings, index)
        if not targets:
            continue

        if stype == StatementType.COPY:
            read_entries: List[Tuple[QualifiedName, Optional[int]]] = []
            if cfg.include_external_sources:
                source = _extract_copy_source(sql)
                if source:
                    read_entries.append((_make_external(source), None))
        elif stype == StatementType.MOVE_PARTITIONS:
            read_entries = []
            move = _extract_move_partitions(sql)
            if move:
                source_ident = _parse_identifier(move[0])
                obj = catalog.lookup(source_ident, session)
                if obj:
                    read_entries.append((obj.qualified(), obj.obj_id))
                else:
                    record_warning(
                        warnings, index, f"unresolved-identifier: {move[0]}"
                    )
        elif stype == StatementType.DELETE_USING:
            read_entries = []
            for token in _extract_delete_sources(sql):
                identifier = _parse_identifier(token)
                obj = catalog.lookup(identifier, session)
                if obj:
                    read_entries.append((obj.qualified(), obj.obj_id))
                else:
                    view_def = catalog.get_view(identifier, session)
                    if view_def and cfg.expand_views_to_bases:
                        for base, base_id in _resolve_view_bases(
                            catalog, view_def.bases, session
                        ):
                            read_entries.append((base, base_id))
                    else:
                        record_warning(
                            warnings, index, f"unresolved-identifier: {token}"
                        )
        else:
            read_entries = expand_readset(sql, catalog, session, cfg, warnings, index)

        if stype == StatementType.EXPORT and not read_entries:
            read_entries = expand_readset(sql, catalog, session, cfg, warnings, index)

        op = map_op(stype)
        base_meta = _extract_metadata(sql, stype, cfg)

        if stype == StatementType.CREATE_TABLE_AS:
            for dst_qn, dst_id in targets:
                if dst_id is None:
                    continue
                obj = catalog.objects.get(dst_id)
                if obj and obj.is_temp:
                    catalog.record_temp_lineage(
                        obj.obj_id,
                        {(src_qn, src_id) for src_qn, src_id in read_entries},
                    )

        for dst_qn, dst_id in targets:
            dst_obj = catalog.objects.get(dst_id) if dst_id is not None else None
            dst_is_temp = dst_obj.is_temp if dst_obj else False
            for src_qn, src_id in read_entries:
                src_obj = catalog.objects.get(src_id) if src_id is not None else None
                src_is_temp = src_obj.is_temp if src_obj else False
                if not cfg.include_temp_tables and (dst_is_temp or src_is_temp):
                    continue
                meta = dict(base_meta)
                if src_id is not None:
                    meta.setdefault("src_obj_id", str(src_id))
                if dst_id is not None:
                    meta.setdefault("dst_obj_id", str(dst_id))
                detailed_edges.append(
                    Edge(
                        src=src_qn,
                        dst=dst_qn,
                        op=op,
                        stmt_index=index,
                        session_id=statement.session_id,
                        timestamp_ms=statement.timestamp_ms,
                        meta=meta,
                    )
                )

    if cfg.compress_temps_in_output:
        detailed_edges = compress_temp_edges(detailed_edges, catalog)

    rendered_edges: List[Tuple[str, str]] = []
    for edge in detailed_edges:
        src_id = edge.meta.get("src_obj_id")
        dst_id = edge.meta.get("dst_obj_id")
        src_render = catalog.render_name(
            edge.src, cfg, int(src_id) if src_id is not None else None
        )
        dst_render = catalog.render_name(
            edge.dst, cfg, int(dst_id) if dst_id is not None else None
        )
        rendered_edges.append((src_render, dst_render))

    live, ephemeral = finalize_liveness_sets(catalog)

    return LineageResult(
        edges=rendered_edges,
        detailed_edges=detailed_edges,
        ephemeral_tables={qn.render() for qn in ephemeral},
        live_tables={qn.render() for qn in live},
        renames=renames,
        warnings=warnings,
    )


def analyze_edges_only(
    initial_tables: Iterable[str],
    sql_list: Iterable[str],
    enabled_mask: Optional[Iterable[bool]] = None,
) -> List[Tuple[str, str]]:
    sql_seq = list(sql_list)
    if enabled_mask is None:
        enabled_iter = [True] * len(sql_seq)
    else:
        enabled_iter = list(enabled_mask)
        if len(enabled_iter) != len(sql_seq):
            raise ValueError("enabled_mask length mismatch")

    tables: Set[QualifiedName] = set()
    for name in initial_tables:
        tables.add(_qualified_from_string(name))
    initial = InitialCatalog(tables)

    statements = [Statement(sql=sql, enabled=enabled) for sql, enabled in zip(sql_seq, enabled_iter)]
    cfg = AnalyzerConfig()
    cfg.name_mode = NameResolutionMode.AS_OF_EVENT
    cfg.ignore_disabled_statements = True
    cfg.tolerate_parse_errors = True
    cfg.expand_views_to_bases = True
    result = analyze_lineage(initial, statements, cfg)
    return result.edges


def _qualified_from_string(name: str) -> QualifiedName:
    ident = _parse_identifier(name)
    return ident.to_qualified("public")

