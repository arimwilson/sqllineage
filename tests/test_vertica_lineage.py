"""Unit tests for Vertica lineage analyzer."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from vertica_lineage import (  # noqa: E402
    AnalyzerConfig,
    EdgeOp,
    InitialCatalog,
    LineageResult,
    NameResolutionMode,
    QualifiedName,
    Statement,
    analyze_lineage,
)


def build_catalog(names: list[str]) -> InitialCatalog:
    return InitialCatalog({QualifiedName(*name.split('.')) for name in names})


def test_example_usage() -> None:
    initial = build_catalog(["public.s", "dw.b"])
    statements = [
        Statement("CREATE TABLE a AS SELECT * FROM public.s"),
        Statement("ALTER TABLE a RENAME TO x"),
        Statement(
            "MERGE INTO dw.tgt USING (SELECT * FROM dw.b) u ON (u.id = tgt.id)"
        ),
        Statement("CREATE LOCAL TEMP TABLE tmp AS SELECT * FROM x"),
        Statement("INSERT INTO dw.tgt SELECT * FROM tmp"),
        Statement("DROP TABLE x"),
    ]

    result = analyze_lineage(initial, statements, AnalyzerConfig())

    assert set(result.edges) == {
        ("public.s", "public.x"),
        ("dw.b", "dw.tgt"),
        ("public.x", "public.tmp"),
        ("public.tmp", "dw.tgt"),
        ("public.x", "dw.tgt"),
    }
    assert result.ephemeral_tables == {"public.tmp", "public.x"}
    assert result.live_tables == {"public.s", "dw.b", "dw.tgt"}
    assert len(result.renames) == 1
    assert result.warnings == {}


def test_rename_chain_and_name_modes() -> None:
    initial = build_catalog(["public.src"])
    statements = [
        Statement("CREATE TABLE a AS SELECT * FROM public.src"),
        Statement("ALTER TABLE a RENAME TO b"),
        Statement("ALTER TABLE b RENAME TO c"),
        Statement("INSERT INTO c SELECT * FROM public.src"),
    ]

    cfg = AnalyzerConfig(name_mode=NameResolutionMode.AS_OF_EVENT)
    res_asof = analyze_lineage(initial, statements, cfg)
    assert res_asof.edges[0] == ("public.src", "public.a")
    cfg_final = AnalyzerConfig(name_mode=NameResolutionMode.FINAL_NAME)
    res_final = analyze_lineage(initial, statements, cfg_final)
    assert res_final.edges[0] == ("public.src", "public.c")
    cfg_stable = AnalyzerConfig(name_mode=NameResolutionMode.STABLE_ID)
    res_stable = analyze_lineage(initial, statements, cfg_stable)
    assert "#obj" in res_stable.edges[0][0]
    assert "#obj" in res_stable.edges[0][1]


def test_drop_and_recreate_same_name() -> None:
    initial = build_catalog(["public.src"])
    statements = [
        Statement("CREATE TABLE t AS SELECT * FROM public.src"),
        Statement("DROP TABLE t"),
        Statement("CREATE TABLE t AS SELECT * FROM public.src"),
    ]

    result = analyze_lineage(initial, statements, AnalyzerConfig())
    assert result.edges == [
        ("public.src", "public.t"),
        ("public.src", "public.t"),
    ]
    assert "public.t" in result.ephemeral_tables


def test_search_path_per_session() -> None:
    initial = build_catalog(["sales.src", "public.other"])
    statements = [
        Statement("SET SEARCH_PATH TO sales", session_id="s1"),
        Statement("CREATE TABLE tgt AS SELECT * FROM src", session_id="s1"),
    ]

    result = analyze_lineage(initial, statements, AnalyzerConfig())
    assert result.edges == [("sales.src", "sales.tgt")]


def test_quoted_identifiers_preserved() -> None:
    initial = InitialCatalog({QualifiedName('"Caps"', '"Mix"')})
    statements = [
        Statement("CREATE TABLE \"Tgt\" AS SELECT * FROM \"Caps\".\"Mix\""),
    ]

    result = analyze_lineage(initial, statements, AnalyzerConfig())
    assert result.edges == [("\"Caps\".\"Mix\"", "public.\"Tgt\"")]


def test_delete_using_edge_controlled_by_config() -> None:
    initial = build_catalog(["public.a", "public.b"])
    statements = [Statement("DELETE FROM a USING b WHERE a.id=b.id")]
    cfg = AnalyzerConfig()
    res_default = analyze_lineage(initial, statements, cfg)
    assert res_default.edges == []
    cfg.include_negative_edges = True
    res_with = analyze_lineage(initial, statements, cfg)
    assert res_with.edges == [("public.b", "public.a")]
    assert res_with.detailed_edges[0].op == EdgeOp.DELETE_USING
    assert res_with.detailed_edges[0].meta["effect"] == "delete"


def test_copy_external_source() -> None:
    initial = build_catalog(["public.tgt"])
    statements = [Statement("COPY public.tgt FROM '/data/path/file.csv'")]
    result = analyze_lineage(initial, statements, AnalyzerConfig())
    assert result.edges == [("ext:///data/path/file.csv", "public.tgt")]


def test_move_partitions_metadata() -> None:
    initial = build_catalog(["public.a", "public.b"])
    statements = [
        Statement(
            "SELECT MOVE_PARTITIONS_TO_TABLE('public.a','2024-01-01','2024-02-01','public.b');"
        )
    ]
    result = analyze_lineage(initial, statements, AnalyzerConfig())
    edge = result.detailed_edges[0]
    assert edge.op == EdgeOp.MOVE_PARTITIONS
    assert edge.meta["source_partition_start"] == "2024-01-01"
    assert edge.meta["source_partition_end"] == "2024-02-01"


def test_view_expansion_and_edges() -> None:
    initial = build_catalog(["public.a", "public.b", "public.base"])
    statements = [
        Statement("CREATE VIEW v1 AS SELECT * FROM public.a"),
        Statement("CREATE VIEW v2 AS SELECT * FROM v1 JOIN public.b ON 1=1"),
        Statement("INSERT INTO public.base SELECT * FROM v2"),
    ]

    result = analyze_lineage(initial, statements, AnalyzerConfig())
    assert set(result.edges) == {
        ("public.v2", "public.base"),
        ("public.v1", "public.base"),
        ("public.a", "public.base"),
        ("public.b", "public.base"),
    }


def test_merge_with_subquery() -> None:
    initial = build_catalog(["public.a", "public.b", "public.tgt"])
    statements = [
        Statement(
            "MERGE INTO public.tgt USING (SELECT * FROM public.a JOIN public.b USING(id)) s ON tgt.id=s.id"
        )
    ]
    result = analyze_lineage(initial, statements, AnalyzerConfig())
    assert sorted(result.edges) == [
        ("public.a", "public.tgt"),
        ("public.b", "public.tgt"),
    ]


def test_temp_compression() -> None:
    initial = build_catalog(["public.src"])
    statements = [
        Statement("CREATE LOCAL TEMP TABLE tmp AS SELECT * FROM public.src"),
        Statement("INSERT INTO public.final SELECT * FROM tmp"),
    ]
    cfg = AnalyzerConfig(compress_temps_in_output=True)
    result = analyze_lineage(initial, statements, cfg)
    assert result.edges == [("public.src", "public.final")]


def test_disabled_statement_warning_when_configured() -> None:
    initial = build_catalog(["public.a", "public.b"])
    statements = [
        Statement("INSERT INTO public.a SELECT * FROM public.b", enabled=False)
    ]
    cfg = AnalyzerConfig(ignore_disabled_statements=False)
    result = analyze_lineage(initial, statements, cfg)
    assert result.edges == []
    assert "ignored-disabled" in result.warnings[0][0]


def test_parse_error_tolerance() -> None:
    initial = build_catalog(["public.a"])
    statements = [Statement("RAISE_PARSE_ERROR"), Statement("SELECT 1")]
    result = analyze_lineage(initial, statements, AnalyzerConfig())
    assert "parse-error" in result.warnings[0][0]


def test_ctas_with_newline_is_detected():
    initial = InitialCatalog({QualifiedName("public","s")})
    stmts = [Statement("CREATE TABLE t AS\nSELECT * FROM public.s")]
    res = analyze_lineage(initial, stmts, AnalyzerConfig())
    assert res.edges == [("public.s", "public.t")]