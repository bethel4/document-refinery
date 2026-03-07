from __future__ import annotations

import sqlite3

from src.agents.indexer import PageIndexerAgent
from src.data_layer.fact_table_extractor import FactTableExtractor
from src.models.ldu import LDU, LDURole, LDUType


def _sample_ldu() -> LDU:
    return LDU(
        ldu_id="doc_ldu_1",
        document_id="doc",
        ldu_type=LDUType.PARAGRAPH,
        role=LDURole.CONTENT,
        text="Section One\n\nRevenue increased by 15 percent in 2025.",
        text_length=49,
        content_hash="7d1a28654a5c0f0f47a1a8b5b2c9e3a9553f18a7f5ba7f4f6e6428dd4bbbf6d0",
        confidence=0.95,
        page_num=1,
        page_refs=[1],
        position_in_document=0,
        parent_section="doc_section_root",
        extraction_method="fast_text",
    )


def test_indexer_outputs_page_section_ldu_structure(tmp_path):
    agent = PageIndexerAgent(out_dir=str(tmp_path))
    tree = agent.run("doc", [_sample_ldu()])

    assert tree["document_id"] == "doc"
    assert tree["pages"][0]["page_number"] == 1
    assert tree["pages"][0]["ldus"][0]["ldu_id"] == "doc_ldu_1"
    assert tree["sections"][0]["section_title"]


def test_fact_table_schema_includes_requested_fields(tmp_path):
    db_path = tmp_path / "facts.db"
    extractor = FactTableExtractor(db_path=str(db_path))
    rows = extractor.ingest_ldus("doc", [_sample_ldu()])

    assert rows >= 1

    with sqlite3.connect(db_path) as conn:
        cols = [row[1] for row in conn.execute("PRAGMA table_info(facts)").fetchall()]
        assert "section" in cols
        assert "line_number" in cols
        assert "description" in cols
        assert "extraction_date" in cols
