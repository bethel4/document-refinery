from __future__ import annotations

import re
import sqlite3
import json
from datetime import date
from pathlib import Path
from typing import Dict, List

from src.models.ldu import LDU


class FactTableExtractor:
    """Extract numeric claims/facts from LDUs into SQLite for structured querying."""

    def __init__(self, db_path: str = ".refinery/facts/facts.db") -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _init_db(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS facts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    document_id TEXT NOT NULL,
                    ldu_id TEXT NOT NULL,
                    section TEXT,
                    page_num INTEGER NOT NULL,
                    line_number INTEGER,
                    key TEXT,
                    metric TEXT,
                    value REAL,
                    unit TEXT,
                    date TEXT,
                    confidence REAL,
                    description TEXT,
                    sentence TEXT,
                    content_hash TEXT NOT NULL,
                    bbox_json TEXT,
                    extraction_date TEXT
                )
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_facts_doc ON facts(document_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_facts_metric ON facts(metric)")
            # Backward-compatible migration for existing DBs.
            cols = [r[1] for r in conn.execute("PRAGMA table_info(facts)").fetchall()]
            if "bbox_json" not in cols:
                conn.execute("ALTER TABLE facts ADD COLUMN bbox_json TEXT")
            if "key" not in cols:
                conn.execute("ALTER TABLE facts ADD COLUMN key TEXT")
            if "date" not in cols:
                conn.execute("ALTER TABLE facts ADD COLUMN date TEXT")
            if "confidence" not in cols:
                conn.execute("ALTER TABLE facts ADD COLUMN confidence REAL")
            if "section" not in cols:
                conn.execute("ALTER TABLE facts ADD COLUMN section TEXT")
            if "line_number" not in cols:
                conn.execute("ALTER TABLE facts ADD COLUMN line_number INTEGER")
            if "description" not in cols:
                conn.execute("ALTER TABLE facts ADD COLUMN description TEXT")
            if "extraction_date" not in cols:
                conn.execute("ALTER TABLE facts ADD COLUMN extraction_date TEXT")

    def ingest_ldus(self, document_id: str, ldus: List[LDU]) -> int:
        rows: List[tuple] = []
        for ldu in ldus:
            for sentence in self._sentences(ldu.text):
                for fact in self._extract_numeric_facts(sentence):
                    rows.append(
                        (
                            document_id,
                            ldu.ldu_id,
                            ldu.parent_section,
                            ldu.page_num,
                            fact["line_number"],
                            fact["key"],
                            fact["metric"],
                            fact["value"],
                            fact["unit"],
                            fact["date"],
                            fact["confidence"],
                            fact["description"],
                            sentence,
                            ldu.content_hash,
                            json.dumps((ldu.bbox or ldu.bounding_box).model_dump()) if (ldu.bbox or ldu.bounding_box) else None,
                            fact["extraction_date"],
                        )
                    )

        if not rows:
            return 0

        with sqlite3.connect(self.db_path) as conn:
            conn.executemany(
                """
                INSERT INTO facts (
                    document_id, ldu_id, section, page_num, line_number, key, metric, value, unit,
                    date, confidence, description, sentence, content_hash, bbox_json, extraction_date
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
        return len(rows)

    def _sentences(self, text: str) -> List[str]:
        return [s.strip() for s in re.split(r"(?<=[.!?])\s+", text) if s.strip()]

    def _extract_numeric_facts(self, sentence: str) -> List[Dict]:
        facts: List[Dict] = []
        number_pattern = re.compile(r"(?<!\w)(\d{1,3}(?:,\d{3})*(?:\.\d+)?)(?:\s*(%|USD|ETB|EUR|million|billion|thousand))?", re.I)
        words = sentence.split()
        for match in number_pattern.finditer(sentence):
            raw_num = match.group(1).replace(",", "")
            unit = (match.group(2) or "").strip()
            try:
                value = float(raw_num)
            except ValueError:
                continue

            start_idx = sentence[: match.start()].strip().split()
            metric_window = words[max(0, len(start_idx) - 4) : len(start_idx)]
            metric = " ".join(metric_window).strip() or "numeric_fact"

            facts.append(
                {
                    "key": metric[:80],
                    "metric": metric[:80],
                    "value": value,
                    "unit": unit[:20],
                    "date": self._extract_date_hint(sentence),
                    "confidence": 0.75,
                    "description": sentence[:240],
                    "line_number": 1,
                    "extraction_date": date.today().isoformat(),
                }
            )
        return facts

    def _extract_date_hint(self, sentence: str) -> str:
        quarter = re.search(r"\bQ[1-4]\b(?:\s+\d{4})?", sentence, flags=re.I)
        if quarter:
            return quarter.group(0)
        year = re.search(r"\b(19|20)\d{2}\b", sentence)
        if year:
            return year.group(0)
        month_year = re.search(
            r"\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+(19|20)\d{2}\b",
            sentence,
            flags=re.I,
        )
        if month_year:
            return month_year.group(0)
        return ""
