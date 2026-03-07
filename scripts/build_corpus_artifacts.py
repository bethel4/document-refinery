from __future__ import annotations

import json
import sys
import time
import hashlib
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List

import fitz

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.agents.indexer import PageIndexerAgent
from src.data_layer.fact_table_extractor import FactTableExtractor
from src.query.vector_store import VectorStoreIngestor
from src.extraction.vision_extractor import VisionExtractor
from src.models.ldu import LDU, LDURole, LDUType

RAW_DIR = ROOT / "data" / "raw"
REFINERY_DIR = ROOT / ".refinery"
EXTRACTIONS_DIR = REFINERY_DIR / "extractions"
PAGEINDEX_DIR = REFINERY_DIR / "pageindex"
QNA_DIR = REFINERY_DIR / "qna"
LEDGER_PATH = REFINERY_DIR / "extraction_ledger.jsonl"

DOC_CLASSES: Dict[str, List[str]] = {
    "financial_statement": [
        "2020_Audited_Financial_Statement_Report.pdf",
        "2021_Audited_Financial_Statement_Report.pdf",
        "2022_Audited_Financial_Statement_Report.pdf",
    ],
    "economic_report": [
        "Consumer Price Index June 2025.pdf",
        "Consumer Price Index, April 2025.pdf",
        "tax_expenditure_ethiopia_2021_22.pdf",
    ],
    "institutional_report": [
        "Company_Profile_2024_25.pdf",
        "CBE Annual Report 2012-13.pdf",
        "20191010_Pharmaceutical-Manufacturing-Opportunites-in-Ethiopia_VF.pdf",
    ],
    "public_sector_report": [
        "2013-E.C-Assigned-regular-budget-and-expense.pdf",
        "2013-E.C-Audit-finding-information.pdf",
        "fta_performance_survey_final_report_2022.pdf",
    ],
}


def _ensure_dirs() -> None:
    for directory in [EXTRACTIONS_DIR, PAGEINDEX_DIR, QNA_DIR]:
        directory.mkdir(parents=True, exist_ok=True)


def _extract_with_fitz(pdf_path: Path) -> List[Dict[str, Any]]:
    pages: List[Dict[str, Any]] = []
    with fitz.open(pdf_path) as doc:
        for page_num, page in enumerate(doc, start=1):
            text = page.get_text("text").strip()
            pages.append(
                {
                    "page_num": page_num,
                    "text": text,
                    "text_length": len(text),
                    "tables": [],
                    "confidence": 0.99 if text else 0.0,
                }
            )
    return pages


def _extract_pages(pdf_path: Path) -> tuple[List[Dict[str, Any]], str, float]:
    pages = _extract_with_fitz(pdf_path)
    if any(page["text"].strip() for page in pages):
        return pages, "fast_text", 0.0

    with fitz.open(pdf_path) as doc:
        total_pages = len(doc)
    extractor = VisionExtractor(max_vision_pages=total_pages, dpi=120, ocr_workers=1)
    result = extractor.extract(str(pdf_path), {"file_path": str(pdf_path), "total_pages": total_pages})
    return result.get("pages", []), "vision", 0.0


def _load_pageindex(document_id: str) -> Dict[str, Any]:
    return json.loads((PAGEINDEX_DIR / f"{document_id}_pageindex.json").read_text(encoding="utf-8"))


def _first_sentence(text: str) -> str:
    normalized = " ".join(text.split())
    if not normalized:
        return ""
    for sep in [". ", "? ", "! "]:
        if sep in normalized:
            return normalized.split(sep, 1)[0].strip() + sep.strip()
    return normalized[:240]


def _pick_qna_candidate(pageindex: Dict[str, Any]) -> Dict[str, Any]:
    best: Dict[str, Any] | None = None
    for page in pageindex.get("pages", []):
        for ldu in page.get("ldus", []):
            text = ldu.get("text", "").strip()
            if not text:
                continue
            score = len(text) + (50 if any(ch.isdigit() for ch in text) else 0)
            if best is None or score > best["score"]:
                best = {
                    "score": score,
                    "page_number": page.get("page_number"),
                    "section_title": ldu.get("section_title") or page.get("section_title") or "Document Root",
                    "ldu_id": ldu.get("ldu_id"),
                    "ldu_text": text,
                    "content_hash": ldu.get("content_hash"),
                }
    if best is None:
        raise ValueError(f"No LDU candidates found in {pageindex.get('document_id')}")
    return best


def _build_qna_entry(document_class: str, pageindex: Dict[str, Any]) -> Dict[str, Any]:
    candidate = _pick_qna_candidate(pageindex)
    title = pageindex.get("title", pageindex["document_id"])
    section_title = candidate["section_title"]
    question = f"What does {title} say in the section '{section_title}'?"
    answer = _first_sentence(candidate["ldu_text"])
    return {
        "document_class": document_class,
        "document_id": pageindex["document_id"],
        "document_title": title,
        "question": question,
        "answer": answer,
        "provenance_chain": [
            {
                "document_id": pageindex["document_id"],
                "page_number": candidate["page_number"],
                "section_title": section_title,
                "ldu_id": candidate["ldu_id"],
                "ldu_text": candidate["ldu_text"],
                "content_hash": candidate["content_hash"],
            }
        ],
    }


def _build_ldus(document_id: str, pages: List[Dict[str, Any]], strategy_used: str) -> List[LDU]:
    ldus: List[LDU] = []
    position = 0
    for page in pages:
        page_num = int(page.get("page_num", 1))
        raw_text = (page.get("text", "") or "").strip()
        if not raw_text:
            continue
        paragraphs = [part.strip() for part in raw_text.split("\n\n") if part.strip()]
        chunks = paragraphs if paragraphs else [raw_text]
        buffer = ""
        for part in chunks:
            candidate = f"{buffer}\n\n{part}".strip() if buffer else part
            if len(candidate) < 120:
                buffer = candidate
                continue
            ldus.append(
                LDU(
                    ldu_id=f"{document_id}_ldu_{position + 1}",
                    document_id=document_id,
                    ldu_type=LDUType.PARAGRAPH,
                    role=LDURole.CONTENT,
                    text=candidate,
                    text_length=len(candidate),
                    content_hash=hashlib.sha256(candidate.encode("utf-8")).hexdigest(),
                    confidence=float(page.get("confidence", 0.0)),
                    page_num=page_num,
                    page_refs=[page_num],
                    position_in_document=position,
                    parent_section=f"{document_id}_section_root",
                    extraction_method=strategy_used,
                )
            )
            position += 1
            buffer = ""
        if buffer:
            ldus.append(
                LDU(
                    ldu_id=f"{document_id}_ldu_{position + 1}",
                    document_id=document_id,
                    ldu_type=LDUType.PARAGRAPH,
                    role=LDURole.CONTENT,
                    text=buffer,
                    text_length=len(buffer),
                    content_hash=hashlib.sha256(buffer.encode("utf-8")).hexdigest(),
                    confidence=float(page.get("confidence", 0.0)),
                    page_num=page_num,
                    page_refs=[page_num],
                    position_in_document=position,
                    parent_section=f"{document_id}_section_root",
                    extraction_method=strategy_used,
                )
            )
            position += 1
    return ldus


def _write_ledger(entries: List[Dict[str, Any]]) -> None:
    LEDGER_PATH.write_text(
        "".join(json.dumps(entry, ensure_ascii=False) + "\n" for entry in entries),
        encoding="utf-8",
    )


def main() -> int:
    _ensure_dirs()
    indexer = PageIndexerAgent(out_dir=str(PAGEINDEX_DIR))
    fact_extractor = FactTableExtractor()
    vector_store = VectorStoreIngestor()

    qna_by_class: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    ledger_entries: List[Dict[str, Any]] = []

    for document_class, filenames in DOC_CLASSES.items():
        for filename in filenames:
            pdf_path = RAW_DIR / filename
            document_id = pdf_path.stem
            started = time.perf_counter()
            pages, strategy_used, cost_estimate = _extract_pages(pdf_path)
            processing_time = round(time.perf_counter() - started, 2)

            ldus = _build_ldus(document_id=document_id, pages=pages, strategy_used=strategy_used)
            pageindex = indexer.run(document_id=document_id, ldus=ldus)
            pageindex["title"] = filename
            pageindex["document_class"] = document_class
            (PAGEINDEX_DIR / f"{document_id}_pageindex.json").write_text(
                json.dumps(pageindex, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )

            vector_info = vector_store.ingest(document_id, ldus)
            fact_rows = fact_extractor.ingest_ldus(document_id, ldus)
            extraction_payload = {
                "document_id": document_id,
                "filename": filename,
                "document_class": document_class,
                "total_duration": processing_time,
                "extraction": {
                    "strategy_used": strategy_used,
                    "pages": pages,
                    "routing_metadata": {
                        "average_confidence": round(
                            sum(float(page.get("confidence", 0.0)) for page in pages) / max(1, len(pages)),
                            3,
                        ),
                        "pages_processed": len(pages),
                        "escalated": False,
                    },
                },
                "ldus": [ldu.model_dump(mode="json") for ldu in ldus],
                "page_index_tree": pageindex,
                "vector_store": vector_info,
                "fact_table": {"sqlite_path": str(fact_extractor.db_path), "rows_inserted": fact_rows},
            }
            (EXTRACTIONS_DIR / f"{document_id}_extraction.json").write_text(
                json.dumps(extraction_payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )

            qna_entry = _build_qna_entry(document_class=document_class, pageindex=_load_pageindex(document_id))
            qna_by_class[document_class].append(qna_entry)

            ledger_entries.append(
                {
                    "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                    "document_id": document_id,
                    "filename": filename,
                    "document_class": document_class,
                    "strategy_used": strategy_used,
                    "confidence_score": extraction_payload["extraction"]["routing_metadata"]["average_confidence"],
                    "cost_estimate": cost_estimate,
                    "processing_time": processing_time,
                    "pages_processed": len(pages),
                    "status": "success",
                }
            )

    for document_class, items in qna_by_class.items():
        (QNA_DIR / f"qna_{document_class}.json").write_text(
            json.dumps(items, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    (QNA_DIR / "qna_examples.json").write_text(
        json.dumps([item for items in qna_by_class.values() for item in items], ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    _write_ledger(ledger_entries)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
