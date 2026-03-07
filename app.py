from __future__ import annotations

import json
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List, Optional

import fitz
import pandas as pd
import streamlit as st
from PIL import Image

from src.domain_analysis.triage.document_classifier import TriageClassifier
from src.extraction.pipeline_runner import ExtractionPipeline
from src.query.pageindex_query import PageIndexQuery
from src.agents.query_agent import QueryAgent


REFINERY_DIR = Path(".refinery")
UPLOADS_DIR = REFINERY_DIR / "uploads"
EXTRACTIONS_DIR = REFINERY_DIR / "extractions"
PAGEINDEX_DIR = REFINERY_DIR / "pageindex"
LEDGER_PATH = REFINERY_DIR / "extraction_ledger.jsonl"
RULES_PATH = REFINERY_DIR / "rules" / "extraction_rules.yaml"
PROFILES_DIR = REFINERY_DIR / "profiles"
QNA_DIR = REFINERY_DIR / "qna"
QUERY_LOG_PATH = QNA_DIR / "asked_queries.jsonl"


def _ensure_dirs() -> None:
    for directory in [REFINERY_DIR, UPLOADS_DIR, EXTRACTIONS_DIR, PAGEINDEX_DIR, PROFILES_DIR, QNA_DIR]:
        directory.mkdir(parents=True, exist_ok=True)


def _read_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _save_upload(uploaded_file) -> Path:
    _ensure_dirs()
    target = UPLOADS_DIR / uploaded_file.name
    target.write_bytes(uploaded_file.getbuffer())
    return target


def _profile_to_dict(profile) -> Dict[str, Any]:
    if hasattr(profile, "model_dump"):
        return profile.model_dump(mode="json")
    if hasattr(profile, "to_dict"):
        return profile.to_dict()
    return dict(profile)


def _strategy_reason(profile: Dict[str, Any]) -> str:
    origin = profile.get("origin_type", "unknown")
    layout = profile.get("layout_complexity", "unknown")
    avg_chars = round(float(profile.get("avg_chars_per_page", 0.0)), 1)
    image_ratio = round(float(profile.get("image_area_ratio", 0.0)), 2)
    strategy = profile.get("recommended_strategy", "unknown")
    return (
        f"Selected `{strategy}` because origin type is `{origin}`, layout is `{layout}`, "
        f"average characters per page is `{avg_chars}`, and image area ratio is `{image_ratio}`."
    )


def _render_pdf_page(pdf_path: Path, page_number: int = 1, zoom: float = 1.5) -> Optional[Image.Image]:
    with fitz.open(pdf_path) as doc:
        if not doc or page_number < 1 or page_number > len(doc):
            return None
        page = doc[page_number - 1]
        pix = page.get_pixmap(matrix=fitz.Matrix(zoom, zoom), alpha=False)
        return Image.open(BytesIO(pix.tobytes("png")))


def _latest_ledger_entry(document_id: str) -> Optional[Dict[str, Any]]:
    if not LEDGER_PATH.exists():
        return None
    entries = [
        json.loads(line)
        for line in LEDGER_PATH.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    for entry in reversed(entries):
        if entry.get("document_id") == document_id:
            return entry
    return None


def _pageindex_path(document_id: str) -> Path:
    return PAGEINDEX_DIR / f"{document_id}_pageindex.json"


def _extraction_path(document_id: str) -> Path:
    return EXTRACTIONS_DIR / f"{document_id}_extraction.json"


def _page_table(pages: List[Dict[str, Any]]) -> pd.DataFrame:
    rows = []
    for page in pages:
        rows.append(
            {
                "page": page.get("page_num", page.get("page_number")),
                "confidence": round(float(page.get("confidence", 0.0)), 3),
                "text_length": page.get("text_length", len(page.get("text", ""))),
                "tables": len(page.get("tables", []) or []),
                "strategy": page.get("strategy_used") or page.get("extraction_method"),
                "preview": " ".join((page.get("text", "") or "").split())[:140],
            }
        )
    return pd.DataFrame(rows)


def _tree_rows(tree: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    sections = tree.get("sections") or tree.get("children") or []
    def visit(nodes: List[Dict[str, Any]], depth: int = 0) -> None:
        for section in nodes:
            rows.append(
                {
                    "section_title": f"{'  ' * depth}{section.get('section_title') or section.get('title')}",
                    "pages": ", ".join(str(p) for p in section.get("page_refs", [])),
                    "ldus": len(section.get("ldu_ids", [])),
                    "summary": section.get("summary", ""),
                }
            )
            visit(section.get("children") or section.get("child_nodes") or [], depth + 1)
    visit(sections)
    return rows


def _tree_outline(tree: Dict[str, Any], document_title: str) -> str:
    sections = tree.get("sections") or tree.get("children") or []
    lines = [tree.get("title") or document_title]

    def visit(nodes: List[Dict[str, Any]], prefix: str = "") -> None:
        for idx, node in enumerate(nodes):
            is_last = idx == len(nodes) - 1
            branch = "└─ " if is_last else "├─ "
            lines.append(f"{prefix}{branch}{node.get('section_title') or node.get('title') or 'Untitled'}")
            child_prefix = f"{prefix}{'   ' if is_last else '│  '}"
            visit(node.get("children") or node.get("child_nodes") or [], child_prefix)

    visit(sections)
    return "\n".join(lines)


def _citation_rows(citations: List[Dict[str, Any]]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "page": citation.get("page_number"),
                "verification": citation.get("verification_status"),
                "hash": str(citation.get("content_hash", ""))[:16],
                "excerpt": citation.get("verification_excerpt") or citation.get("text_excerpt", ""),
            }
            for citation in citations
        ]
    )


def _split_topics(raw_topics: str) -> List[str]:
    return [part.strip() for part in raw_topics.split(",") if part.strip()]


def _has_mock_content(extraction: Dict[str, Any]) -> bool:
    pages = extraction.get("extraction", {}).get("pages", [])
    return any("mock tesseract ocr text" in (page.get("text", "") or "").lower() for page in pages)


def _page_ref_label(page_refs: List[int]) -> str:
    if not page_refs:
        return "page unknown"
    if len(page_refs) == 1:
        return f"page {page_refs[0]}"
    return f"pages {page_refs[0]}-{page_refs[-1]}"


def _save_query_result(
    document_id: str,
    document_name: str,
    question: str,
    result: Dict[str, Any],
    mode: str,
) -> None:
    entry = {
        "document_id": document_id,
        "document_name": document_name,
        "mode": mode,
        "question": question,
        "answer": result.get("answer"),
        "status": result.get("status") or result.get("verification_status"),
        "provenance_chain": result.get("provenance_chain", result.get("citations", [])),
    }
    with QUERY_LOG_PATH.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(entry, ensure_ascii=False) + "\n")


def _section_ldu_texts(tree: Dict[str, Any], section: Dict[str, Any]) -> List[str]:
    ldu_ids = set(section.get("ldu_ids", []))
    texts: List[str] = []
    for page in tree.get("pages", []):
        for ldu in page.get("ldus", []):
            if ldu.get("ldu_id") in ldu_ids and (ldu.get("text") or "").strip():
                texts.append(ldu["text"].strip())
    return texts


def _topic_excerpt(tree: Dict[str, Any], section: Dict[str, Any], topic: str, max_chars: int = 280) -> str:
    texts = _section_ldu_texts(tree, section)
    if not texts:
        return section.get("summary", "")

    normalized_topic_terms = [term.lower() for term in _split_topics(topic.replace("/", " ").replace("-", " "))]
    normalized_topic_terms.extend([token.lower() for token in topic.split() if token.strip()])

    for text in texts:
        collapsed = " ".join(text.split())
        lowered = collapsed.lower()
        positions = [lowered.find(term) for term in normalized_topic_terms if term and lowered.find(term) >= 0]
        if positions:
            start = max(0, min(positions) - 80)
            return collapsed[start : start + max_chars]

    return " ".join(texts[0].split())[:max_chars]


def _topic_result_payload(tree: Dict[str, Any], topic: str, matches: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    payload: List[Dict[str, Any]] = []
    for match in matches:
        page_refs = [int(page) for page in match.get("page_refs", [])]
        payload.append(
            {
                "topic": topic,
                "found_in": match.get("section_title") or match.get("title") or "Document Body",
                "pages": _page_ref_label(page_refs),
                "relevant_text": _topic_excerpt(tree, match, topic),
                "content_hash": str(match.get("content_hash", ""))[:16],
                "ldu_ids": match.get("ldu_ids", []),
            }
        )
    return payload


def _triage_classifier() -> TriageClassifier:
    return TriageClassifier(rules_file=str(RULES_PATH), profiles_dir=str(PROFILES_DIR))


def _run_pipeline(pdf_path: Path, dpi: int) -> Dict[str, Any]:
    pipeline = ExtractionPipeline(max_workers=1)
    if hasattr(pipeline.router, "vision_extractor"):
        pipeline.router.vision_extractor.dpi = dpi
        pipeline.router.vision_extractor.ocr_workers = 1
    return pipeline.process_document(pdf_path)


def main() -> None:
    st.set_page_config(page_title="Document Refinery Demo", layout="wide")
    _ensure_dirs()

    st.title("Document Refinery ")
    st.caption("Triage -> Extraction -> PageIndex -> Query with Provenance")

    if "uploaded_pdf_path" not in st.session_state:
        st.session_state.uploaded_pdf_path = None
    if "uploaded_pdf_name" not in st.session_state:
        st.session_state.uploaded_pdf_name = None
    if "triage_profile" not in st.session_state:
        st.session_state.triage_profile = None
    if "extraction_result" not in st.session_state:
        st.session_state.extraction_result = None
    if "pageindex_lookup_results" not in st.session_state:
        st.session_state.pageindex_lookup_results = None
    if "query_result" not in st.session_state:
        st.session_state.query_result = None

    with st.sidebar:
        st.subheader("Input")
        uploaded_file = st.file_uploader("Upload PDF", type=["pdf"])
        dpi = st.slider("Vision DPI", min_value=100, max_value=300, value=150, step=25)
        use_existing = st.checkbox("Use Existing Extraction", value=False)
        existing_files = sorted(EXTRACTIONS_DIR.glob("*_extraction.json"))
        selected_existing = None
        if use_existing and existing_files:
            selected_existing = st.selectbox("Existing Extraction", existing_files, format_func=lambda p: p.name)

    if uploaded_file is not None:
        if st.session_state.uploaded_pdf_name != uploaded_file.name:
            st.session_state.uploaded_pdf_path = str(_save_upload(uploaded_file))
            st.session_state.uploaded_pdf_name = uploaded_file.name
            st.session_state.triage_profile = None
            st.session_state.extraction_result = None

    if use_existing and selected_existing is not None:
        extraction = _read_json(selected_existing)
        st.session_state.extraction_result = extraction
        st.session_state.uploaded_pdf_path = extraction.get("file_path")
        st.session_state.uploaded_pdf_name = Path(st.session_state.uploaded_pdf_path).name if st.session_state.uploaded_pdf_path else None

    pdf_path = Path(st.session_state.uploaded_pdf_path) if st.session_state.uploaded_pdf_path else None
    if pdf_path is None or not pdf_path.exists():
        st.info("Upload a PDF to start the demo sequence.")
        return

    st.write(f"**Current document:** `{pdf_path.name}`")

    triage_tab, extraction_tab, pageindex_tab, query_tab = st.tabs(
        ["1. Triage", "2. Extraction", "3. PageIndex", "4. Query with Provenance"]
    )

    with triage_tab:
        st.subheader("DocumentProfile")
        if st.button("Run Triage", use_container_width=True):
            profile = _triage_classifier().classify_document(str(pdf_path))
            st.session_state.triage_profile = _profile_to_dict(profile)

        profile = st.session_state.triage_profile
        if profile:
            left, right = st.columns([1.2, 1])
            with left:
                st.json(profile)
            with right:
                st.markdown("**Strategy selection**")
                st.write(_strategy_reason(profile))
                st.metric("Origin Type", profile.get("origin_type", "unknown"))
                st.metric("Recommended Strategy", profile.get("recommended_strategy", "unknown"))
                st.metric("Category Confidence", f"{float(profile.get('category_confidence', 0.0)):.2f}")
        else:
            st.caption("Run triage to show the DocumentProfile and strategy explanation.")

    with extraction_tab:
        st.subheader("Extraction")
        if st.button("Run Extraction Pipeline", use_container_width=True):
            st.session_state.extraction_result = _run_pipeline(
                pdf_path=pdf_path,
                dpi=dpi,
            )

        extraction = st.session_state.extraction_result
        if extraction and "error" not in extraction:
            document_id = extraction.get("document_id", pdf_path.stem)
            pages = extraction.get("extraction", {}).get("pages", [])
            first_page = pages[0] if pages else {}
            preview_image = _render_pdf_page(pdf_path, int(first_page.get("page_num", 1) or 1))
            ledger_entry = _latest_ledger_entry(document_id)

            if _has_mock_content(extraction):
                st.warning(
                    "This extraction contains mock OCR placeholder text. "
                    "PageIndex and query results will not be reliable until you rerun extraction with real OCR dependencies available."
                )

            left, right = st.columns(2)
            with left:
                st.markdown("**Original PDF page**")
                if preview_image is not None:
                    st.image(preview_image, use_container_width=True)
            with right:
                st.markdown("**Structured extraction output**")
                if pages:
                    st.dataframe(_page_table(pages), use_container_width=True, hide_index=True)
                with st.expander("Raw extraction JSON"):
                    st.json(extraction.get("extraction", {}))

            st.markdown("**Ledger entry**")
            if ledger_entry:
                st.json(ledger_entry)
            else:
                st.caption("No ledger entry found yet.")
        elif extraction and "error" in extraction:
            st.error(extraction["error"])
        else:
            st.caption("Run extraction to compare the source page with structured output and ledger metrics.")

    with pageindex_tab:
        st.subheader("PageIndex Navigation")
        extraction = st.session_state.extraction_result
        if not extraction or "error" in extraction:
            st.caption("Run extraction first.")
        else:
            document_id = extraction.get("document_id", pdf_path.stem)
            pageindex_file = _pageindex_path(document_id)
            if not pageindex_file.exists():
                st.warning("PageIndex file is missing for this document.")
            else:
                tree = _read_json(pageindex_file)
                st.markdown("**Tree outline**")
                st.code(_tree_outline(tree, pdf_path.stem), language="text")
                sections_df = pd.DataFrame(_tree_rows(tree))
                st.dataframe(sections_df, use_container_width=True, hide_index=True)

                navigator = PageIndexQuery()
                topic = st.text_input(
                    "Locate information in the tree",
                    placeholder="inflation trend, revenue, glossary",
                    key="pageindex_topics",
                )
                if st.button("Locate in Tree", use_container_width=True):
                    topics = _split_topics(topic)
                    st.session_state.pageindex_lookup_results = [
                        {
                            "topic": item,
                            "matches": navigator.top_k_sections(tree, item, k=3),
                        }
                        for item in topics
                    ]

                lookup_results = st.session_state.pageindex_lookup_results
                if lookup_results:
                    st.markdown("**Top section matches without vector search**")
                    for item in lookup_results:
                        st.write(f"**Topic:** `{item['topic']}`")
                        if not item["matches"]:
                            st.info(f"No matching section found for `{item['topic']}`.")
                            continue

                        for result in _topic_result_payload(tree, item["topic"], item["matches"][:1]):
                            st.markdown(f"**Found in:** {result['found_in']}, {result['pages']}")
                            st.markdown(f"**Relevant text:** {result['relevant_text']}")
                            st.caption(
                                f"hash={result['content_hash']} ldu_ids={', '.join(result['ldu_ids'][:3])}"
                            )

    with query_tab:
        st.subheader("Query with Provenance")
        extraction = st.session_state.extraction_result
        if not extraction or "error" in extraction:
            st.caption("Run extraction first.")
        else:
            document_id = extraction.get("document_id", pdf_path.stem)
            pageindex_file = _pageindex_path(document_id)
            if not pageindex_file.exists():
                st.warning("PageIndex file is missing for this document.")
            else:
                tree = _read_json(pageindex_file)
                question = st.text_input(
                    "Ask a natural language question",
                    placeholder="What does the report say about annual inflation or revenue growth?",
                    key="query_question",
                )
                audit_mode = st.checkbox("Verify as claim", value=False)

                if st.button("Ask Question", use_container_width=True):
                    agent = QueryAgent()
                    if question.strip():
                        if audit_mode:
                            payload = agent.audit_claim(
                                claim=question,
                                document_id=document_id,
                                pageindex_tree=tree,
                                document_name=pdf_path.name,
                            )
                            _save_query_result(
                                document_id=document_id,
                                document_name=pdf_path.name,
                                question=question,
                                result=payload,
                                mode="audit",
                            )
                            st.session_state.query_result = {
                                "mode": "audit",
                                "payload": payload,
                            }
                        else:
                            payload = agent.answer_query(
                                question=question,
                                document_id=document_id,
                                document_name=pdf_path.name,
                                pageindex_tree=tree,
                            )
                            _save_query_result(
                                document_id=document_id,
                                document_name=pdf_path.name,
                                question=question,
                                result=payload,
                                mode="answer",
                            )
                            st.session_state.query_result = {
                                "mode": "answer",
                                "payload": payload,
                            }

                stored_query = st.session_state.query_result
                if stored_query:
                    result = stored_query["payload"]
                    if stored_query["mode"] == "audit":
                        st.markdown("**Verification result**")
                        st.json(result)
                        citations = result.get("citations", [])
                    else:
                        st.markdown("**Answer**")
                        st.write(result.get("answer", ""))
                        citations = result.get("provenance_chain", [])
                        st.markdown("**ProvenanceChain**")
                        for citation in citations[:5]:
                            st.write(f"Document: {citation.get('document_name')}")
                            st.write(f"Page: {citation.get('page_number')}")
                            st.write(f"Bounding Box: {citation.get('bbox')}")
                            st.write(f"content_hash: {str(citation.get('content_hash', ''))[:16]}")
                            st.write("---")

                    if citations:
                        st.markdown("**Source verification against PDF**")
                        st.dataframe(_citation_rows(citations), use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()
