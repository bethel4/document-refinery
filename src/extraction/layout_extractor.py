"""
Layout-based Extraction Strategy using Docling 2.x
Optimized for structured documents with tables and complex layouts.
"""

from typing import Dict, Any, List
import logging
import time
import re

logger = logging.getLogger(__name__)

DocumentConverter = None
DOCLING_AVAILABLE = None  # lazily determined at runtime


from .extractor_base import BaseExtractor


class LayoutExtractor(BaseExtractor):
    """Layout extraction using Docling 2.x"""

    def __init__(self, max_num_pages: int | None = None, max_file_size: int | None = None, **_: Any):
        super().__init__("LayoutExtractor")
        self.max_num_pages = max_num_pages
        self.max_file_size = max_file_size

    def _get_docling_converter(self):
        """Import Docling lazily to avoid long import times at startup."""
        global DocumentConverter, DOCLING_AVAILABLE
        if DOCLING_AVAILABLE is False:
            return None
        if DocumentConverter is not None:
            return DocumentConverter

        started = time.time()
        try:
            from docling.document_converter import DocumentConverter as _DocumentConverter

            DocumentConverter = _DocumentConverter
            DOCLING_AVAILABLE = True
            logger.info("Docling imported successfully in %.2fs", time.time() - started)
            return DocumentConverter
        except Exception as exc:
            DOCLING_AVAILABLE = False
            logger.warning("Docling import failed (%s) after %.2fs", exc, time.time() - started)
            return None

    # --------------------------------------------------
    # Main Extraction Entry
    # --------------------------------------------------
    def extract(self, pdf_path: str, profile: Dict[str, Any]) -> Dict[str, Any]:
        self.log_extraction_start(pdf_path, "layout")

        DocumentConverterLocal = self._get_docling_converter()
        if not DocumentConverterLocal:
            return {
                "strategy_used": "layout",
                "error": "Docling is not available in this environment.",
                "pages": [],
                "extraction_metadata": {
                    "total_pages": 0,
                    "total_text_length": 0,
                    "total_tables": 0,
                    "average_confidence": 0.0,
                    "extraction_cost": "medium",
                    "processing_time_seconds": 0.0,
                    "pages_processed": 0,
                    "confidence_threshold": 0.7,
                    "error": True,
                },
            }

        start_time = time.time()

        try:
            converter = DocumentConverterLocal()

            max_num_pages = self.max_num_pages
            if max_num_pages is None:
                # Safety default: avoid spending minutes on huge PDFs.
                max_num_pages = int(profile.get("total_pages") or 0) or 50

            max_file_size = self.max_file_size
            if max_file_size is None:
                max_file_size = 50 * 1024 * 1024  # 50MB default cap

            conversion_result = converter.convert(
                pdf_path,
                max_num_pages=int(max_num_pages),
                max_file_size=int(max_file_size),
            )
            doc = conversion_result.document

            # Export structured markdown
            markdown_content = doc.export_to_markdown()

            # Parse into pages
            pages = self._split_markdown_into_pages(markdown_content)

            pages_output = []
            total_text_length = 0
            total_tables = 0
            total_confidence = 0

            for idx, page_text in enumerate(pages, start=1):
                tables = self._extract_tables_from_markdown(page_text, page_num=idx)

                text_without_tables = self._remove_tables_from_markdown(page_text)

                confidence = self._compute_layout_confidence(
                    text_without_tables,
                    tables
                )

                page_result = {
                    "page_num": idx,
                    "text": text_without_tables.strip(),
                    "text_length": len(text_without_tables),
                    "tables": tables,
                    "confidence": confidence,
                    "extraction_method": "layout",
                }

                pages_output.append(page_result)
                total_text_length += len(text_without_tables)
                total_tables += len(tables)
                total_confidence += confidence

            avg_confidence = (
                total_confidence / len(pages_output)
                if pages_output else 0
            )

            result = {
                "strategy_used": "layout",
                "pages": pages_output,
                "document_elements": {
                    "total_tables": total_tables,
                    "document_structure": "complex" if total_tables > 3 else "moderate"
                },
                "extraction_metadata": {
                    "total_pages": len(pages_output),
                    "total_text_length": total_text_length,
                    "total_tables": total_tables,
                    "average_confidence": avg_confidence,
                    "extraction_cost": "medium",
                    "processing_time_seconds": round(time.time() - start_time, 2),
                    "docling_version": "2.x"
                }
            }

            self.log_extraction_complete(pdf_path, result)
            return result

        except Exception as e:
            logger.error(f"Layout extraction failed: {e}")
            return {
                "strategy_used": "layout",
                "error": str(e),
                "pages": [],
                "extraction_metadata": {
                    "total_pages": 0,
                    "total_text_length": 0,
                    "total_tables": 0,
                    "average_confidence": 0.0,
                    "extraction_cost": "medium",
                    "processing_time_seconds": 0.0,
                    "pages_processed": 0,
                    "confidence_threshold": 0.7,
                    "error": True,
                },
            }

    def extract_page(self, page: Dict[str, Any], profile: Dict[str, Any]) -> Dict[str, Any]:
        """Extract a single page by converting a temporary one-page PDF.

        This avoids converting the full document during page-level escalation.
        """
        page_num = page.get("page_num", page.get("page_number"))
        try:
            page_num_int = int(page_num)
        except Exception:
            page_num_int = None

        source_pdf = profile.get("file_path") or profile.get("pdf_path")
        if not source_pdf or not page_num_int:
            fallback = dict(page)
            fallback["extraction_method"] = "layout"
            return fallback

        try:
            import fitz  # type: ignore
            from pathlib import Path
            from tempfile import TemporaryDirectory

            with TemporaryDirectory(prefix="layout_page_") as tmp_dir:
                one_page_path = str(Path(tmp_dir) / f"page_{page_num_int:04d}.pdf")
                with fitz.open(str(source_pdf)) as doc:
                    if page_num_int < 1 or page_num_int > len(doc):
                        raise ValueError(f"Invalid page number {page_num_int} for document with {len(doc)} pages")
                    single = fitz.open()
                    try:
                        idx = page_num_int - 1
                        single.insert_pdf(doc, from_page=idx, to_page=idx)
                        single.save(one_page_path)
                    finally:
                        single.close()

                page_profile = dict(profile)
                page_profile["file_path"] = one_page_path
                page_profile["total_pages"] = 1
                result = self.extract(one_page_path, page_profile)
                pages = result.get("pages", [])
                if pages:
                    out = dict(pages[0])
                    out["page_num"] = page_num_int
                    return out
        except Exception as exc:
            logger.warning("Layout extract_page failed for page %s: %s", page_num, exc)

        fallback = dict(page)
        fallback["extraction_method"] = "layout"
        return fallback

    # --------------------------------------------------
    # Markdown Processing Helpers
    # --------------------------------------------------

    def _split_markdown_into_pages(self, markdown: str) -> List[str]:
        """
        Splits markdown output into logical pages.
        Docling separates pages using page breaks.
        """
        # Simple heuristic split
        pages = re.split(r"\n\s*\n(?=#)", markdown)
        return pages if pages else [markdown]

    def _extract_tables_from_markdown(self, markdown: str, page_num: int) -> List[Dict[str, Any]]:
        """
        Extract markdown tables.
        """
        tables = []
        table_pattern = r"(\|.+?\|\n\|[-:\s|]+\|\n(?:\|.*\|\n?)*)"

        matches = re.findall(table_pattern, markdown)

        for idx, table_md in enumerate(matches):
            rows = [row.strip() for row in table_md.strip().split("\n")]

            if len(rows) < 2:
                continue

            headers = [h.strip() for h in rows[0].split("|") if h.strip()]
            data_rows = []

            for row in rows[2:]:
                cols = [c.strip() for c in row.split("|") if c.strip()]
                if cols:
                    data_rows.append(cols)

            tables.append({
                "table_id": f"layout_table_{idx + 1}",
                "page_num": page_num,
                "headers": headers,
                "data": data_rows,
                "rows": len(data_rows),
                "columns": len(headers),
                "confidence": 0.85
            })

        return tables

    def _remove_tables_from_markdown(self, markdown: str) -> str:
        """
        Remove markdown tables from text.
        """
        table_pattern = r"(\|.+?\|\n\|[-:\s|]+\|\n(?:\|.*\|\n?)*)"
        return re.sub(table_pattern, "", markdown)

    # --------------------------------------------------
    # Confidence Logic
    # --------------------------------------------------

    def _compute_layout_confidence(self, text: str, tables: List[Dict]) -> float:
        """
        Compute confidence for layout extraction.
        """
        text_score = 0.6 if len(text.strip()) > 100 else 0.3
        table_score = min(len(tables) * 0.1, 0.3)
        structure_score = 0.1 if tables else 0.05

        confidence = text_score + table_score + structure_score
        return max(0.0, min(1.0, confidence))
