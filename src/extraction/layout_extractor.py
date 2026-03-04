"""
Layout-based Extraction Strategy using Docling 2.x
Optimized for structured documents with tables and complex layouts.
"""

from typing import Dict, Any, List
import logging
import time
import re

logger = logging.getLogger(__name__)

# ----------------------------
# Docling Import (Modern API)
# ----------------------------
try:
    from docling.document_converter import DocumentConverter
    DOCLING_AVAILABLE = True
except ImportError:
    logger.warning("Docling not installed. Layout extractor will use mock implementation.")
    DocumentConverter = None
    DOCLING_AVAILABLE = False


from .extractor_base import BaseExtractor


class LayoutExtractor(BaseExtractor):
    """Layout extraction using Docling 2.x"""

    def __init__(self):
        super().__init__("LayoutExtractor")
        self.docling_available = DOCLING_AVAILABLE

    # --------------------------------------------------
    # Main Extraction Entry
    # --------------------------------------------------
    def extract(self, pdf_path: str, profile: Dict[str, Any]) -> Dict[str, Any]:
        self.log_extraction_start(pdf_path, "layout")

        if not self.docling_available:
            return self._mock_extract(pdf_path, profile)

        start_time = time.time()

        try:
            converter = DocumentConverter()
            conversion_result = converter.convert(pdf_path)
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
                tables = self._extract_tables_from_markdown(page_text)

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
                    "extraction_method": "docling_layout",
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
                "extraction_metadata": {"error": True}
            }

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

    def _extract_tables_from_markdown(self, markdown: str) -> List[Dict[str, Any]]:
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
                "table_id": idx + 1,
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

    # --------------------------------------------------
    # Mock Fallback
    # --------------------------------------------------

    def _mock_extract(self, pdf_path: str, profile: Dict[str, Any]) -> Dict[str, Any]:
        import random

        pages = []
        total_pages = profile.get("total_pages", 5)

        for page_num in range(1, min(total_pages + 1, 6)):
            mock_text = f"Mock structured text for page {page_num}."

            pages.append({
                "page_num": page_num,
                "text": mock_text,
                "text_length": len(mock_text),
                "tables": [],
                "confidence": random.uniform(0.7, 0.9),
                "extraction_method": "mock_layout"
            })

        return {
            "strategy_used": "layout",
            "pages": pages,
            "document_elements": {"document_structure": "mock"},
            "extraction_metadata": {
                "total_pages": len(pages),
                "total_text_length": sum(p["text_length"] for p in pages),
                "total_tables": 0,
                "average_confidence": sum(p["confidence"] for p in pages) / len(pages),
                "extraction_cost": "medium",
                "processing_time_seconds": 1.0,
                "docling_version": "mock"
            }
        }