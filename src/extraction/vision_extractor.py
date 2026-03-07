"""
Vision-based Extraction Strategy using local Tesseract OCR.

Optimized for scanned documents and image-heavy content.
"""

from typing import Dict, Any, List
import logging
import os
import json
import shutil
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from tempfile import TemporaryDirectory

logger = logging.getLogger(__name__)

try:
    from pdf2image import convert_from_path
    from PIL import Image
    PDF2IMAGE_AVAILABLE = True
except ImportError as e:
    logger.warning(f"pdf2image/PIL not available: {e}")
    PDF2IMAGE_AVAILABLE = False
    convert_from_path = None
    Image = None

try:
    import pytesseract
    TESSERACT_AVAILABLE = True
except Exception as e:
    logger.warning(f"Tesseract OCR not available: {e}")
    pytesseract = None
    TESSERACT_AVAILABLE = False

from .extractor_base import BaseExtractor


def _ocr_page_worker(task: tuple[str, int, str, str]) -> Dict[str, Any]:
    """OCR a single page image in a separate process."""
    image_path, page_num, language, tesseract_config = task
    try:
        from PIL import Image as WorkerImage
        import pytesseract as worker_pytesseract

        with WorkerImage.open(image_path) as page_image:
            ocr_text = worker_pytesseract.image_to_string(
                page_image,
                lang=language,
                config=tesseract_config,
            )
            ocr_data = worker_pytesseract.image_to_data(
                page_image,
                lang=language,
                config=tesseract_config,
                output_type=worker_pytesseract.Output.DICT,
            )

        confidences: List[float] = []
        for conf in ocr_data.get("conf", []):
            try:
                value = float(conf)
            except (TypeError, ValueError):
                continue
            if value > 0:
                confidences.append(value)

        avg_confidence = (sum(confidences) / len(confidences) / 100.0) if confidences else 0.5
        word_count = sum(1 for token in ocr_data.get("text", []) if str(token).strip())
        line_count = sum(1 for line in ocr_text.splitlines() if line.strip())

        return {
            "page_num": page_num,
            "image_path": image_path,
            "text": ocr_text.strip(),
            "confidence": avg_confidence,
            "word_count": word_count,
            "line_count": line_count,
        }
    except Exception as exc:
        return {
            "page_num": page_num,
            "image_path": image_path,
            "text": "",
            "confidence": 0.2,
            "word_count": 0,
            "line_count": 0,
            "error": str(exc),
        }


class VisionExtractor(BaseExtractor):
    """Vision extraction for scanned documents using local Tesseract OCR."""
    
    def __init__(
        self,
        dpi: int = 150,
        language: str = "eng",
        max_vision_pages: int = 5,
        ocr_workers: int = 0,
    ):
        super().__init__("VisionExtractor")
        self.dpi = dpi
        self.language = language
        self.max_vision_pages = max_vision_pages  # NEW: Limit pages processed
        self.ocr_workers = ocr_workers
        self.pdf2image_available = PDF2IMAGE_AVAILABLE
        self.tesseract_available = TESSERACT_AVAILABLE
        
        # Output directories
        self.image_dir = Path(".refinery/pages")
        self.ocr_dir = Path(".refinery/ocr")
        
        # Create directories
        self.image_dir.mkdir(parents=True, exist_ok=True)
        self.ocr_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(
            "VisionExtractor initialized: DPI=%s, Language=%s, MaxPages=%s, Workers=%s, Backend=%s",
            dpi,
            language,
            max_vision_pages,
            ocr_workers or "auto",
            "tesseract" if self.tesseract_available else "unavailable",
        )
    
    def extract(self, pdf_path: str, profile: Dict[str, Any]) -> Dict[str, Any]:
        """Extract content using local Tesseract OCR with performance limits."""
        self.log_extraction_start(pdf_path, "vision")
        
        if not self.pdf2image_available or not self.tesseract_available:
            missing = []
            if not self.pdf2image_available:
                missing.append("pdf2image/PIL")
            if not self.tesseract_available:
                missing.append("pytesseract/tesseract")
            return {
                "strategy_used": "vision",
                "error": f"Real OCR unavailable: missing {', '.join(missing)}",
                "pages": [],
                "extraction_metadata": self._error_metadata(
                    missing_dependencies=missing,
                    message=f"Real OCR unavailable: missing {', '.join(missing)}",
                ),
            }
        
        try:
            total_pages = int(profile.get("total_pages", 0) or 0)
            pages_to_process = self.max_vision_pages if total_pages <= 0 else min(self.max_vision_pages, total_pages)
            pages_to_process = max(1, pages_to_process)

            with TemporaryDirectory(prefix="vision_pages_") as tmp_dir:
                page_image_paths = self._render_pages_to_images(
                    pdf_path=pdf_path,
                    output_dir=tmp_dir,
                    pages_to_process=pages_to_process,
                )
                ocr_outputs = self._run_parallel_ocr(page_image_paths)
                ocr_engine = "tesseract"
                pages_output = []

                for ocr_output in sorted(ocr_outputs, key=lambda item: item["page_num"]):
                    page_result = self._build_page_result(ocr_output=ocr_output, profile=profile)
                    if "error" not in page_result:
                        pages_output.append(page_result)

            total_confidence = sum(page["confidence"] for page in pages_output)
            total_text_length = sum(page["text_length"] for page in pages_output)
            tables_found = sum(len(page["tables"]) for page in pages_output)
            
            # Compute overall metrics
            avg_confidence = total_confidence / len(pages_output) if pages_output else 0
            
            final_total_pages = total_pages or pages_to_process
            result = {
                "strategy_used": "vision",
                "pages": pages_output,
                "combined_text": "\n\n".join(page["text"] for page in pages_output),
                "extraction_metadata": {
                    "total_pages": len(pages_output),
                    "total_text_length": total_text_length,
                    "total_tables": tables_found,
                    "average_confidence": avg_confidence,
                    "extraction_cost": "high",
                    "processing_time_seconds": 0,  # Will be set by pipeline
                    "ocr_engine": ocr_engine,
                    "dpi": self.dpi,
                    "language": self.language,
                    "performance_limits": {
                        "max_vision_pages": self.max_vision_pages,
                        "pages_skipped": max(0, final_total_pages - len(pages_output))
                    }
                }
            }
            
            self.log_extraction_complete(pdf_path, result)
            return result
            
        except Exception as e:
            logger.error(f"Vision extraction failed: {e}")
            return {
                "strategy_used": "vision",
                "error": str(e),
                "pages": [],
                "extraction_metadata": self._error_metadata(message=str(e)),
            }

    def _error_metadata(self, message: str, missing_dependencies: List[str] | None = None) -> Dict[str, Any]:
        return {
            "total_pages": 0,
            "total_text_length": 0,
            "total_tables": 0,
            "average_confidence": 0.0,
            "extraction_cost": "high",
            "processing_time_seconds": 0.0,
            "ocr_engine": "tesseract",
            "dpi": self.dpi,
            "language": self.language,
            "pages_processed": 0,
            "confidence_threshold": 0.7,
            "performance_limits": {
                "max_vision_pages": self.max_vision_pages,
                "error": True,
                "message": message,
                "missing_dependencies": missing_dependencies or [],
            },
        }

    def extract_page(self, page: Dict[str, Any], profile: Dict[str, Any]) -> Dict[str, Any]:
        """Compatibility method for older routers expecting page-level extraction.

        If PDF path is available in profile, run full extraction once and return matching page.
        Otherwise return normalized input page to avoid hard failure during escalation.
        """
        page_num = page.get("page_num", page.get("page_number"))
        pdf_path = profile.get("file_path") or profile.get("pdf_path")
        if pdf_path:
            result = self.extract(str(pdf_path), profile)
            for candidate in result.get("pages", []):
                candidate_num = candidate.get("page_num", candidate.get("page_number"))
                if candidate_num == page_num:
                    return candidate

        fallback = dict(page)
        fallback["extraction_method"] = "vision"
        fallback["tables"] = self._normalize_tables(fallback.get("tables", []), int(page_num or 1))
        return fallback

    def _render_pages_to_images(self, pdf_path: str, output_dir: str, pages_to_process: int) -> List[str]:
        """Render pages to PNG files and return file paths."""
        image_paths = convert_from_path(
            pdf_path,
            dpi=self.dpi,
            first_page=1,
            last_page=pages_to_process,
            fmt="png",
            output_folder=output_dir,
            paths_only=True,
        )
        return [str(path) for path in image_paths]

    def _run_parallel_ocr(self, page_image_paths: List[str]) -> List[Dict[str, Any]]:
        """Run Tesseract OCR across pages using multiple processes."""
        if not page_image_paths:
            return []

        tesseract_config = "--psm 3 --oem 3"
        tasks = [
            (image_path, page_num, self.language, tesseract_config)
            for page_num, image_path in enumerate(page_image_paths, start=1)
        ]

        cpu_count = os.cpu_count() or 1
        requested_workers = self.ocr_workers if self.ocr_workers > 0 else min(4, cpu_count)
        worker_count = max(1, min(requested_workers, len(tasks)))

        if worker_count == 1:
            return [_ocr_page_worker(task) for task in tasks]

        try:
            with ProcessPoolExecutor(max_workers=worker_count) as executor:
                return list(executor.map(_ocr_page_worker, tasks))
        except (PermissionError, OSError) as exc:
            logger.warning(
                "Parallel OCR unavailable (%s). Falling back to sequential OCR.",
                exc,
            )
            return [_ocr_page_worker(task) for task in tasks]

    def _build_page_result(self, ocr_output: Dict[str, Any], profile: Dict[str, Any]) -> Dict[str, Any]:
        """Build normalized page result and persist OCR artifacts."""
        page_num = int(ocr_output["page_num"])
        if "error" in ocr_output:
            return {
                "page_num": page_num,
                "text": "",
                "text_length": 0,
                "tables": [],
                "confidence": 0.2,
                "error": ocr_output["error"],
            }

        pdf_name = Path(profile.get("file_path", "unknown")).stem
        temp_image_path = Path(ocr_output["image_path"])
        image_path = self.image_dir / f"{pdf_name}_page_{page_num}.png"
        shutil.copyfile(temp_image_path, image_path)

        ocr_text = ocr_output["text"]
        ocr_path = self.ocr_dir / f"{pdf_name}_page_{page_num}.txt"
        ocr_path.write_text(ocr_text, encoding="utf-8")

        ocr_detailed_path = self.ocr_dir / f"{pdf_name}_page_{page_num}_detailed.json"
        ocr_detailed_path.write_text(
            json.dumps(
                {
                    "page_num": page_num,
                    "image_path": str(image_path),
                    "text": ocr_text,
                    "avg_confidence": ocr_output["confidence"],
                    "word_count": ocr_output["word_count"],
                    "line_count": ocr_output["line_count"],
                    "extraction_metadata": {
                        "dpi": self.dpi,
                        "language": self.language,
                        "parallel_ocr": True,
                    },
                },
                indent=2,
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )

        tables = self._normalize_tables(self._detect_tables_from_text(ocr_text, page_num), page_num)
        return {
            "page_num": page_num,
            "text": ocr_text,
            "text_length": len(ocr_text),
            "tables": tables,
            "confidence": float(ocr_output["confidence"]),
            "extraction_method": "vision",
            "page_metadata": {
                "image_path": str(image_path),
                "ocr_path": str(ocr_path),
                "ocr_detailed_path": str(ocr_detailed_path),
                "dpi": self.dpi,
                "language": self.language,
                "word_count": int(ocr_output["word_count"]),
                "line_count": int(ocr_output["line_count"]),
                "avg_confidence": float(ocr_output["confidence"]),
            },
        }
    
    def _detect_tables_from_text(self, text: str, page_num: int) -> List[Dict[str, Any]]:
        """Basic table detection from OCR text."""
        tables = []
        
        try:
            lines = text.strip().split('\n')
            
            # Look for table-like patterns
            table_lines = []
            for line in lines:
                # Simple heuristic: multiple spaces or tabs suggest table structure
                if '  ' in line or '\t' in line:
                    table_lines.append(line.strip())
            
            # If we have enough table-like lines, treat as table
            if len(table_lines) >= 3:
                # Parse table data
                table_data = []
                for line in table_lines:
                    # Split by multiple spaces or tabs
                    row = [cell.strip() for cell in line.split('  ') if cell.strip()]
                    if len(row) > 1:  # At least 2 columns
                        table_data.append(row)
                
                if len(table_data) >= 2:  # At least header + 1 data row
                    tables.append({
                        "table_id": "ocr_table_1",
                        "page_num": page_num,
                        "rows": len(table_data),
                        "columns": len(table_data[0]) if table_data else 0,
                        "headers": [str(v) for v in (table_data[0] if table_data else [])],
                        "data": [[str(v) for v in row] for row in (table_data[1:] if len(table_data) > 1 else [])],
                        "confidence": 0.6,  # Lower confidence for OCR table detection
                        "detection_method": "ocr_text_pattern"
                    })
            
        except Exception as e:
            logger.warning(f"Table detection failed for page {page_num}: {e}")
        
        return tables
    
    def _normalize_tables(self, tables: List[Dict[str, Any]], page_num: int) -> List[Dict[str, Any]]:
        """Normalize table payloads to ExtractedDocument schema."""
        normalized: List[Dict[str, Any]] = []
        for idx, table in enumerate(tables, start=1):
            headers = table.get("headers")
            data = table.get("data")

            if isinstance(data, dict):
                headers = headers or data.get("headers", [])
                data = data.get("data", [])

            headers = [str(v) for v in (headers or [])]
            rows_data = [[str(v) for v in row] for row in (data or []) if isinstance(row, list)]
            columns = int(table.get("columns", len(headers)))
            rows = int(table.get("rows", len(rows_data)))

            normalized.append({
                "table_id": str(table.get("table_id", f"table_{idx}")),
                "page_num": int(table.get("page_num", page_num)),
                "rows": rows,
                "columns": columns,
                "headers": headers,
                "data": rows_data,
                "confidence": float(table.get("confidence", 0.6)),
            })
        return normalized

    def _mock_extract(self, pdf_path: str, profile: Dict[str, Any]) -> Dict[str, Any]:
        """Mock vision extraction when dependencies are not available."""
        import random

        time.sleep(1.0)

        pages: List[Dict[str, Any]] = []
        total_pages = int(profile.get("total_pages", 3) or 3)

        for page_num in range(1, min(total_pages + 1, 4)):  # Mock first 3 pages
            mock_text = f"Mock OCR text for scanned page {page_num} with financial data and tables."
            mock_tables: List[Dict[str, Any]] = []
            if page_num == 2:
                mock_tables = [{
                    "table_id": "mock_table_1",
                    "page_num": page_num,
                    "rows": random.randint(8, 20),
                    "columns": random.randint(4, 7),
                    "headers": ["Date", "Description", "Amount", "Balance"],
                    "data": [
                        [f"2024-0{i}-{j}" for j in range(4)]
                        for i in range(random.randint(8, 15))
                    ],
                    "confidence": 0.75,
                    "detection_method": "mock_ocr_pattern",
                }]

            pages.append({
                "page_num": page_num,
                "text": mock_text,
                "text_length": len(mock_text),
                "tables": self._normalize_tables(mock_tables, page_num),
                "confidence": random.uniform(0.6, 0.85),
                "extraction_method": "mock",
                "page_metadata": {
                    "image_path": f".refinery/pages/mock_page_{page_num}.png",
                    "ocr_path": f".refinery/ocr/mock_page_{page_num}.txt",
                    "dpi": self.dpi,
                    "language": self.language,
                    "word_count": len(mock_text.split()),
                    "line_count": len(mock_text.split("\\n")),
                },
            })

        avg_confidence = sum(p["confidence"] for p in pages) / len(pages) if pages else 0.0

        return {
            "strategy_used": "vision",
            "pages": pages,
            "extraction_metadata": {
                "total_pages": len(pages),
                "total_text_length": sum(p["text_length"] for p in pages),
                "total_tables": sum(len(p["tables"]) for p in pages),
                "average_confidence": avg_confidence,
                "extraction_cost": "high",
                "processing_time_seconds": 1.0,
                "ocr_engine": "mock",
                "dpi": self.dpi,
                "language": self.language,
            },
        }
