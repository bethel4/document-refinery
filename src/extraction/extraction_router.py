from typing import Dict, Any, List
import logging
from pathlib import Path
from tempfile import TemporaryDirectory

import fitz

from .config_loader import load_extraction_rules  # YAML loader

logger = logging.getLogger(__name__)

class ExtractionRouter:
    """Stage 2-ready Router for multi-strategy extraction with page-level confidence escalation."""

    def __init__(self, rules_path: str = ".refinery/rules/extraction_rules.yaml", max_workers: int = 4):
        # Load thresholds and rules
        self.rules = load_extraction_rules(rules_path)
        self.conf_threshold = self.rules.get("confidence_threshold", 0.7)
        self.max_workers = max(1, int(max_workers or 1))

        # Initialize extractors
        # Import extractors lazily (Docling/transformers can be expensive at import time).
        from .fast_text_extractor import FastTextExtractor
        from .layout_extractor import LayoutExtractor
        from .vision_extractor import VisionExtractor

        self.fast_extractor = FastTextExtractor(**self.rules.get("fast_text", {}))
        layout_cfg = dict(self.rules.get("layout", {}))
        layout_cfg.setdefault("max_workers", self.max_workers)
        self.layout_extractor = LayoutExtractor(**layout_cfg)
        self.vision_extractor = VisionExtractor(**self.rules.get("vision", {}))

        # Strategy map
        self.strategy_map = {
            "fast_text": self.fast_extractor,
            "layout": self.layout_extractor,
            "vision": self.vision_extractor,
        }

        logger.info(f"ExtractionRouter initialized with confidence threshold {self.conf_threshold}")

    def route(self, pdf_path: str, profile: Dict[str, Any]) -> Dict[str, Any]:
        """Route document and escalate pages individually if needed."""
        # Prefer deterministic routing triggers over profile recommendation when possible.
        origin_type = (profile.get("origin_type") or "").strip()
        layout_complexity = (profile.get("layout_complexity") or "").strip()

        forced_strategy = (profile.get("force_strategy") or "").strip()
        if forced_strategy and forced_strategy in self.strategy_map:
            recommended_strategy = forced_strategy
        else:
            recommended_strategy = profile.get("recommended_strategy", "fast_text")
            # Routing overrides by origin type.
            if origin_type == "scanned_image":
                recommended_strategy = "vision"
            elif origin_type == "mixed":
                # Design rule: mixed-origin documents default to Docling (layout).
                recommended_strategy = "layout"
            elif origin_type == "native_digital":
                # Start cheap for digital docs; escalate per-page if needed.
                recommended_strategy = "layout" if layout_complexity == "multi_column" else "fast_text"
        doc_id = profile.get("document_id", "unknown")
        page_results_cache: Dict[str, List[Dict[str, Any]]] = {}
        # NOTE: We no longer special-case mixed-origin docs to the hybrid router by default.
        # Mixed docs now start with Docling (layout) as the primary strategy and can still
        # escalate per-page via the usual FastText→Layout→Vision ladder.

        logger.info(f"Routing document {doc_id} using {recommended_strategy}")
        extractor = self.strategy_map.get(recommended_strategy, self.fast_extractor)

        # Initial extraction
        initial_result = extractor.extract(pdf_path, profile)
        pages = initial_result.get("pages", [])

        # Evaluate per-page confidence
        escalated_pages = []
        for page in pages:
            conf = page.get("confidence", 0)
            if conf < self.conf_threshold:
                page_num = page.get("page_num", page.get("page_number", "unknown"))
                logger.warning(f"Page {page_num} below threshold {conf:.2f}, escalating")
                # Decide next strategy
                next_strategy = self._next_strategy(recommended_strategy)
                page_result = self._extract_escalated_page(
                    pdf_path=pdf_path,
                    profile=profile,
                    strategy=next_strategy,
                    page=page,
                    page_results_cache=page_results_cache,
                )
                page_result["escalated_from"] = recommended_strategy
                page_result["strategy_used"] = next_strategy
                escalated_pages.append(page_result)
            else:
                page["escalated_from"] = None
                page["strategy_used"] = recommended_strategy
                escalated_pages.append(page)

        # Compile final result
        avg_confidence = sum(p.get("confidence", 0) for p in escalated_pages) / len(escalated_pages) if escalated_pages else 0
        
        # Add extraction metadata from initial result
        extraction_metadata = initial_result.get("extraction_metadata", {})
        extraction_metadata.update({
            "average_confidence": avg_confidence,
            "pages_processed": len(escalated_pages),
            "confidence_threshold": self.conf_threshold
        })
        
        return {
            "strategy_used": recommended_strategy,
            "document_id": doc_id,
            "pages": escalated_pages,
            "extraction_metadata": extraction_metadata,
            "routing_metadata": {
                "recommended_strategy": recommended_strategy,
                "actual_strategy": recommended_strategy,
                "escalated": any(p.get("escalated_from") for p in escalated_pages),
                "confidence_threshold": self.conf_threshold,
                "average_confidence": avg_confidence,
                "pages_processed": len(escalated_pages)
            }
        }

    def _next_strategy(self, current: str) -> str:
        """Determine the next strategy in escalation."""
        order = ["fast_text", "layout", "vision"]
        if current not in order:
            return "vision"
        idx = order.index(current)
        if idx + 1 < len(order):
            return order[idx + 1]
        return "vision"  # ultimate fallback

    def _extract_escalated_page(
        self,
        pdf_path: str,
        profile: Dict[str, Any],
        strategy: str,
        page: Dict[str, Any],
        page_results_cache: Dict[str, List[Dict[str, Any]]],
    ) -> Dict[str, Any]:
        """Extract a single page using target strategy with compatibility fallback.

        Preferred path uses extractor.extract_page(page, profile) when implemented.
        Fallback runs extractor.extract(pdf_path, profile) once per strategy and selects the page.
        """
        extractor = self.strategy_map[strategy]
        page_num = page.get("page_num", page.get("page_number"))

        if hasattr(extractor, "extract_page"):
            return extractor.extract_page(page, profile)

        if strategy not in page_results_cache:
            full_result = extractor.extract(pdf_path, profile)
            page_results_cache[strategy] = full_result.get("pages", [])

        strategy_pages = page_results_cache.get(strategy, [])
        for candidate in strategy_pages:
            candidate_num = candidate.get("page_num", candidate.get("page_number"))
            if page_num is not None and candidate_num == page_num:
                return candidate

        logger.warning(
            "Escalation fallback could not find page %s for strategy %s; keeping original page output",
            page_num,
            strategy,
        )
        return page

    def _route_mixed_document(self, pdf_path: str, profile: Dict[str, Any]) -> Dict[str, Any]:
        """Route mixed documents at page granularity for better speed/cost."""
        doc_id = profile.get("document_id", "unknown")
        page_plan = self._build_mixed_page_plan(pdf_path=pdf_path)

        if not page_plan:
            logger.warning("Mixed routing plan is empty. Falling back to strategy routing.")
            fallback_profile = dict(profile)
            fallback_profile["origin_type"] = "unknown"
            return self.route(pdf_path=pdf_path, profile=fallback_profile)

        pages_out: List[Dict[str, Any]] = []
        strategy_counts = {"fast_text": 0, "layout": 0, "vision": 0}
        escalated = False

        with TemporaryDirectory(prefix="mixed_pages_") as tmp_dir:
            with fitz.open(pdf_path) as source_doc:
                for item in page_plan:
                    page_num = item["page_num"]
                    page_strategy = item["strategy"]
                    strategy_counts[page_strategy] = strategy_counts.get(page_strategy, 0) + 1

                    single_page_pdf = str(Path(tmp_dir) / f"page_{page_num:04d}.pdf")
                    self._write_single_page_pdf(source_doc=source_doc, page_num=page_num, output_path=single_page_pdf)

                    page_result = self._extract_single_page(
                        pdf_path=single_page_pdf,
                        profile=profile,
                        strategy=page_strategy,
                        page_num=page_num,
                    )

                    if page_result.get("confidence", 0.0) < self.conf_threshold and page_strategy != "vision":
                        next_strategy = self._next_strategy(page_strategy)
                        escalated_page = self._extract_single_page(
                            pdf_path=single_page_pdf,
                            profile=profile,
                            strategy=next_strategy,
                            page_num=page_num,
                        )
                        escalated_page["escalated_from"] = page_strategy
                        escalated_page["strategy_used"] = next_strategy
                        page_result = escalated_page
                        escalated = True
                    else:
                        page_result["escalated_from"] = None
                        page_result["strategy_used"] = page_strategy

                    pages_out.append(page_result)

        pages_out.sort(key=lambda p: int(p.get("page_num", 0)))

        total_text_length = sum(int(page.get("text_length", 0)) for page in pages_out)
        total_tables = sum(len(page.get("tables", []) or []) for page in pages_out)
        average_confidence = (
            sum(float(page.get("confidence", 0.0)) for page in pages_out) / len(pages_out)
            if pages_out else 0.0
        )

        return {
            "strategy_used": "hybrid",
            "document_id": doc_id,
            "pages": pages_out,
            "extraction_metadata": {
                "total_pages": len(pages_out),
                "total_text_length": total_text_length,
                "total_tables": total_tables,
                "average_confidence": average_confidence,
                "extraction_cost": "medium",
                "processing_time_seconds": 0,
                "ocr_engine": "tesseract",
                "pages_processed": len(pages_out),
                "confidence_threshold": self.conf_threshold,
                "strategy_counts": strategy_counts,
            },
            "routing_metadata": {
                "recommended_strategy": profile.get("recommended_strategy", "hybrid"),
                "actual_strategy": "hybrid",
                "escalated": escalated,
                "confidence_threshold": self.conf_threshold,
                "average_confidence": average_confidence,
                "pages_processed": len(pages_out),
                "strategy_counts": strategy_counts,
            },
        }

    def _build_mixed_page_plan(self, pdf_path: str) -> List[Dict[str, Any]]:
        """Classify each page in a mixed doc into fast_text / layout / vision strategy."""
        page_plan: List[Dict[str, Any]] = []
        with fitz.open(pdf_path) as doc:
            for idx in range(len(doc)):
                page = doc[idx]
                page_num = idx + 1

                text = page.get_text() or ""
                char_count = len(text.strip())

                rect = page.rect
                page_area = max(1.0, rect.width * rect.height)
                image_area = 0.0
                for img in page.get_images():
                    try:
                        bbox = page.get_image_bbox(img[0])
                        if bbox:
                            image_area += bbox.width * bbox.height
                    except Exception:
                        continue
                image_ratio = min(1.0, image_area / page_area)

                # Detect table-like structure or multi-column layout.
                table_like = False
                if text:
                    digits = sum(ch.isdigit() for ch in text)
                    if ("\t" in text) or ("  " in text and digits > 30):
                        table_like = True

                x_positions = []
                try:
                    blocks = page.get_text("blocks") or []
                    for block in blocks:
                        if isinstance(block, (list, tuple)) and len(block) >= 5:
                            x_positions.append(float(block[0]))
                except Exception:
                    pass
                distinct_x = len({round(x, 1) for x in x_positions})
                multi_column = distinct_x >= 3

                # Routing heuristics:
                # - Image-dominated or very low text → vision (OCR)
                # - Table-heavy / multi-column → layout (Docling)
                # - Text-heavy & low image ratio → fast_text
                if char_count < 80 or image_ratio > 0.7:
                    strategy = "vision"
                elif table_like or multi_column:
                    strategy = "layout"
                elif image_ratio > 0.3:
                    strategy = "layout"
                else:
                    strategy = "fast_text"

                page_plan.append(
                    {
                        "page_num": page_num,
                        "char_count": char_count,
                        "image_ratio": image_ratio,
                        "table_like": table_like,
                        "multi_column": multi_column,
                        "strategy": strategy,
                    }
                )
        return page_plan

    def _write_single_page_pdf(self, source_doc: fitz.Document, page_num: int, output_path: str) -> None:
        """Write one page (1-based page_num) into a temporary single-page PDF."""
        single_doc = fitz.open()
        try:
            page_idx = page_num - 1
            single_doc.insert_pdf(source_doc, from_page=page_idx, to_page=page_idx)
            single_doc.save(output_path)
        finally:
            single_doc.close()

    def _extract_single_page(self, pdf_path: str, profile: Dict[str, Any], strategy: str, page_num: int) -> Dict[str, Any]:
        """Extract one single-page PDF with selected strategy and normalize output."""
        extractor = self.strategy_map.get(strategy, self.fast_extractor)
        page_profile = dict(profile)
        page_profile["file_path"] = pdf_path
        page_profile["total_pages"] = 1

        result = extractor.extract(pdf_path, page_profile)
        pages = result.get("pages", [])
        if not pages:
            return {
                "page_num": page_num,
                "text": "",
                "text_length": 0,
                "tables": [],
                "confidence": 0.0,
                "extraction_method": strategy,
            }

        page = dict(pages[0])
        page["page_num"] = page_num
        page.setdefault("text", "")
        page.setdefault("text_length", len(page["text"]))
        page.setdefault("tables", [])
        page.setdefault("confidence", 0.0)
        page.setdefault("extraction_method", strategy)
        return page

    def get_strategy_stats(self) -> Dict[str, Any]:
        return {
            "available_strategies": list(self.strategy_map.keys()),
            "confidence_threshold": self.conf_threshold,
        }
