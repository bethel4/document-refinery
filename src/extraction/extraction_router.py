from typing import Dict, Any, Optional, List
import logging

from .fast_text_extractor import FastTextExtractor
from .layout_extractor import LayoutExtractor
from .vision_extractor import VisionExtractor
from .config_loader import load_extraction_rules  # YAML loader

logger = logging.getLogger(__name__)

class ExtractionRouter:
    """Stage 2-ready Router for multi-strategy extraction with page-level confidence escalation."""

    def __init__(self, rules_path: str = ".refinery/rules/extraction_rules.yaml"):
        # Load thresholds and rules
        self.rules = load_extraction_rules(rules_path)
        self.conf_threshold = self.rules.get("confidence_threshold", 0.7)

        # Initialize extractors
        self.fast_extractor = FastTextExtractor(**self.rules.get("fast_text", {}))
        self.layout_extractor = LayoutExtractor(**self.rules.get("layout", {}))
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
        recommended_strategy = profile.get("recommended_strategy", "fast_text")
        doc_id = profile.get("document_id", "unknown")
        page_results_cache: Dict[str, List[Dict[str, Any]]] = {}

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

    def get_strategy_stats(self) -> Dict[str, Any]:
        return {
            "available_strategies": list(self.strategy_map.keys()),
            "confidence_threshold": self.conf_threshold,
        }
