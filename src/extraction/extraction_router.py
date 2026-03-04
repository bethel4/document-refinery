"""
Extraction Router with confidence-gated escalation.
Routes documents to appropriate extraction strategy and escalates when confidence is low.
"""

from typing import Dict, Any, Optional
import logging

from .fast_text_extractor import FastTextExtractor
from .layout_extractor import LayoutExtractor
from .vision_extractor import VisionExtractor

logger = logging.getLogger(__name__)

class ExtractionRouter:
    """Router for extraction strategies with confidence-gated escalation."""
    
    def __init__(self, 
                 confidence_threshold: float = 0.7,
                 fast_text_config: Optional[Dict] = None,
                 layout_config: Optional[Dict] = None,
                 vision_config: Optional[Dict] = None):
        """
        Initialize extraction router.
        
        Args:
            confidence_threshold: Minimum confidence to avoid escalation
            fast_text_config: Configuration for fast text extractor
            layout_config: Configuration for layout extractor  
            vision_config: Configuration for vision extractor
        """
        self.confidence_threshold = confidence_threshold
        self.logger = logging.getLogger(f"{__name__}.ExtractionRouter")
        
        # Initialize extractors
        self.fast_extractor = FastTextExtractor(**(fast_text_config or {}))
        self.layout_extractor = LayoutExtractor()
        self.vision_extractor = VisionExtractor(**(vision_config or {}))
        
        # Strategy mapping
        self.strategy_map = {
            "fast_text": self.fast_extractor,
            "layout": self.layout_extractor,
            "vision": self.vision_extractor,
            "hybrid": self.vision_extractor  # Hybrid uses vision
        }
        
        self.logger.info(f"ExtractionRouter initialized with confidence threshold: {confidence_threshold}")
    
    def route(self, pdf_path: str, profile: Dict[str, Any]) -> Dict[str, Any]:
        """
        Route document to appropriate extraction strategy.
        
        Args:
            pdf_path: Path to PDF file
            profile: Document profile from triage phase
            
        Returns:
            Extraction result with strategy metadata
        """
        recommended_strategy = profile.get("recommended_strategy", "fast_text")
        doc_id = profile.get("document_id", "unknown")
        
        self.logger.info(f"Routing {doc_id} to strategy: {recommended_strategy}")
        
        # Get initial extraction
        extractor = self.strategy_map.get(recommended_strategy, self.fast_extractor)
        initial_result = extractor.extract(pdf_path, profile)
        
        # Check for errors
        if "error" in initial_result:
            self.logger.error(f"Initial extraction failed: {initial_result['error']}")
            return self._fallback_extraction(pdf_path, profile, initial_result)
        
        # Calculate average confidence
        page_confidences = [page.get("confidence", 0) for page in initial_result.get("pages", [])]
        avg_confidence = sum(page_confidences) / len(page_confidences) if page_confidences else 0
        
        # Check if escalation is needed
        if avg_confidence < self.confidence_threshold:
            return self._escalate_extraction(pdf_path, profile, initial_result, avg_confidence)
        
        # Add routing metadata
        initial_result["routing_metadata"] = {
            "recommended_strategy": recommended_strategy,
            "actual_strategy": recommended_strategy,
            "escalated": False,
            "average_confidence": avg_confidence,
            "confidence_threshold": self.confidence_threshold,
            "pages_processed": len(initial_result.get("pages", []))
        }
        
        self.logger.info(f"Extraction completed without escalation: {avg_confidence:.3f} confidence")
        return initial_result
    
    def _escalate_extraction(self, pdf_path: str, profile: Dict[str, Any], 
                          initial_result: Dict[str, Any], avg_confidence: float) -> Dict[str, Any]:
        """
        Escalate to next strategy when confidence is low.
        """
        recommended_strategy = profile.get("recommended_strategy", "fast_text")
        doc_id = profile.get("document_id", "unknown")
        
        self.logger.warning(f"Escalating {doc_id} due to low confidence: {avg_confidence:.3f}")
        
        # Determine escalation strategy
        if recommended_strategy == "fast_text":
            escalated_strategy = "layout"
        elif recommended_strategy == "layout":
            escalated_strategy = "vision"
        else:
            # Already at highest strategy, cannot escalate
            self.logger.warning(f"Cannot escalate from {recommended_strategy} - already at highest level")
            return initial_result
        
        # Get escalated extraction
        escalated_extractor = self.strategy_map[escalated_strategy]
        escalated_result = escalated_extractor.extract(pdf_path, profile)
        
        # Check for errors in escalation
        if "error" in escalated_result:
            self.logger.error(f"Escalated extraction failed: {escalated_result['error']}")
            return self._combine_results(initial_result, escalated_result, escalated_strategy, True, False)
        
        # Calculate escalated confidence
        page_confidences = [page.get("confidence", 0) for page in escalated_result.get("pages", [])]
        escalated_confidence = sum(page_confidences) / len(page_confidences) if page_confidences else 0
        
        # Combine results
        combined_result = self._combine_results(initial_result, escalated_result, escalated_strategy, True, escalated_confidence)
        
        self.logger.info(f"Escalation completed: {recommended_strategy} → {escalated_strategy}, confidence: {avg_confidence:.3f} → {escalated_confidence:.3f}")
        return combined_result
    
    def _fallback_extraction(self, pdf_path: str, profile: Dict[str, Any], 
                         failed_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Provide fallback extraction when primary strategy fails.
        """
        self.logger.error(f"Attempting fallback extraction for {profile.get('document_id', 'unknown')}")
        
        # Try vision as ultimate fallback
        try:
            fallback_result = self.vision_extractor.extract(pdf_path, profile)
            fallback_result["routing_metadata"] = {
                "recommended_strategy": profile.get("recommended_strategy", "fast_text"),
                "actual_strategy": "vision",
                "escalated": True,
                "fallback": True,
                "original_error": failed_result.get("error", "Unknown error")
            }
            return fallback_result
        except Exception as e:
            self.logger.error(f"Fallback extraction also failed: {e}")
            return {
                "strategy_used": "fallback_failed",
                "error": f"Primary and fallback extraction failed: {failed_result.get('error')}, {str(e)}",
                "pages": [],
                "extraction_metadata": {"error": True}
            }
    
    def _combine_results(self, initial_result: Dict[str, Any], escalated_result: Dict[str, Any],
                      final_strategy: str, escalated: bool, final_confidence: float) -> Dict[str, Any]:
        """
        Combine initial and escalated extraction results.
        """
        # Use escalated result as primary
        combined = escalated_result.copy()
        
        # Add routing metadata
        combined["routing_metadata"] = {
            "recommended_strategy": initial_result.get("strategy_used", "unknown"),
            "actual_strategy": final_strategy,
            "escalated": escalated,
            "average_confidence": final_confidence,
            "confidence_threshold": self.confidence_threshold,
            "pages_processed": len(escalated_result.get("pages", [])),
            "initial_strategy_confidence": self._calculate_avg_confidence(initial_result),
            "escalated_strategy_confidence": final_confidence
        }
        
        # Preserve initial extraction metadata for comparison
        combined["initial_extraction"] = {
            "strategy": initial_result.get("strategy_used"),
            "confidence": self._calculate_avg_confidence(initial_result),
            "pages": len(initial_result.get("pages", []))
        }
        
        return combined
    
    def _calculate_avg_confidence(self, result: Dict[str, Any]) -> float:
        """Calculate average confidence from extraction result."""
        page_confidences = [page.get("confidence", 0) for page in result.get("pages", [])]
        return sum(page_confidences) / len(page_confidences) if page_confidences else 0
    
    def get_strategy_stats(self) -> Dict[str, Any]:
        """Get statistics about available strategies."""
        return {
            "available_strategies": list(self.strategy_map.keys()),
            "confidence_threshold": self.confidence_threshold,
            "extractors": {
                "fast_text": type(self.fast_extractor).__name__,
                "layout": type(self.layout_extractor).__name__,
                "vision": type(self.vision_extractor).__name__
            }
        }
