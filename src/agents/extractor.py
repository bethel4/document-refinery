"""
Extraction Agent - ExtractionRouter with confidence-gated escalation.
"""

from typing import Dict, Any, Optional
import logging

from ..models.document_models import ExtractedDocument, ExtractionStrategy, ExtractionLedgerEntry
from ..strategies.fast_text_extractor import FastTextExtractor
from ..strategies.layout_extractor import LayoutExtractor
from ..strategies.vision_extractor import VisionExtractor

logger = logging.getLogger(__name__)

class ExtractionAgent:
    """Extraction Agent with confidence-gated escalation."""
    
    def __init__(self, 
                 confidence_threshold: float = 0.7,
                 fast_text_config: Optional[Dict] = None,
                 layout_config: Optional[Dict] = None,
                 vision_config: Optional[Dict] = None):
        """
        Initialize extraction agent.
        
        Args:
            confidence_threshold: Minimum confidence to avoid escalation
            fast_text_config: Configuration for fast text extractor
            layout_config: Configuration for layout extractor  
            vision_config: Configuration for vision extractor
        """
        self.confidence_threshold = confidence_threshold
        self.logger = logging.getLogger(f"{__name__}.ExtractionAgent")
        
        # Initialize extractors
        self.fast_extractor = FastTextExtractor(**(fast_text_config or {}))
        self.layout_extractor = LayoutExtractor()
        self.vision_extractor = VisionExtractor(**(vision_config or {}))
        
        # Strategy mapping
        self.strategy_map = {
            ExtractionStrategy.FAST_TEXT: self.fast_extractor,
            ExtractionStrategy.LAYOUT: self.layout_extractor,
            ExtractionStrategy.VISION: self.vision_extractor,
            ExtractionStrategy.HYBRID: self.vision_extractor  # Hybrid uses vision
        }
        
        self.logger.info(f"ExtractionAgent initialized with confidence threshold: {confidence_threshold}")
    
    def extract_document(self, pdf_path: str, profile: Dict[str, Any]) -> ExtractedDocument:
        """
        Extract document with confidence-gated escalation.
        
        Args:
            pdf_path: Path to PDF file
            profile: Document profile from triage phase
            
        Returns:
            ExtractedDocument with routing metadata
        """
        recommended_strategy = ExtractionStrategy(profile.get("recommended_strategy", "fast_text"))
        doc_id = profile.get("document_id", "unknown")
        
        self.logger.info(f"Routing {doc_id} to strategy: {recommended_strategy.value}")
        
        # Get initial extraction
        extractor = self.strategy_map.get(recommended_strategy, self.fast_extractor)
        initial_result = extractor.extract(pdf_path, profile)
        
        # Check for errors
        if hasattr(initial_result, 'extraction_metadata') and initial_result.extraction_metadata.get("error"):
            self.logger.error(f"Initial extraction failed: {initial_result.extraction_metadata['error']}")
            return self._fallback_extraction(pdf_path, profile, initial_result)
        
        # Calculate average confidence
        page_confidences = [page.confidence for page in initial_result.pages]
        avg_confidence = sum(page_confidences) / len(page_confidences) if page_confidences else 0
        
        # Check if escalation is needed
        if avg_confidence < self.confidence_threshold:
            return self._escalate_extraction(pdf_path, profile, initial_result, avg_confidence)
        
        # Add routing metadata
        initial_result.routing_metadata = {
            "recommended_strategy": recommended_strategy.value,
            "actual_strategy": recommended_strategy.value,
            "escalated": False,
            "average_confidence": avg_confidence,
            "confidence_threshold": self.confidence_threshold,
            "pages_processed": len(initial_result.pages)
        }
        
        self.logger.info(f"Extraction completed without escalation: {avg_confidence:.3f} confidence")
        return initial_result
    
    def _escalate_extraction(self, pdf_path: str, profile: Dict[str, Any], 
                          initial_result: ExtractedDocument, avg_confidence: float) -> ExtractedDocument:
        """
        Escalate to next strategy when confidence is low.
        """
        recommended_strategy = ExtractionStrategy(profile.get("recommended_strategy", "fast_text"))
        doc_id = profile.get("document_id", "unknown")
        
        self.logger.warning(f"Escalating {doc_id} due to low confidence: {avg_confidence:.3f}")
        
        # Determine escalation strategy
        if recommended_strategy == ExtractionStrategy.FAST_TEXT:
            escalated_strategy = ExtractionStrategy.LAYOUT
        elif recommended_strategy == ExtractionStrategy.LAYOUT:
            escalated_strategy = ExtractionStrategy.VISION
        else:
            # Already at highest strategy, cannot escalate
            self.logger.warning(f"Cannot escalate from {recommended_strategy.value} - already at highest level")
            return initial_result
        
        # Get escalated extraction
        escalated_extractor = self.strategy_map[escalated_strategy]
        escalated_result = escalated_extractor.extract(pdf_path, profile)
        
        # Check for errors in escalation
        if hasattr(escalated_result, 'extraction_metadata') and escalated_result.extraction_metadata.get("error"):
            self.logger.error(f"Escalated extraction failed: {escalated_result.extraction_metadata['error']}")
            return self._combine_results(initial_result, escalated_result, escalated_strategy, True, False)
        
        # Calculate escalated confidence
        page_confidences = [page.confidence for page in escalated_result.pages]
        escalated_confidence = sum(page_confidences) / len(page_confidences) if page_confidences else 0
        
        # Combine results
        combined_result = self._combine_results(initial_result, escalated_result, escalated_strategy, True, escalated_confidence)
        
        self.logger.info(f"Escalation completed: {recommended_strategy.value} → {escalated_strategy.value}, confidence: {avg_confidence:.3f} → {escalated_confidence:.3f}")
        return combined_result
    
    def _fallback_extraction(self, pdf_path: str, profile: Dict[str, Any], 
                         failed_result: ExtractedDocument) -> ExtractedDocument:
        """
        Provide fallback extraction when primary strategy fails.
        """
        self.logger.error(f"Attempting fallback extraction for {profile.get('document_id', 'unknown')}")
        
        # Try vision as ultimate fallback
        try:
            fallback_result = self.vision_extractor.extract(pdf_path, profile)
            fallback_result.routing_metadata = {
                "recommended_strategy": profile.get("recommended_strategy", "fast_text"),
                "actual_strategy": "vision",
                "escalated": True,
                "fallback": True,
                "original_error": failed_result.extraction_metadata.get("error", "Unknown error")
            }
            return fallback_result
        except Exception as e:
            self.logger.error(f"Fallback extraction also failed: {e}")
            # Return error document
            return ExtractedDocument(
                document_id=profile.get("document_id", "unknown"),
                strategy_used="fallback_failed",
                pages=[],
                extraction_metadata={"error": f"Primary and fallback extraction failed: {failed_result.extraction_metadata.get('error')}, {str(e)}"}
            )
    
    def _combine_results(self, initial_result: ExtractedDocument, escalated_result: ExtractedDocument,
                      final_strategy: ExtractionStrategy, escalated: bool, final_confidence: float) -> ExtractedDocument:
        """
        Combine initial and escalated extraction results.
        """
        # Use escalated result as primary
        combined = escalated_result
        
        # Add routing metadata
        combined.routing_metadata = {
            "recommended_strategy": initial_result.strategy_used,
            "actual_strategy": final_strategy.value,
            "escalated": escalated,
            "average_confidence": final_confidence,
            "confidence_threshold": self.confidence_threshold,
            "pages_processed": len(escalated_result.pages),
            "initial_strategy_confidence": self._calculate_avg_confidence(initial_result),
            "escalated_strategy_confidence": final_confidence
        }
        
        # Preserve initial extraction metadata for comparison
        combined.initial_extraction = {
            "strategy": initial_result.strategy_used,
            "confidence": self._calculate_avg_confidence(initial_result),
            "pages": len(initial_result.pages)
        }
        
        return combined
    
    def _calculate_avg_confidence(self, result: ExtractedDocument) -> float:
        """Calculate average confidence from extraction result."""
        page_confidences = [page.confidence for page in result.pages]
        return sum(page_confidences) / len(page_confidences) if page_confidences else 0
    
    def create_ledger_entry(self, profile: Dict[str, Any], result: ExtractedDocument, 
                          processing_time: float) -> ExtractionLedgerEntry:
        """Create ledger entry for extraction tracking."""
        import time
        
        return ExtractionLedgerEntry(
            timestamp=time.strftime("%Y-%m-%d %H:%M:%S"),
            document_id=profile.get("document_id", "unknown"),
            filename=profile.get("filename", ""),
            origin_type=profile.get("origin_type", "unknown"),
            category=profile.get("category", "unknown"),
            recommended_strategy=ExtractionStrategy(profile.get("recommended_strategy", "fast_text")),
            actual_strategy=ExtractionStrategy(result.strategy_used),
            escalated=result.routing_metadata.get("escalated", False) if result.routing_metadata else False,
            confidence=result.routing_metadata.get("average_confidence", 0) if result.routing_metadata else 0,
            pages_processed=len(result.pages),
            total_duration=processing_time,
            extraction_cost=result.extraction_metadata.get("extraction_cost", "unknown"),
            status="success" if not result.extraction_metadata.get("error") else "failed",
            error_message=result.extraction_metadata.get("error")
        )
    
    def get_strategy_stats(self) -> Dict[str, Any]:
        """Get statistics about available strategies."""
        return {
            "available_strategies": [s.value for s in self.strategy_map.keys()],
            "confidence_threshold": self.confidence_threshold,
            "extractors": {
                "fast_text": type(self.fast_extractor).__name__,
                "layout": type(self.layout_extractor).__name__,
                "vision": type(self.vision_extractor).__name__
            }
        }
