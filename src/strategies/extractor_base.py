"""
Base extractor interface for document extraction strategies.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, List
import logging

logger = logging.getLogger(__name__)

class BaseExtractor(ABC):
    """Abstract base class for document extraction strategies."""
    
    def __init__(self, name: str):
        self.name = name
        self.logger = logging.getLogger(f"{__name__}.{name}")
    
    @abstractmethod
    def extract(self, pdf_path: str, profile: Dict[str, Any]):
        """
        Extract content from PDF using specific strategy.
        
        Args:
            pdf_path: Path to PDF file
            profile: Document profile from triage phase
            
        Returns:
            ExtractedDocument with extraction results
        """
        pass
    
    def compute_page_confidence(self, page_data: Dict[str, Any]) -> float:
        """
        Compute confidence score for extracted page content.
        
        Args:
            page_data: Raw page data (text, images, structure)
            
        Returns:
            Confidence score between 0.0 and 1.0
        """
        # Base implementation - override in subclasses
        return 0.5
    
    def validate_extraction(self, result: Dict[str, Any]) -> bool:
        """
        Validate extraction result meets quality standards.
        
        Args:
            result: Extraction result dictionary
            
        Returns:
            True if extraction is valid
        """
        required_fields = ['pages', 'strategy_used', 'extraction_metadata']
        return all(field in result for field in required_fields)
    
    def log_extraction_start(self, pdf_path: str, strategy: str):
        """Log extraction start."""
        self.logger.info(f"[{strategy}] Starting extraction: {pdf_path}")
    
    def log_extraction_complete(self, pdf_path: str, result: Dict[str, Any]):
        """Log extraction completion."""
        pages_count = len(result.get('pages', []))
        avg_confidence = result.get('extraction_metadata', {}).get('average_confidence', 0)
        self.logger.info(f"[{self.name}] Completed {pdf_path}: {pages_count} pages, {avg_confidence:.2f} avg confidence")
