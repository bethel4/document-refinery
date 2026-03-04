"""
Fast Text Extraction Strategy using pdfplumber.
Optimized for native digital documents with clear text layers.
"""

try:
    import pdfplumber
except ImportError:
    # Mock implementation for testing
    pdfplumber = None

from typing import Dict, Any, List
import logging

from .extractor_base import BaseExtractor

logger = logging.getLogger(__name__)

class MockPDFPage:
    """Mock PDF page for testing without pdfplumber."""
    def __init__(self, page_num: int, width: float = 612, height: float = 792):
        self.page_num = page_num
        self.width = width
        self.height = height
        self.text = f"Mock text content for page {page_num}"
        self.images = []
        self.chars = []
    
    def extract_text(self):
        return self.text

class FastTextExtractor(BaseExtractor):
    """Fast text extraction for simple digital documents."""
    
    def __init__(self, char_threshold: int = 100, image_ratio_threshold: float = 0.5):
        super().__init__("FastTextExtractor")
        self.char_threshold = char_threshold
        self.image_ratio_threshold = image_ratio_threshold
        self.pdfplumber_available = pdfplumber is not None
    
    def extract(self, pdf_path: str, profile: Dict[str, Any]) -> Dict[str, Any]:
        """Extract text content using pdfplumber."""
        self.log_extraction_start(pdf_path, "fast_text")
        
        if not self.pdfplumber_available:
            return self._mock_extract(pdf_path, profile)
        
        try:
            pages_output = []
            total_confidence = 0
            total_text_length = 0
            
            with pdfplumber.open(pdf_path) as pdf:
                for page_num, page in enumerate(pdf.pages, start=1):
                    # Extract text and metadata
                    page_text = page.extract_text() or ""
                    page_chars = len(page_text)
                    
                    # Compute page confidence
                    confidence = self._compute_page_confidence(page, page_text)
                    
                    # Extract tables if present
                    tables = self._extract_tables(page)
                    
                    page_result = {
                        "page_num": page_num,
                        "text": page_text,
                        "text_length": page_chars,
                        "tables": tables,
                        "confidence": confidence,
                        "extraction_method": "pdfplumber_text",
                        "page_metadata": {
                            "width": page.width,
                            "height": page.height,
                            "has_images": len(page.images) > 0,
                            "image_count": len(page.images)
                        }
                    }
                    
                    pages_output.append(page_result)
                    total_confidence += confidence
                    total_text_length += page_chars
            
            # Compute overall metrics
            avg_confidence = total_confidence / len(pages_output) if pages_output else 0
            
            result = {
                "strategy_used": "fast_text",
                "pages": pages_output,
                "extraction_metadata": {
                    "total_pages": len(pages_output),
                    "total_text_length": total_text_length,
                    "average_confidence": avg_confidence,
                    "extraction_cost": "low",
                    "processing_time_seconds": 0,  # Will be set by pipeline
                    "thresholds_used": {
                        "char_threshold": self.char_threshold,
                        "image_ratio_threshold": self.image_ratio_threshold
                    }
                }
            }
            
            self.log_extraction_complete(pdf_path, result)
            return result
            
        except Exception as e:
            logger.error(f"Fast text extraction failed: {e}")
            return {
                "strategy_used": "fast_text",
                "error": str(e),
                "pages": [],
                "extraction_metadata": {"error": True}
            }
    
    def _mock_extract(self, pdf_path: str, profile: Dict[str, Any]) -> Dict[str, Any]:
        """Mock implementation when pdfplumber is not available."""
        import time
        import random
        
        # Simulate processing time
        time.sleep(0.5)
        
        # Mock fast text extraction
        pages = []
        total_pages = profile.get("total_pages", 10)
        
        for page_num in range(1, min(total_pages + 1, 6)):  # Mock first 5 pages
            # Generate mock text content
            mock_text = f"Mock fast text extraction for page {page_num} with simple content."
            
            # Add tables for some pages
            mock_tables = []
            if page_num in [2, 4]:
                mock_tables = [{
                    "table_id": 1,
                    "rows": random.randint(3, 8),
                    "columns": random.randint(2, 4),
                    "data": {
                        "headers": [f"Column {i}" for i in range(random.randint(2, 4))],
                        "data": [[f"Data {i}-{j}" for j in range(random.randint(2, 4))] 
                                 for i in range(random.randint(3, 8))]
                    },
                    "confidence": 0.9
                }]
            
            pages.append({
                "page_num": page_num,
                "text": mock_text,
                "text_length": len(mock_text),
                "tables": mock_tables,
                "confidence": random.uniform(0.8, 0.95),
                "extraction_method": "mock_fast_text",
                "page_metadata": {
                    "width": 612,
                    "height": 792,
                    "has_images": False,
                    "image_count": 0
                }
            })
        
        avg_confidence = sum(p["confidence"] for p in pages) / len(pages)
        
        return {
            "strategy_used": "fast_text",
            "pages": pages,
            "extraction_metadata": {
                "total_pages": len(pages),
                "total_text_length": sum(p["text_length"] for p in pages),
                "total_tables": sum(len(p["tables"]) for p in pages),
                "average_confidence": avg_confidence,
                "extraction_cost": "low",
                "processing_time_seconds": 0.5,
                "pdfplumber_version": "mock"
            }
        }
