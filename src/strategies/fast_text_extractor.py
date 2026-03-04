"""
Fast Text Extraction Strategy using pdfplumber.
Optimized for native digital documents with clear text layers.
"""

try:
    import pdfplumber
except ImportError:
    pdfplumber = None

from typing import Dict, Any, List
import logging

from ..models.document_models import ExtractedDocument, ExtractedPage, ExtractedTable
from ..strategies.extractor_base import BaseExtractor

logger = logging.getLogger(__name__)

class FastTextExtractor(BaseExtractor):
    """Fast text extraction for simple digital documents."""
    
    def __init__(self, char_threshold: int = 100, image_ratio_threshold: float = 0.5):
        super().__init__("FastTextExtractor")
        self.char_threshold = char_threshold
        self.image_ratio_threshold = image_ratio_threshold
        self.pdfplumber_available = pdfplumber is not None
    
    def extract(self, pdf_path: str, profile: Dict[str, Any]) -> ExtractedDocument:
        """Extract text content using pdfplumber."""
        self.log_extraction_start(pdf_path, "fast_text")
        
        if not self.pdfplumber_available:
            return self._mock_extract(pdf_path, profile)
        
        try:
            pages_output = []
            total_confidence = 0
            total_text_length = 0
            tables_found = 0
            
            with pdfplumber.open(pdf_path) as pdf:
                for page_num, page in enumerate(pdf.pages, start=1):
                    # Extract text and metadata
                    page_text = page.extract_text() or ""
                    page_chars = len(page_text)
                    
                    # Compute page confidence
                    confidence = self._compute_page_confidence(page, page_text)
                    
                    # Extract tables if present
                    tables = self._extract_tables(page)
                    
                    page_result = ExtractedPage(
                        page_num=page_num,
                        text=page_text,
                        text_length=page_chars,
                        tables=tables,
                        confidence=confidence,
                        extraction_method="pdfplumber_text",
                        page_metadata={
                            "width": page.width,
                            "height": page.height,
                            "has_images": len(page.images) > 0,
                            "image_count": len(page.images)
                        }
                    )
                    
                    pages_output.append(page_result)
                    total_confidence += confidence
                    total_text_length += page_chars
                    tables_found += len(tables)
            
            # Create ExtractedDocument
            avg_confidence = total_confidence / len(pages_output) if pages_output else 0
            
            result = ExtractedDocument(
                document_id=profile.get("document_id", "unknown"),
                strategy_used="fast_text",
                pages=pages_output,
                extraction_metadata={
                    "total_pages": len(pages_output),
                    "total_text_length": total_text_length,
                    "total_tables": tables_found,
                    "average_confidence": avg_confidence,
                    "extraction_cost": "low",
                    "processing_time_seconds": 0,  # Will be set by pipeline
                    "thresholds_used": {
                        "char_threshold": self.char_threshold,
                        "image_ratio_threshold": self.image_ratio_threshold
                    }
                }
            )
            
            self.log_extraction_complete(pdf_path, result.dict())
            return result
            
        except Exception as e:
            logger.error(f"Fast text extraction failed: {e}")
            # Return error document
            return ExtractedDocument(
                document_id=profile.get("document_id", "unknown"),
                strategy_used="fast_text",
                pages=[],
                extraction_metadata={"error": str(e)}
            )
    
    def _compute_page_confidence(self, page, text: str) -> float:
        """Compute confidence score for page extraction."""
        try:
            # Page dimensions
            page_area = page.width * page.height if page.width and page.height else 1
            
            # Text density metrics
            text_length = len(text.strip())
            char_density = text_length / page_area if page_area > 0 else 0
            
            # Image presence penalty
            image_count = len(page.images)
            image_penalty = min(image_count * 0.1, 0.3)
            
            # Text quality signals
            has_meaningful_text = text_length > self.char_threshold
            text_quality_bonus = 0.3 if has_meaningful_text else 0
            
            # Font diversity (indicates structured content)
            chars = page.chars if hasattr(page, 'chars') else []
            font_diversity = len(set(char.get('font', 'unknown') for char in chars))
            font_bonus = min(font_diversity * 0.05, 0.2)
            
            # Calculate confidence
            base_confidence = min(char_density * 1000, 0.5)  # Density-based
            confidence = base_confidence + text_quality_bonus + font_bonus - image_penalty
            
            return max(0.0, min(1.0, confidence))
            
        except Exception as e:
            logger.warning(f"Confidence calculation failed: {e}")
            return 0.5
    
    def _extract_tables(self, page) -> List[ExtractedTable]:
        """Extract tables from page using pdfplumber."""
        try:
            tables = page.extract_tables()
            table_results = []
            
            for table_idx, table in enumerate(tables):
                if table and len(table) > 1:  # Valid table with header + data
                    table_result = ExtractedTable(
                        table_id=table_idx + 1,
                        rows=len(table),
                        columns=len(table[0]) if table else 0,
                        headers=table[0] if table else [],
                        data=table[1:] if len(table) > 1 else [],
                        confidence=0.8,  # High confidence for extracted tables
                        detection_method="pdfplumber_table"
                    )
                    table_results.append(table_result)
            
            return table_results
            
        except Exception as e:
            logger.warning(f"Table extraction failed: {e}")
            return []
    
    def _mock_extract(self, pdf_path: str, profile: Dict[str, Any]) -> ExtractedDocument:
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
                mock_tables = [ExtractedTable(
                    table_id=1,
                    rows=random.randint(3, 8),
                    columns=random.randint(2, 4),
                    headers=[f"Column {i}" for i in range(random.randint(2, 4))],
                    data=[[f"Data {i}-{j}" for j in range(random.randint(2, 4))] 
                           for i in range(random.randint(3, 8))],
                    confidence=0.9,
                    detection_method="mock_table"
                )]
            
            pages.append(ExtractedPage(
                page_num=page_num,
                text=mock_text,
                text_length=len(mock_text),
                tables=mock_tables,
                confidence=random.uniform(0.8, 0.95),
                extraction_method="mock_fast_text",
                page_metadata={
                    "width": 612,
                    "height": 792,
                    "has_images": False,
                    "image_count": 0
                }
            ))
        
        avg_confidence = sum(p.confidence for p in pages) / len(pages)
        
        return ExtractedDocument(
            document_id=profile.get("document_id", "unknown"),
            strategy_used="fast_text",
            pages=pages,
            extraction_metadata={
                "total_pages": len(pages),
                "total_text_length": sum(p.text_length for p in pages),
                "total_tables": sum(len(p.tables) for p in pages),
                "average_confidence": avg_confidence,
                "extraction_cost": "low",
                "processing_time_seconds": 0.5,
                "pdfplumber_version": "mock"
            }
        )
