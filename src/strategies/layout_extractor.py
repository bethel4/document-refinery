"""
Layout-based Extraction Strategy using Docling.
Optimized for structured documents with tables, forms, and complex layouts.
"""

from typing import Dict, Any, List
import logging

try:
    from docling.document import Document as DoclingDocument
    from docling.datamodel.base_models import TextElement, TableElement
except ImportError:
    logger = logging.getLogger(__name__)
    logger.warning("Docling not installed. Layout extractor will use mock implementation.")
    DoclingDocument = None
    TextElement = None
    TableElement = None

from ..models.document_models import ExtractedDocument, ExtractedPage, ExtractedTable
from ..strategies.extractor_base import BaseExtractor

logger = logging.getLogger(__name__)

class LayoutExtractor(BaseExtractor):
    """Layout extraction for structured documents using Docling."""
    
    def __init__(self):
        super().__init__("LayoutExtractor")
        self.docling_available = DoclingDocument is not None
    
    def extract(self, pdf_path: str, profile: Dict[str, Any]) -> ExtractedDocument:
        """Extract structured content using Docling."""
        self.log_extraction_start(pdf_path, "layout")
        
        if not self.docling_available:
            return self._mock_extract(pdf_path, profile)
        
        try:
            # Process with Docling
            doc = DoclingDocument.from_pdf(pdf_path)
            
            pages_output = []
            total_confidence = 0
            total_text_length = 0
            tables_found = 0
            
            # Process each page
            for page_elem in doc.pages:
                page_result = self._process_docling_page(page_elem)
                pages_output.append(page_result)
                
                total_confidence += page_result.confidence
                total_text_length += page_result.text_length
                tables_found += len(page_result.tables)
            
            # Extract document-level elements
            document_elements = self._extract_document_elements(doc)
            
            # Compute overall metrics
            avg_confidence = total_confidence / len(pages_output) if pages_output else 0
            
            result = ExtractedDocument(
                document_id=profile.get("document_id", "unknown"),
                strategy_used="layout",
                pages=pages_output,
                extraction_metadata={
                    "total_pages": len(pages_output),
                    "total_text_length": total_text_length,
                    "total_tables": tables_found,
                    "average_confidence": avg_confidence,
                    "extraction_cost": "medium",
                    "processing_time_seconds": 0,  # Will be set by pipeline
                    "docling_version": "latest" if self.docling_available else "mock"
                }
            )
            
            self.log_extraction_complete(pdf_path, result.dict())
            return result
            
        except Exception as e:
            logger.error(f"Layout extraction failed: {e}")
            # Return error document
            return ExtractedDocument(
                document_id=profile.get("document_id", "unknown"),
                strategy_used="layout",
                pages=[],
                extraction_metadata={"error": str(e)}
            )
    
    def _process_docling_page(self, page_elem) -> ExtractedPage:
        """Process a single Docling page element."""
        try:
            # Extract text elements
            text_elements = [elem for elem in page_elem.elements if isinstance(elem, TextElement)]
            page_text = " ".join([elem.text for elem in text_elements])
            
            # Extract table elements
            table_elements = [elem for elem in page_elem.elements if isinstance(elem, TableElement)]
            tables = []
            
            for table_idx, table_elem in enumerate(table_elements):
                table_data = self._convert_docling_table(table_elem)
                if table_data:
                    tables.append(ExtractedTable(
                        table_id=table_idx + 1,
                        rows=len(table_data.get("data", [])),
                        columns=len(table_data.get("headers", [])),
                        headers=table_data.get("headers", []),
                        data=table_data.get("data", []),
                        confidence=0.85,
                        detection_method="docling_table"
                    ))
            
            # Compute page confidence
            confidence = self._compute_layout_confidence(text_elements, table_elements, page_elem)
            
            return ExtractedPage(
                page_num=getattr(page_elem, 'page_no', 0),
                text=page_text,
                text_length=len(page_text),
                tables=tables,
                confidence=confidence,
                extraction_method="docling_layout",
                page_metadata={
                    "width": getattr(page_elem, 'width', 0),
                    "height": getattr(page_elem, 'height', 0),
                    "element_types": list(set(type(elem).__name__ for elem in page_elem.elements))
                }
            )
            
        except Exception as e:
            logger.warning(f"Docling page processing failed: {e}")
            return ExtractedPage(
                page_num=getattr(page_elem, 'page_no', 0),
                text="",
                text_length=0,
                tables=[],
                confidence=0.3,
                error=str(e)
            )
    
    def _convert_docling_table(self, table_elem) -> Dict[str, Any]:
        """Convert Docling table to standard format."""
        try:
            if hasattr(table_elem, 'data') and hasattr(table_elem, 'headers'):
                return {
                    "headers": table_elem.headers,
                    "data": table_elem.data,
                    "confidence": 0.85
                }
            return None
        except Exception as e:
            logger.warning(f"Table conversion failed: {e}")
            return None
    
    def _extract_document_elements(self, doc) -> Dict[str, Any]:
        """Extract document-level elements."""
        try:
            # Count different element types
            text_elements = 0
            table_elements = 0
            image_elements = 0
            
            for page in doc.pages:
                for elem in page.elements:
                    if isinstance(elem, TextElement):
                        text_elements += 1
                    elif isinstance(elem, TableElement):
                        table_elements += 1
                    # Add image element counting when available
            
            return {
                "total_text_elements": text_elements,
                "total_table_elements": table_elements,
                "total_image_elements": image_elements,
                "document_structure": "complex" if table_elements > 5 else "moderate"
            }
            
        except Exception as e:
            logger.warning(f"Document element extraction failed: {e}")
            return {"error": str(e)}
    
    def _compute_layout_confidence(self, text_elements: List, table_elements: List, page_elem) -> float:
        """Compute confidence for layout-based extraction."""
        try:
            # Base confidence from text extraction
            text_confidence = 0.7 if text_elements else 0.3
            
            # Table detection confidence
            table_confidence = min(len(table_elements) * 0.1, 0.3)
            
            # Page structure confidence
            if hasattr(page_elem, 'elements') and len(page_elem.elements) > 0:
                structure_confidence = 0.2
            else:
                structure_confidence = 0.0
            
            # Overall confidence
            confidence = text_confidence + table_confidence + structure_confidence
            return max(0.0, min(1.0, confidence))
            
        except Exception as e:
            logger.warning(f"Layout confidence calculation failed: {e}")
            return 0.6
    
    def _mock_extract(self, pdf_path: str, profile: Dict[str, Any]) -> ExtractedDocument:
        """Mock implementation when Docling is not available."""
        import time
        import random
        
        # Simulate processing time
        time.sleep(1.5)
        
        # Mock structured extraction
        pages = []
        total_pages = profile.get("total_pages", 10)
        
        for page_num in range(1, min(total_pages + 1, 6)):  # Mock first 5 pages
            # Generate mock structured content
            mock_text = f"Mock structured text for page {page_num} with layout analysis."
            
            # Add tables for some pages
            mock_tables = []
            if page_num in [2, 4]:
                mock_tables = [ExtractedTable(
                    table_id=1,
                    rows=random.randint(5, 15),
                    columns=random.randint(3, 8),
                    headers=[f"Column {i}" for i in range(random.randint(3, 6))],
                    data=[[f"Data {i}-{j}" for j in range(random.randint(3, 6))] 
                           for i in range(random.randint(5, 10))],
                    confidence=0.85,
                    detection_method="mock_layout"
                )]
            
            pages.append(ExtractedPage(
                page_num=page_num,
                text=mock_text,
                text_length=len(mock_text),
                tables=mock_tables,
                confidence=random.uniform(0.7, 0.95),
                extraction_method="mock_layout",
                page_metadata={
                    "width": 612,
                    "height": 792,
                    "element_types": ["TextElement", "TableElement"]
                }
            ))
        
        avg_confidence = sum(p.confidence for p in pages) / len(pages)
        
        return ExtractedDocument(
            document_id=profile.get("document_id", "unknown"),
            strategy_used="layout",
            pages=pages,
            extraction_metadata={
                "total_pages": len(pages),
                "total_text_length": sum(p.text_length for p in pages),
                "total_tables": sum(len(p.tables) for p in pages),
                "average_confidence": avg_confidence,
                "extraction_cost": "medium",
                "processing_time_seconds": 1.5,
                "docling_version": "mock"
            }
        )
