"""
Vision-based Extraction Strategy using Tesseract OCR.
Optimized for scanned documents and image-heavy content.
"""

from typing import Dict, Any, List
import logging
import time
import os
from pathlib import Path
from io import BytesIO

try:
    from pdf2image import convert_from_path
    import pytesseract
    from PIL import Image
    PDF2IMAGE_AVAILABLE = True
    TESSERACT_AVAILABLE = True
except ImportError as e:
    logger = logging.getLogger(__name__)
    logger.warning(f"OCR dependencies not available: {e}")
    PDF2IMAGE_AVAILABLE = False
    TESSERACT_AVAILABLE = False
    convert_from_path = None
    pytesseract = None
    Image = None

from ..models.document_models import ExtractedDocument, ExtractedPage, ExtractedTable
from ..strategies.extractor_base import BaseExtractor

logger = logging.getLogger(__name__)

class VisionExtractor(BaseExtractor):
    """Vision extraction for scanned documents using Tesseract OCR."""
    
    def __init__(self, dpi: int = 300, language: str = 'eng'):
        super().__init__("VisionExtractor")
        self.dpi = dpi
        self.language = language
        self.pdf2image_available = PDF2IMAGE_AVAILABLE
        self.tesseract_available = TESSERACT_AVAILABLE
        
        # Output directories
        self.image_dir = Path(".refinery/pages")
        self.ocr_dir = Path(".refinery/ocr")
        
        # Create directories
        self.image_dir.mkdir(parents=True, exist_ok=True)
        self.ocr_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"VisionExtractor initialized: DPI={dpi}, Language={language}")
    
    def extract(self, pdf_path: str, profile: Dict[str, Any]) -> ExtractedDocument:
        """Extract content using Tesseract OCR approach."""
        self.log_extraction_start(pdf_path, "vision")
        
        if not self.pdf2image_available or not self.tesseract_available:
            return self._mock_extract(pdf_path, profile)
        
        try:
            # Convert PDF to images
            page_images = convert_from_path(pdf_path, dpi=self.dpi)
            
            pages_output = []
            total_confidence = 0
            total_text_length = 0
            tables_found = 0
            
            # Process each page with OCR
            for page_num, page_image in enumerate(page_images, start=1):
                # Extract with Tesseract
                page_result = self._extract_with_tesseract(page_image, page_num, profile)
                
                if not hasattr(page_result, 'page_metadata') or not page_result.page_metadata.get("error"):
                    pages_output.append(page_result)
                    total_confidence += page_result.confidence
                    total_text_length += page_result.text_length
                    tables_found += len(page_result.tables)
            
            # Compute overall metrics
            avg_confidence = total_confidence / len(pages_output) if pages_output else 0
            
            result = ExtractedDocument(
                document_id=profile.get("document_id", "unknown"),
                strategy_used="vision",
                pages=pages_output,
                extraction_metadata={
                    "total_pages": len(pages_output),
                    "total_text_length": total_text_length,
                    "total_tables": tables_found,
                    "average_confidence": avg_confidence,
                    "extraction_cost": "high",
                    "processing_time_seconds": 0,  # Will be set by pipeline
                    "ocr_engine": "tesseract",
                    "dpi": self.dpi,
                    "language": self.language
                }
            )
            
            self.log_extraction_complete(pdf_path, result.dict())
            return result
            
        except Exception as e:
            logger.error(f"Vision extraction failed: {e}")
            # Return error document
            return ExtractedDocument(
                document_id=profile.get("document_id", "unknown"),
                strategy_used="vision",
                pages=[],
                extraction_metadata={"error": str(e)}
            )
    
    def _extract_with_tesseract(self, page_image, page_num: int, profile: Dict[str, Any]) -> ExtractedPage:
        """Extract content from image using Tesseract OCR."""
        try:
            # Save page image
            pdf_name = Path(profile.get("file_path", "unknown")).stem
            image_path = self.image_dir / f"{pdf_name}_page_{page_num}.png"
            page_image.save(image_path, "PNG")
            
            # Run OCR with Tesseract
            ocr_text = pytesseract.image_to_string(page_image, lang=self.language)
            
            # Get OCR confidence data
            ocr_data = pytesseract.image_to_data(page_image, lang=self.language, output_type=pytesseract.Output.DICT)
            
            # Calculate confidence
            confidences = [int(conf) for conf in ocr_data['conf'] if int(conf) > 0]
            avg_confidence = sum(confidences) / len(confidences) / 100 if confidences else 0.5
            
            # Detect tables (basic approach)
            tables = self._detect_tables_from_text(ocr_text, page_num)
            
            # Save OCR text file
            ocr_path = self.ocr_dir / f"{pdf_name}_page_{page_num}.txt"
            with open(ocr_path, "w", encoding="utf-8") as f:
                f.write(ocr_text)
            
            return ExtractedPage(
                page_num=page_num,
                text=ocr_text.strip(),
                text_length=len(ocr_text.strip()),
                tables=tables,
                confidence=avg_confidence,
                extraction_method="tesseract_ocr",
                page_metadata={
                    "image_path": str(image_path),
                    "ocr_path": str(ocr_path),
                    "dpi": self.dpi,
                    "language": self.language,
                    "word_count": len(ocr_text.split()),
                    "line_count": len(ocr_text.strip().split('\n'))
                }
            )
            
        except Exception as e:
            logger.error(f"Tesseract extraction failed for page {page_num}: {e}")
            return ExtractedPage(
                page_num=page_num,
                text="",
                text_length=0,
                tables=[],
                confidence=0.2,
                error=str(e)
            )
    
    def _detect_tables_from_text(self, text: str, page_num: int) -> List[ExtractedTable]:
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
                    tables.append(ExtractedTable(
                        table_id=1,
                        rows=len(table_data),
                        columns=len(table_data[0]) if table_data else 0,
                        headers=table_data[0] if table_data else [],
                        data=table_data[1:] if len(table_data) > 1 else [],
                        confidence=0.6,  # Lower confidence for OCR table detection
                        detection_method="ocr_text_pattern"
                    ))
            
        except Exception as e:
            logger.warning(f"Table detection failed for page {page_num}: {e}")
        
        return tables
    
    def _mock_extract(self, pdf_path: str, profile: Dict[str, Any]) -> ExtractedDocument:
        """Mock vision extraction when dependencies are not available."""
        import random
        
        # Simulate processing time
        time.sleep(2.0)
        
        # Mock Tesseract OCR extraction
        pages = []
        total_pages = profile.get("total_pages", 10)
        
        for page_num in range(1, min(total_pages + 1, 4)):  # Mock first 3 pages
            # Generate mock OCR text
            mock_text = f"Mock Tesseract OCR text for scanned page {page_num} with financial data and tables."
            
            # Mock table detection
            mock_tables = []
            if page_num == 2:  # Add table to page 2
                mock_tables = [ExtractedTable(
                    table_id=1,
                    rows=random.randint(8, 20),
                    columns=random.randint(4, 7),
                    headers=["Date", "Description", "Amount", "Balance"],
                    data=[[f"2024-0{i}-{j}" for j in range(4)]
                           for i in range(random.randint(8, 15))],
                    confidence=0.75,
                    detection_method="mock_ocr_pattern"
                )]
            
            pages.append(ExtractedPage(
                page_num=page_num,
                text=mock_text,
                text_length=len(mock_text),
                tables=mock_tables,
                confidence=random.uniform(0.6, 0.85),
                extraction_method="mock_tesseract_ocr",
                page_metadata={
                    "image_path": f".refinery/pages/mock_page_{page_num}.png",
                    "ocr_path": f".refinery/ocr/mock_page_{page_num}.txt",
                    "dpi": 300,
                    "language": "eng",
                    "word_count": len(mock_text.split()),
                    "line_count": len(mock_text.split('\n'))
                }
            ))
        
        avg_confidence = sum(p.confidence for p in pages) / len(pages)
        
        return ExtractedDocument(
            document_id=profile.get("document_id", "unknown"),
            strategy_used="vision",
            pages=pages,
            extraction_metadata={
                "total_pages": len(pages),
                "total_text_length": sum(p.text_length for p in pages),
                "total_tables": sum(len(p.tables) for p in pages),
                "average_confidence": avg_confidence,
                "extraction_cost": "high",
                "processing_time_seconds": 2.0,
                "ocr_engine": "mock_tesseract",
                "dpi": 300,
                "language": "eng"
            }
        )
