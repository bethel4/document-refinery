"""
Vision-based Extraction Strategy using Tesseract OCR.
Optimized for scanned documents and image-heavy content.
"""

from typing import Dict, Any, List
import logging
import time
import os
import json
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

from .extractor_base import BaseExtractor

logger = logging.getLogger(__name__)

class VisionExtractor(BaseExtractor):
    """Vision extraction for scanned documents using Tesseract OCR."""
    
    def __init__(self, dpi: int = 150, language: str = 'eng', max_vision_pages: int = 5):
        super().__init__("VisionExtractor")
        self.dpi = dpi
        self.language = language
        self.max_vision_pages = max_vision_pages  # NEW: Limit pages processed
        self.pdf2image_available = PDF2IMAGE_AVAILABLE
        self.tesseract_available = TESSERACT_AVAILABLE
        
        # Output directories
        self.image_dir = Path(".refinery/pages")
        self.ocr_dir = Path(".refinery/ocr")
        
        # Create directories
        self.image_dir.mkdir(parents=True, exist_ok=True)
        self.ocr_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"VisionExtractor initialized: DPI={dpi}, Language={language}, MaxPages={max_vision_pages}")
    
    def extract(self, pdf_path: str, profile: Dict[str, Any]) -> Dict[str, Any]:
        """Extract content using Tesseract OCR approach with performance limits."""
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
            
            # Process each page with OCR - SEQUENTIAL (no nested parallelism)
            for page_num, page_image in enumerate(page_images, start=1):
                # PERFORMANCE LIMIT: Stop after max_vision_pages
                if page_num > self.max_vision_pages:
                    logger.warning(f"Vision page limit reached ({self.max_vision_pages}), skipping remaining pages")
                    break
                
                # Extract with Tesseract
                page_result = self._extract_with_tesseract(page_image, page_num, profile)
                
                if "error" not in page_result:
                    pages_output.append(page_result)
                    total_confidence += page_result["confidence"]
                    total_text_length += page_result["text_length"]
                    tables_found += len(page_result["tables"])
            
            # Compute overall metrics
            avg_confidence = total_confidence / len(pages_output) if pages_output else 0
            
            result = {
                "strategy_used": "vision",
                "pages": pages_output,
                "extraction_metadata": {
                    "total_pages": len(pages_output),
                    "total_text_length": total_text_length,
                    "total_tables": tables_found,
                    "average_confidence": avg_confidence,
                    "extraction_cost": "high",
                    "processing_time_seconds": 0,  # Will be set by pipeline
                    "ocr_engine": "tesseract",
                    "dpi": self.dpi,
                    "language": self.language,
                    "performance_limits": {
                        "max_vision_pages": self.max_vision_pages,
                        "pages_skipped": max(0, len(page_images) - len(pages_output))
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
                "extraction_metadata": {"error": True}
            }
    
    def _extract_with_tesseract(self, page_image, page_num: int, profile: Dict[str, Any]) -> Dict[str, Any]:
        """Extract content from image using Tesseract OCR with bounding boxes."""
        try:
            # Save page image
            pdf_name = Path(profile.get("file_path", "unknown")).stem
            image_path = self.image_dir / f"{pdf_name}_page_{page_num}.png"
            page_image.save(image_path, "PNG")
            
            # Run OCR with stable CLI-style config (works across pytesseract versions).
            tesseract_config = "--psm 3 --oem 3"
            ocr_text = pytesseract.image_to_string(page_image, lang=self.language, config=tesseract_config)
            ocr_data = pytesseract.image_to_data(
                page_image,
                lang=self.language,
                config=tesseract_config,
                output_type=pytesseract.Output.DICT
            )
            
            # Calculate confidence
            confidences = [int(conf) for conf in ocr_data['conf'] if int(conf) > 0]
            avg_confidence = sum(confidences) / len(confidences) / 100 if confidences else 0.5
            
            # Extract bounding boxes and detailed OCR data
            bounding_boxes = []
            words = []
            lines = []
            
            for i in range(len(ocr_data.get('text', []))):
                if ocr_data['text'][i].strip():
                    word_data = {
                        'word': ocr_data['text'][i],
                        'confidence': ocr_data['conf'][i] / 100,
                        'bbox': {
                            'left': ocr_data['left'][i],
                            'top': ocr_data['top'][i],
                            'width': ocr_data['width'][i],
                            'height': ocr_data['height'][i]
                        },
                        'line_num': ocr_data['line_num'][i],
                        'page_num': ocr_data['page_num'][i],
                        'block_num': ocr_data['block_num'][i]
                    }
                    words.append(word_data)
                    
                    # Group words into lines
                    line_num = ocr_data['line_num'][i]
                    if line_num not in [l['line_num'] for l in lines]:
                        line_words = [w for w in words if w['line_num'] == line_num]
                        if line_words:
                            line_bbox = {
                                'line_num': line_num,
                                'words': line_words,
                                'bbox': {
                                    'left': min(w['bbox']['left'] for w in line_words),
                                    'top': min(w['bbox']['top'] for w in line_words),
                                    'right': max(w['bbox']['left'] + w['bbox']['width'] for w in line_words),
                                    'bottom': max(w['bbox']['top'] + w['bbox']['height'] for w in line_words)
                                },
                                'confidence': sum(w['confidence'] for w in line_words) / len(line_words)
                            }
                            lines.append(line_bbox)
            
            # Save detailed OCR data with bounding boxes
            ocr_detailed_path = self.ocr_dir / f"{pdf_name}_page_{page_num}_detailed.json"
            ocr_detailed_data = {
                'page_num': page_num,
                'image_path': str(image_path),
                'text': ocr_text.strip(),
                'avg_confidence': avg_confidence,
                'word_count': len(words),
                'line_count': len(lines),
                'words': words,
                'lines': lines,
                'bounding_boxes': bounding_boxes,
                'extraction_metadata': {
                    'dpi': self.dpi,
                    'language': self.language,
                    'tesseract_config': {
                        'psm': 3,
                        'oem': 3
                    }
                }
            }
            
            with open(ocr_detailed_path, 'w', encoding='utf-8') as f:
                json.dump(ocr_detailed_data, f, indent=2, ensure_ascii=False)
            
            # Save plain text file (for compatibility)
            ocr_path = self.ocr_dir / f"{pdf_name}_page_{page_num}.txt"
            with open(ocr_path, "w", encoding="utf-8") as f:
                f.write(ocr_text.strip())
            
            # Detect tables (basic approach)
            tables = self._detect_tables_from_text(ocr_text, page_num)
            
            return {
                "page_num": page_num,
                "text": ocr_text.strip(),
                "text_length": len(ocr_text.strip()),
                "tables": tables,
                "confidence": avg_confidence,
                "extraction_method": "tesseract_ocr",
                "page_metadata": {
                    "image_path": str(image_path),
                    "ocr_path": str(ocr_path),
                    "ocr_detailed_path": str(ocr_detailed_path),
                    "dpi": self.dpi,
                    "language": self.language,
                    "word_count": len(words),
                    "line_count": len(lines),
                    "bounding_boxes_count": len(words),
                    "avg_confidence": avg_confidence
                }
            }
            
        except Exception as e:
            logger.error(f"Tesseract extraction failed for page {page_num}: {e}")
            return {
                "page_num": page_num,
                "text": "",
                "text_length": 0,
                "tables": [],
                "confidence": 0.2,
                "error": str(e)
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
                        "table_id": 1,
                        "rows": len(table_data),
                        "columns": len(table_data[0]) if table_data else 0,
                        "data": {
                            "headers": table_data[0] if table_data else [],
                            "data": table_data[1:] if len(table_data) > 1 else []
                        },
                        "confidence": 0.6,  # Lower confidence for OCR table detection
                        "detection_method": "ocr_text_pattern"
                    })
            
        except Exception as e:
            logger.warning(f"Table detection failed for page {page_num}: {e}")
        
        return tables
    
    def _mock_extract(self, pdf_path: str, profile: Dict[str, Any]) -> Dict[str, Any]:
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
                mock_tables = [{
                    "table_id": 1,
                    "rows": random.randint(8, 20),
                    "columns": random.randint(4, 7),
                    "data": {
                        "headers": ["Date", "Description", "Amount", "Balance"],
                        "data": [
                            [f"2024-0{i}-{j}" for j in range(4)]
                            for i in range(random.randint(8, 15))
                        ]
                    },
                    "confidence": 0.75,
                    "detection_method": "mock_ocr_pattern"
                }]
            
            pages.append({
                "page_num": page_num,
                "text": mock_text,
                "text_length": len(mock_text),
                "tables": mock_tables,
                "confidence": random.uniform(0.6, 0.85),
                "extraction_method": "mock_tesseract_ocr",
                "page_metadata": {
                    "image_path": f".refinery/pages/mock_page_{page_num}.png",
                    "ocr_path": f".refinery/ocr/mock_page_{page_num}.txt",
                    "dpi": 300,
                    "language": "eng",
                    "word_count": len(mock_text.split()),
                    "line_count": len(mock_text.split('\n'))
                }
            })
        
        avg_confidence = sum(p["confidence"] for p in pages) / len(pages)
        
        return {
            "strategy_used": "vision",
            "pages": pages,
            "extraction_metadata": {
                "total_pages": len(pages),
                "total_text_length": sum(p["text_length"] for p in pages),
                "total_tables": sum(len(p["tables"]) for p in pages),
                "average_confidence": avg_confidence,
                "extraction_cost": "high",
                "processing_time_seconds": 2.0,
                "ocr_engine": "mock_tesseract",
                "dpi": 300,
                "language": "eng"
            }
        }
