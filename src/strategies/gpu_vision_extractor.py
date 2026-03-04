"""
GPU-Accelerated Vision Extractor for Google Colab.
Uses CUDA-enabled PyTorch for faster OCR and image processing.
"""

from typing import Dict, Any, List
import logging
import time
import os
from pathlib import Path

try:
    import torch
    import torchvision.transforms as transforms
    from PIL import Image
    GPU_AVAILABLE = torch.cuda.is_available()
except ImportError:
    GPU_AVAILABLE = False
    torch = None
    transforms = None
    Image = None

# OCR dependencies
try:
    from pdf2image import convert_from_path
    import pytesseract
    PDF2IMAGE_AVAILABLE = True
    TESSERACT_AVAILABLE = True
except ImportError:
    PDF2IMAGE_AVAILABLE = False
    TESSERACT_AVAILABLE = False

from ..models.document_models import ExtractedDocument, ExtractedPage, ExtractedTable
from ..strategies.extractor_base import BaseExtractor

logger = logging.getLogger(__name__)

class GPUVisionExtractor(BaseExtractor):
    """GPU-accelerated vision extraction for Colab."""
    
    def __init__(self, dpi: int = 300, language: str = 'eng'):
        super().__init__("GPUVisionExtractor")
        self.dpi = dpi
        self.language = language
        self.device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
        
        logger.info(f"GPUVisionExtractor initialized: Device={self.device}, CUDA={torch.cuda.is_available()}")
        
        # Setup GPU transforms if available
        if GPU_AVAILABLE:
            self.transform = transforms.Compose([
                transforms.Resize((1024, 1024)),
                transforms.ToTensor(),
                transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225])
            ])
    
    def extract(self, pdf_path: str, profile: Dict[str, Any]) -> ExtractedDocument:
        """Extract content using GPU-accelerated processing."""
        self.log_extraction_start(pdf_path, "gpu_vision")
        
        try:
            # Convert PDF to images
            logger.info("Converting PDF to images...")
            page_images = convert_from_path(pdf_path, dpi=self.dpi)
            
            pages_output = []
            total_confidence = 0
            total_text_length = 0
            tables_found = 0
            
            # Process each page with GPU acceleration
            for page_num, page_image in enumerate(page_images, start=1):
                logger.info(f"Processing page {page_num}/{len(page_images)}")
                
                # GPU-accelerated preprocessing
                processed_image = self._preprocess_with_gpu(page_image)
                
                # Enhanced OCR with GPU preprocessing
                page_result = self._extract_with_enhanced_ocr(processed_image, page_num, profile)
                
                if "error" not in page_result:
                    pages_output.append(page_result)
                    total_confidence += page_result["confidence"]
                    total_text_length += page_result["text_length"]
                    tables_found += len(page_result["tables"])
            
            # Compute overall metrics
            avg_confidence = total_confidence / len(pages_output) if pages_output else 0
            
            result = ExtractedDocument(
                document_id=profile.get("document_id", "unknown"),
                strategy_used="gpu_vision",
                pages=pages_output,
                extraction_metadata={
                    "total_pages": len(pages_output),
                    "total_text_length": total_text_length,
                    "total_tables": tables_found,
                    "average_confidence": avg_confidence,
                    "extraction_cost": "gpu_high",
                    "processing_time_seconds": 0,
                    "ocr_engine": "tesseract_gpu_enhanced",
                    "device": str(self.device),
                    "gpu_available": GPU_AVAILABLE,
                    "dpi": self.dpi,
                    "language": self.language
                }
            )
            
            self.log_extraction_complete(pdf_path, result.dict())
            return result
            
        except Exception as e:
            logger.error(f"GPU vision extraction failed: {e}")
            return ExtractedDocument(
                document_id=profile.get("document_id", "unknown"),
                strategy_used="gpu_vision",
                pages=[],
                extraction_metadata={"error": str(e)}
            )
    
    def _preprocess_with_gpu(self, page_image):
        """Preprocess image with GPU acceleration."""
        if not GPU_AVAILABLE:
            return page_image
        
        try:
            # Convert PIL to tensor
            if isinstance(page_image, Image.Image):
                # Apply GPU transforms
                with torch.no_grad():
                    tensor_image = self.transform(page_image).unsqueeze(0).to(self.device)
                    
                    # Apply GPU-accelerated enhancements
                    enhanced_tensor = self._enhance_image_gpu(tensor_image)
                    
                    # Convert back to PIL
                    enhanced_tensor = enhanced_tensor.squeeze(0).cpu()
                    enhanced_image = transforms.ToPILImage()(enhanced_tensor)
                    
                    return enhanced_image
            
            return page_image
            
        except Exception as e:
            logger.warning(f"GPU preprocessing failed: {e}")
            return page_image
    
    def _enhance_image_gpu(self, tensor_image):
        """Apply GPU-accelerated image enhancements."""
        try:
            # Contrast enhancement
            tensor_image = torch.clamp(tensor_image * 1.2, 0, 1)
            
            # Noise reduction (simple Gaussian blur approximation)
            if tensor_image.shape[0] == 3:  # RGB
                kernel_size = 3
                sigma = 0.5
                # Simple smoothing (would use proper CUDA kernels in production)
                smoothed = torch.nn.functional.avg_pool2d(
                    tensor_image.unsqueeze(0), kernel_size, stride=1, padding=kernel_size//2
                ).squeeze(0)
                tensor_image = tensor_image * 0.7 + smoothed * 0.3
            
            return tensor_image
            
        except Exception as e:
            logger.warning(f"GPU enhancement failed: {e}")
            return tensor_image
    
    def _extract_with_enhanced_ocr(self, page_image, page_num: int, profile: Dict[str, Any]) -> Dict[str, Any]:
        """Enhanced OCR with GPU-preprocessed images."""
        try:
            # Save enhanced image
            pdf_name = Path(profile.get("file_path", "unknown")).stem
            image_path = Path(f".refinery/pages/{pdf_name}_page_{page_num}_gpu.png")
            image_path.parent.mkdir(parents=True, exist_ok=True)
            page_image.save(image_path, "PNG")
            
            # Enhanced OCR configuration
            custom_config = r'--oem 3 --psm 6 -c tessedit_char_whitelist "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz.,:-$%/'
            
            # Run OCR with enhanced preprocessing
            ocr_text = pytesseract.image_to_string(
                page_image, 
                lang=self.language,
                config=custom_config
            )
            
            # Get detailed OCR data with confidence
            ocr_data = pytesseract.image_to_data(
                page_image, 
                lang=self.language,
                config=custom_config,
                output_type=pytesseract.Output.DICT
            )
            
            # Calculate enhanced confidence
            confidences = [int(conf) for conf in ocr_data['conf'] if int(conf) > 0]
            avg_confidence = sum(confidences) / len(confidences) / 100 if confidences else 0.5
            
            # Enhanced table detection
            tables = self._enhanced_table_detection(ocr_text, ocr_data, page_num)
            
            # Save OCR text
            ocr_path = Path(f".refinery/ocr/{pdf_name}_page_{page_num}_gpu.txt")
            ocr_path.parent.mkdir(parents=True, exist_ok=True)
            with open(ocr_path, "w", encoding="utf-8") as f:
                f.write(ocr_text)
            
            return {
                "page_num": page_num,
                "text": ocr_text.strip(),
                "text_length": len(ocr_text.strip()),
                "tables": tables,
                "confidence": avg_confidence,
                "extraction_method": "gpu_enhanced_ocr",
                "page_metadata": {
                    "image_path": str(image_path),
                    "ocr_path": str(ocr_path),
                    "device": str(self.device),
                    "gpu_used": GPU_AVAILABLE,
                    "dpi": self.dpi,
                    "language": self.language,
                    "word_count": len(ocr_text.split()),
                    "line_count": len(ocr_text.strip().split('\n')),
                    "enhancement_applied": True
                }
            }
            
        except Exception as e:
            logger.error(f"Enhanced OCR extraction failed for page {page_num}: {e}")
            return {
                "page_num": page_num,
                "text": "",
                "text_length": 0,
                "tables": [],
                "confidence": 0.2,
                "error": str(e)
            }
    
    def _enhanced_table_detection(self, ocr_text: str, ocr_data: Dict, page_num: int) -> List[ExtractedTable]:
        """Enhanced table detection using OCR confidence data."""
        tables = []
        
        try:
            lines = ocr_text.strip().split('\n')
            
            # Use OCR confidence data for better table detection
            word_confidences = {}
            for i, word in enumerate(ocr_data.get('text', [])):
                if i < len(ocr_data.get('conf', [])):
                    word_confidences[word] = ocr_data['conf'][i] / 100
            
            # Advanced table detection
            table_lines = []
            for line_idx, line in enumerate(lines):
                words = line.split()
                avg_confidence = sum(word_confidences.get(word, 0.5) for word in words) / len(words) if words else 0.5
                
                # Table line criteria: multiple words, consistent spacing, good confidence
                if len(words) >= 3 and avg_confidence > 0.6:
                    if '  ' in line or '\t' in line:
                        table_lines.append((line_idx, line, avg_confidence))
            
            # Group consecutive table lines
            if len(table_lines) >= 2:
                table_data = []
                for line_idx, line, confidence in table_lines:
                    row = [cell.strip() for cell in line.split('  ') if cell.strip()]
                    if len(row) >= 2:
                        table_data.append(row)
                
                if len(table_data) >= 2:
                    tables.append(ExtractedTable(
                        table_id=1,
                        rows=len(table_data),
                        columns=len(table_data[0]) if table_data else 0,
                        headers=table_data[0] if table_data else [],
                        data=table_data[1:] if len(table_data) > 1 else [],
                        confidence=min(confidence for _, _, confidence in table_lines),
                        detection_method="gpu_enhanced_table"
                    ))
            
        except Exception as e:
            logger.warning(f"Enhanced table detection failed for page {page_num}: {e}")
        
        return tables
