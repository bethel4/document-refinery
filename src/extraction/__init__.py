"""
Document Extraction Module.

Provides three extraction strategies with confidence-gated routing:
- FastTextExtractor: For simple digital documents
- LayoutExtractor: For structured documents  
- VisionExtractor: For scanned documents

Usage:
    from src.extraction.pipeline_runner import ExtractionPipeline
    
    pipeline = ExtractionPipeline(max_workers=4)
    results = pipeline.process_batch("data/raw")
"""

from .fast_text_extractor import FastTextExtractor
from .layout_extractor import LayoutExtractor
from .vision_extractor import VisionExtractor
from .extraction_router import ExtractionRouter
from .pipeline_runner import ExtractionPipeline

__all__ = [
    'FastTextExtractor',
    'LayoutExtractor', 
    'VisionExtractor',
    'ExtractionRouter',
    'ExtractionPipeline'
]
