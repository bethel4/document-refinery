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

from typing import Any

# NOTE: Keep this package import lightweight.
# Some optional extractors (e.g. Docling-based layout) can be expensive to import.

__all__ = ["FastTextExtractor", "LayoutExtractor", "VisionExtractor", "ExtractionRouter", "ExtractionPipeline"]


def __getattr__(name: str) -> Any:
    if name == "FastTextExtractor":
        from .fast_text_extractor import FastTextExtractor

        return FastTextExtractor
    if name == "LayoutExtractor":
        from .layout_extractor import LayoutExtractor

        return LayoutExtractor
    if name == "VisionExtractor":
        from .vision_extractor import VisionExtractor

        return VisionExtractor
    if name == "ExtractionRouter":
        from .extraction_router import ExtractionRouter

        return ExtractionRouter
    if name == "ExtractionPipeline":
        from .pipeline_runner import ExtractionPipeline

        return ExtractionPipeline

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
