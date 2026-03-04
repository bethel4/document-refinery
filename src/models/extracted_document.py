"""
ExtractedDocument Pydantic model for extraction results.
"""

from datetime import datetime
from typing import List, Dict, Any, Optional
from enum import Enum

from pydantic import BaseModel, Field


class ExtractionMethod(str, Enum):
    """Extraction method used."""
    FAST_TEXT = "fast_text"
    LAYOUT = "layout"
    VISION = "vision"
    HYBRID = "hybrid"
    MOCK = "mock"


class TableData(BaseModel):
    """Table extraction data."""
    table_id: str
    page_num: int
    rows: int
    columns: int
    headers: List[str]
    data: List[List[str]]
    confidence: float = Field(ge=0.0, le=1.0)
    bbox: Optional[Dict[str, int]] = None


class PageExtraction(BaseModel):
    """Page-level extraction results."""
    page_num: int
    text: str
    text_length: int
    tables: List[TableData] = Field(default_factory=list)
    confidence: float = Field(ge=0.0, le=1.0)
    extraction_method: ExtractionMethod
    escalated_from: Optional[ExtractionMethod] = None
    strategy_used: str
    page_metadata: Optional[Dict[str, Any]] = None


class ExtractionMetadata(BaseModel):
    """Extraction process metadata."""
    total_pages: int
    total_text_length: int
    total_tables: int
    average_confidence: float = Field(ge=0.0, le=1.0)
    extraction_cost: str
    processing_time_seconds: float
    ocr_engine: Optional[str] = None
    dpi: Optional[int] = None
    language: Optional[str] = None
    docling_version: Optional[str] = None
    pages_processed: int
    confidence_threshold: float
    performance_limits: Optional[Dict[str, Any]] = None


class RoutingMetadata(BaseModel):
    """Strategy routing metadata."""
    recommended_strategy: str
    actual_strategy: str
    escalated: bool
    confidence_threshold: float
    average_confidence: float
    pages_processed: int


class PerformanceMetrics(BaseModel):
    """Performance metrics for the extraction."""
    triage_speed: str
    extraction_speed: str
    overall_efficiency: str


class ExtractedDocument(BaseModel):
    """Complete extracted document with all metadata."""
    
    # Document identification
    document_id: str
    filename: str
    file_path: str
    
    # Timing information
    total_duration: float
    
    # Triage results
    triage: Dict[str, Any]
    
    # Extraction results
    extraction: Dict[str, Any]
    
    # Performance metrics
    performance: PerformanceMetrics
    
    # Processing timestamps
    processed_at: datetime = Field(default_factory=datetime.now)
    
    class Config:
        use_enum_values = True
        
    def get_pages(self) -> List[PageExtraction]:
        """Get extracted pages as PageExtraction objects."""
        pages_data = self.extraction.get("pages", [])
        return [PageExtraction(**page) for page in pages_data]
    
    def get_average_confidence(self) -> float:
        """Get average confidence across all pages."""
        routing_meta = self.extraction.get("routing_metadata", {})
        return routing_meta.get("average_confidence", 0.0)
    
    def get_strategy_used(self) -> str:
        """Get the extraction strategy used."""
        return self.extraction.get("strategy_used", "unknown")
    
    def get_total_text_length(self) -> int:
        """Get total extracted text length."""
        extraction_meta = self.extraction.get("extraction_metadata", {})
        return extraction_meta.get("total_text_length", 0)
    
    def get_page_count(self) -> int:
        """Get number of pages processed."""
        routing_meta = self.extraction.get("routing_metadata", {})
        return routing_meta.get("pages_processed", 0)
    
    def was_escalated(self) -> bool:
        """Check if extraction was escalated."""
        routing_meta = self.extraction.get("routing_metadata", {})
        return routing_meta.get("escalated", False)
    
    def to_summary_dict(self) -> Dict[str, Any]:
        """Get summary of extraction results."""
        return {
            "document_id": self.document_id,
            "filename": self.filename,
            "strategy_used": self.get_strategy_used(),
            "confidence": self.get_average_confidence(),
            "pages_processed": self.get_page_count(),
            "total_text_length": self.get_total_text_length(),
            "was_escalated": self.was_escalated(),
            "processing_time": self.total_duration,
            "origin_type": self.triage.get("profile", {}).get("origin_type", "unknown"),
            "category": self.triage.get("profile", {}).get("category", "unknown")
        }
