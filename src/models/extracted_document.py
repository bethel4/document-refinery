"""
ExtractedDocument Pydantic model for extraction results.
"""

from datetime import datetime
from typing import List, Dict, Any, Optional
from enum import Enum

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .ldu import BoundingBox


class ExtractionMethod(str, Enum):
    """Extraction method used."""
    FAST_TEXT = "fast_text"
    LAYOUT = "layout"
    VISION = "vision"
    HYBRID = "hybrid"
    MOCK = "mock"


class ExtractionCost(str, Enum):
    """Estimated extraction cost band."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


class TableData(BaseModel):
    """Table extraction data."""
    table_id: str
    page_num: int = Field(ge=1)
    rows: int = Field(ge=0)
    columns: int = Field(ge=0)
    headers: List[str]
    data: List[List[str]]
    confidence: float = Field(ge=0.0, le=1.0)
    bounding_box: Optional[BoundingBox] = None
    # Backward-compatible alias used by existing payloads.
    bbox: Optional[BoundingBox] = None

    @model_validator(mode="after")
    def normalize_bbox(self) -> "TableData":
        if self.bbox and not self.bounding_box:
            self.bounding_box = self.bbox
        elif self.bounding_box and not self.bbox:
            self.bbox = self.bounding_box
        return self


class PageExtraction(BaseModel):
    """Page-level extraction results."""
    page_num: int = Field(ge=1)
    text: str
    text_length: int = Field(ge=0)
    tables: List[TableData] = Field(default_factory=list)
    confidence: float = Field(ge=0.0, le=1.0)
    extraction_method: ExtractionMethod
    escalated_from: Optional[ExtractionMethod] = None
    strategy_used: ExtractionMethod
    page_metadata: Optional[Dict[str, Any]] = None

    @model_validator(mode="after")
    def normalize_text_length(self) -> "PageExtraction":
        if self.text_length != len(self.text):
            self.text_length = len(self.text)
        return self


class ExtractionMetadata(BaseModel):
    """Extraction process metadata."""
    total_pages: int = Field(ge=0)
    total_text_length: int = Field(ge=0)
    total_tables: int = Field(ge=0)
    average_confidence: float = Field(ge=0.0, le=1.0)
    extraction_cost: ExtractionCost
    processing_time_seconds: float = Field(ge=0.0)
    ocr_engine: Optional[str] = None
    dpi: Optional[int] = Field(default=None, gt=0)
    language: Optional[str] = None
    docling_version: Optional[str] = None
    pages_processed: int = Field(ge=0)
    confidence_threshold: float = Field(ge=0.0, le=1.0)
    performance_limits: Optional[Dict[str, Any]] = None


class RoutingMetadata(BaseModel):
    """Strategy routing metadata."""
    recommended_strategy: Optional[ExtractionMethod] = None
    actual_strategy: Optional[ExtractionMethod] = None
    escalated: bool = False
    confidence_threshold: float = Field(default=0.0, ge=0.0, le=1.0)
    average_confidence: float = Field(ge=0.0, le=1.0)
    pages_processed: int = Field(default=0, ge=0)


class PerformanceMetrics(BaseModel):
    """Performance metrics for the extraction."""
    triage_speed: str
    extraction_speed: str
    overall_efficiency: str


class TriageSnapshot(BaseModel):
    """Normalized triage output stored with extraction results."""
    duration: Optional[float] = Field(default=None, ge=0.0)
    profile: Dict[str, Any] = Field(default_factory=dict)


class NormalizedExtractionOutput(BaseModel):
    """Normalized extraction payload with typed nested models."""
    duration: Optional[float] = Field(default=None, ge=0.0)
    strategy_used: ExtractionMethod
    routing_metadata: RoutingMetadata
    pages: List[PageExtraction] = Field(default_factory=list)
    extraction_metadata: Optional[ExtractionMetadata] = None
    document_elements: Dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def normalize_summary_counts(self) -> "NormalizedExtractionOutput":
        if self.routing_metadata.recommended_strategy is None:
            self.routing_metadata.recommended_strategy = self.strategy_used
        if self.routing_metadata.actual_strategy is None:
            self.routing_metadata.actual_strategy = self.strategy_used
        if self.extraction_metadata:
            self.extraction_metadata.total_pages = len(self.pages)
            self.extraction_metadata.pages_processed = len(self.pages)
        elif self.routing_metadata.pages_processed == 0:
            self.routing_metadata.pages_processed = len(self.pages)
        return self


class ExtractedDocument(BaseModel):
    """Complete extracted document with all metadata."""
    
    # Document identification
    document_id: str
    filename: str
    file_path: str
    
    # Timing information
    total_duration: float = Field(ge=0.0)
    
    # Triage output (normalized)
    triage: TriageSnapshot
    
    # Extraction output (normalized)
    extraction: NormalizedExtractionOutput
    
    # Performance metrics
    performance: PerformanceMetrics
    
    # Processing timestamps
    processed_at: datetime = Field(default_factory=datetime.now)
    
    model_config = ConfigDict(use_enum_values=True)
        
    def get_pages(self) -> List[PageExtraction]:
        """Get extracted pages as PageExtraction objects."""
        return self.extraction.pages
    
    def get_average_confidence(self) -> float:
        """Get average confidence across all pages."""
        return self.extraction.routing_metadata.average_confidence
    
    def get_strategy_used(self) -> str:
        """Get the extraction strategy used."""
        return self.extraction.strategy_used.value
    
    def get_total_text_length(self) -> int:
        """Get total extracted text length."""
        if self.extraction.extraction_metadata:
            return self.extraction.extraction_metadata.total_text_length
        return sum(page.text_length for page in self.extraction.pages)
    
    def get_page_count(self) -> int:
        """Get number of pages processed."""
        return self.extraction.routing_metadata.pages_processed
    
    def was_escalated(self) -> bool:
        """Check if extraction was escalated."""
        return self.extraction.routing_metadata.escalated
    
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
            "origin_type": self.triage.profile.get("origin_type", "unknown"),
            "category": self.triage.profile.get("category", "unknown")
        }
