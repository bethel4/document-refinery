"""
Document Models - Pydantic schemas for document refinery.
"""

from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import datetime
from enum import Enum

class OriginType(str, Enum):
    NATIVE_DIGITAL = "native_digital"
    SCANNED_IMAGE = "scanned_image"
    MIXED = "mixed"
    UNKNOWN = "unknown"

class LayoutComplexity(str, Enum):
    SINGLE_COLUMN = "single_column"
    MULTI_COLUMN = "multi_column"
    TABLE_HEAVY = "table_heavy"
    FIGURE_HEAVY = "figure_heavy"
    MIXED = "mixed"

class ExtractionStrategy(str, Enum):
    FAST_TEXT = "fast_text"
    LAYOUT = "layout"
    VISION = "vision"
    HYBRID = "hybrid"

class DocumentProfile(BaseModel):
    """Complete document profile from triage phase."""
    
    # Basic metadata
    document_id: str
    filename: str
    file_path: str
    file_size_bytes: int
    analyzed_at: datetime = Field(default_factory=datetime.now)
    
    # Classification results
    origin_type: OriginType
    layout_complexity: LayoutComplexity
    language: str
    language_confidence: float = Field(ge=0.0, le=1.0)
    
    domain_hint: str
    domain_confidence: float = Field(ge=0.0, le=1.0)
    
    # Category assignment from YAML
    category: str
    category_confidence: float = Field(ge=0.0, le=1.0)
    
    # Processing recommendations
    recommended_strategy: ExtractionStrategy
    estimated_extraction_cost: str
    
    # Document metrics
    total_pages: int
    total_chars: int
    avg_chars_per_page: float
    image_area_ratio: float
    detected_table_count: int
    x_cluster_count: int
    
    # Mixed document metrics
    scanned_page_ratio: Optional[float] = None
    digital_page_ratio: Optional[float] = None
    
    # Quality scores
    text_quality_score: float = Field(ge=0.0, le=1.0)
    structure_quality_score: float = Field(ge=0.0, le=1.0)
    overall_quality_score: float = Field(ge=0.0, le=1.0)
    
    # Additional properties
    detected_fonts: List[str] = Field(default_factory=list)
    has_watermarks: bool = False
    has_signatures: bool = False
    is_searchable: bool = True

class PageIndex(BaseModel):
    """Page-level index and metadata."""
    
    page_num: int
    text_length: int
    confidence: float
    has_tables: bool
    table_count: int
    extraction_method: str
    word_count: Optional[int] = None
    line_count: Optional[int] = None

class ExtractedTable(BaseModel):
    """Structured table extraction result."""
    
    table_id: int
    rows: int
    columns: int
    headers: List[str]
    data: List[List[str]]
    confidence: float
    detection_method: str

class ExtractedPage(BaseModel):
    """Complete page extraction result."""
    
    page_num: int
    text: str
    text_length: int
    confidence: float
    tables: List[ExtractedTable] = Field(default_factory=list)
    extraction_method: str
    page_metadata: Dict[str, Any] = Field(default_factory=dict)

class ExtractedDocument(BaseModel):
    """Complete document extraction result."""
    
    document_id: str
    strategy_used: ExtractionStrategy
    pages: List[ExtractedPage]
    extraction_metadata: Dict[str, Any]
    routing_metadata: Optional[Dict[str, Any]] = None
    
    # Performance metrics
    total_text_length: int = Field(default=0)
    total_tables: int = Field(default=0)
    average_confidence: float = Field(default=0.0)
    processing_time_seconds: float = Field(default=0.0)

class ProvenanceChain(BaseModel):
    """Processing provenance tracking."""
    
    document_id: str
    processing_stages: List[Dict[str, Any]] = Field(default_factory=list)
    strategy_decisions: List[Dict[str, Any]] = Field(default_factory=list)
    escalations: List[Dict[str, Any]] = Field(default_factory=list)
    quality_checks: List[Dict[str, Any]] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)

class LDU(BaseModel):
    """Logical Document Unit - semantic chunking result."""
    
    ldu_id: str
    document_id: str
    page_range: str  # "1-5", "6-10", etc.
    content_type: str  # "table", "text", "mixed"
    content: str
    metadata: Dict[str, Any] = Field(default_factory=dict)
    confidence: float = Field(ge=0.0, le=1.0)
    created_at: datetime = Field(default_factory=datetime.now)

class ExtractionLedgerEntry(BaseModel):
    """Ledger entry for extraction tracking."""
    
    timestamp: str
    document_id: str
    filename: str
    origin_type: OriginType
    category: str
    recommended_strategy: ExtractionStrategy
    actual_strategy: ExtractionStrategy
    escalated: bool
    confidence: float
    pages_processed: int
    total_duration: float
    extraction_cost: str
    status: str  # "success", "failed", "partial"
    error_message: Optional[str] = None
