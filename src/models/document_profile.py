"""
DocumentProfile Pydantic model for document classification results.
"""

from datetime import datetime
from typing import List, Optional
from enum import Enum

from pydantic import BaseModel, Field


class OriginType(str, Enum):
    """Document origin type classification."""
    NATIVE_DIGITAL = "native_digital"
    SCANNED_IMAGE = "scanned_image"
    MIXED = "mixed"
    UNKNOWN = "unknown"


class LayoutComplexity(str, Enum):
    """Document layout complexity classification."""
    SINGLE_COLUMN = "single_column"
    MULTI_COLUMN = "multi_column"
    TABLE_HEAVY = "table_heavy"
    FIGURE_HEAVY = "figure_heavy"
    MIXED = "mixed"


class DocumentCategory(str, Enum):
    """Document processing category."""
    HIGH_COMPLEXITY = "high_complexity"
    MODERATE_COMPLEXITY = "moderate_complexity"
    SIMPLE_TEXT = "simple_text"


class ExtractionStrategy(str, Enum):
    """Recommended extraction strategy."""
    FAST_TEXT = "fast_text"
    LAYOUT = "layout"
    VISION = "vision"
    HYBRID = "hybrid"


class DocumentProfile(BaseModel):
    """Pydantic model for document classification results."""
    
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
    
    # Domain classification
    domain_hint: str
    domain_confidence: float = Field(ge=0.0, le=1.0)
    
    # Category assignment from YAML
    category: DocumentCategory
    category_confidence: float = Field(ge=0.0, le=1.0)
    
    # Processing recommendations
    recommended_strategy: ExtractionStrategy
    estimated_extraction_cost: str  # fast_text, needs_layout_model, needs_vision_model
    
    # Document metrics (computed)
    total_pages: int
    total_chars: int
    avg_chars_per_page: float
    image_area_ratio: float
    detected_table_count: int
    x_cluster_count: int
    
    # Mixed document metrics (optional)
    scanned_page_ratio: Optional[float] = Field(None, ge=0.0, le=1.0)
    digital_page_ratio: Optional[float] = Field(None, ge=0.0, le=1.0)
    
    # Quality scores
    text_quality_score: float = Field(ge=0.0, le=1.0)
    structure_quality_score: float = Field(ge=0.0, le=1.0)
    overall_quality_score: float = Field(ge=0.0, le=1.0)
    
    # Additional document properties
    detected_fonts: List[str] = Field(default_factory=list)
    has_watermarks: bool = False
    has_signatures: bool = False
    is_searchable: bool = True
    
    # Processing metadata
    confidence: float = Field(ge=0.0, le=1.0)
    pages: int
    
    class Config:
        use_enum_values = True
        
    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "document_id": self.document_id,
            "origin_type": self.origin_type.value if hasattr(self.origin_type, 'value') else self.origin_type,
            "category": self.category.value if hasattr(self.category, 'value') else self.category,
            "recommended_strategy": self.recommended_strategy.value if hasattr(self.recommended_strategy, 'value') else self.recommended_strategy,
            "confidence": self.confidence,
            "pages": self.pages,
            "file_size": self.file_size_bytes,
            "scanned_page_ratio": self.scanned_page_ratio,
            "digital_page_ratio": self.digital_page_ratio,
            "avg_chars_per_page": self.avg_chars_per_page,
            "image_area_ratio": self.image_area_ratio,
            "is_searchable": self.is_searchable
        }
