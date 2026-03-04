"""
PageIndex Pydantic model for document page indexing and navigation.
"""

from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple
from enum import Enum

from pydantic import BaseModel, Field


class PageType(str, Enum):
    """Page type classification."""
    COVER = "cover"
    TITLE = "title"
    TOC = "table_of_contents"
    CONTENT = "content"
    CHAPTER_START = "chapter_start"
    SECTION_START = "section_start"
    APPENDIX = "appendix"
    BIBLIOGRAPHY = "bibliography"
    INDEX = "index"
    GLOSSARY = "glossary"
    BACK_COVER = "back_cover"
    BLANK = "blank"
    MIXED = "mixed"


class ContentType(str, Enum):
    """Content type on page."""
    TEXT_ONLY = "text_only"
    TABLE_HEAVY = "table_heavy"
    FIGURE_HEAVY = "figure_heavy"
    MIXED_CONTENT = "mixed_content"
    FORM = "form"
    CHART = "chart"
    DIAGRAM = "diagram"


class PageFeatures(BaseModel):
    """Extracted features from a page."""
    word_count: int
    line_count: int
    paragraph_count: int
    table_count: int
    figure_count: int
    image_count: int
    font_count: int
    column_count: int
    has_watermark: bool = False
    has_signature: bool = False
    has_page_number: bool = False
    text_density: float = Field(ge=0.0, le=1.0)
    image_coverage: float = Field(ge=0.0, le=1.0)


class PageNavigation(BaseModel):
    """Navigation information for a page."""
    page_number: Optional[int] = None
    section_number: Optional[str] = None
    chapter_number: Optional[str] = None
    title: Optional[str] = None
    subtitle: Optional[str] = None
    outline_level: Optional[int] = None
    is_section_start: bool = False
    is_chapter_start: bool = False
    previous_page_id: Optional[str] = None
    next_page_id: Optional[str] = None


class PageQuality(BaseModel):
    """Quality metrics for a page."""
    ocr_confidence: float = Field(ge=0.0, le=1.0)
    text_clarity: float = Field(ge=0.0, le=1.0)
    layout_quality: float = Field(ge=0.0, le=1.0)
    scan_quality: Optional[float] = Field(None, ge=0.0, le=1.0)
    noise_level: float = Field(ge=0.0, le=1.0)
    skew_angle: Optional[float] = None
    brightness: Optional[float] = Field(None, ge=0.0, le=1.0)
    contrast: Optional[float] = Field(None, ge=0.0, le=1.0)


class PageIndex(BaseModel):
    """Page index entry for document navigation and analysis."""
    
    # Basic identification
    page_id: str
    document_id: str
    page_num: int
    
    # Classification
    page_type: PageType
    content_type: ContentType
    primary_language: str
    language_confidence: float = Field(ge=0.0, le=1.0)
    
    # Content summary
    title: Optional[str] = None
    summary: Optional[str] = None
    keywords: List[str] = Field(default_factory=list)
    
    # Features and metrics
    features: PageFeatures
    quality: PageQuality
    
    # Navigation
    navigation: PageNavigation
    
    # File references
    image_path: Optional[str] = None
    ocr_text_path: Optional[str] = None
    ocr_detailed_path: Optional[str] = None
    
    # Processing metadata
    processed_at: datetime = Field(default_factory=datetime.now)
    extraction_method: str
    processing_version: str = "1.0"
    
    # Relationships
    linked_pages: List[str] = Field(default_factory=list)
    referenced_pages: List[str] = Field(default_factory=list)
    
    class Config:
        use_enum_values = True
        
    def is_content_page(self) -> bool:
        """Check if this is a main content page."""
        return self.page_type in [
            PageType.CONTENT, 
            PageType.CHAPTER_START, 
            PageType.SECTION_START
        ]
    
    def is_navigation_page(self) -> bool:
        """Check if this is a navigation page."""
        return self.page_type in [
            PageType.TOC,
            PageType.INDEX,
            PageType.GLOSSARY
        ]
    
    def is_front_matter(self) -> bool:
        """Check if this is front matter."""
        return self.page_type in [
            PageType.COVER,
            PageType.TITLE,
            PageType.TOC
        ]
    
    def is_back_matter(self) -> bool:
        """Check if this is back matter."""
        return self.page_type in [
            PageType.APPENDIX,
            PageType.BIBLIOGRAPHY,
            PageType.INDEX,
            PageType.GLOSSARY,
            PageType.BACK_COVER
        ]
    
    def has_tables(self) -> bool:
        """Check if page contains tables."""
        return self.features.table_count > 0
    
    def has_figures(self) -> bool:
        """Check if page contains figures."""
        return self.features.figure_count > 0
    
    def is_high_quality(self) -> bool:
        """Check if page has high quality metrics."""
        return (
            self.quality.ocr_confidence > 0.8 and
            self.quality.text_clarity > 0.8 and
            self.quality.layout_quality > 0.8
        )
    
    def get_text_density_score(self) -> float:
        """Get text density score (0-1)."""
        return self.features.text_density
    
    def get_complexity_score(self) -> float:
        """Get page complexity score based on features."""
        # Simple complexity calculation
        complexity = 0.0
        complexity += min(self.features.table_count * 0.2, 0.4)
        complexity += min(self.features.figure_count * 0.15, 0.3)
        complexity += min(self.features.column_count * 0.1, 0.2)
        complexity += (1.0 - self.quality.ocr_confidence) * 0.1
        return min(complexity, 1.0)
    
    def to_summary(self) -> Dict[str, Any]:
        """Get page summary for navigation."""
        return {
            "page_id": self.page_id,
            "page_num": self.page_num,
            "page_type": self.page_type.value,
            "title": self.title,
            "content_type": self.content_type.value,
            "has_tables": self.has_tables(),
            "has_figures": self.has_figures(),
            "quality_score": self.quality.ocr_confidence,
            "complexity_score": self.get_complexity_score(),
            "is_content": self.is_content_page(),
            "summary": self.summary
        }
