"""
LDU (Logical Document Unit) Pydantic model for document chunking.
"""

from datetime import datetime
from typing import List, Dict, Any, Optional
from enum import Enum

from pydantic import BaseModel, Field


class LDUType(str, Enum):
    """Logical Document Unit types."""
    SECTION = "section"
    CHAPTER = "chapter"
    PARAGRAPH = "paragraph"
    TABLE = "table"
    FIGURE = "figure"
    FOOTNOTE = "footnote"
    HEADER = "header"
    FOOTER = "footer"
    PAGE = "page"
    DOCUMENT = "document"


class LDURole(str, Enum):
    """Logical Document Unit roles."""
    TITLE = "title"
    CONTENT = "content"
    METADATA = "metadata"
    NAVIGATION = "navigation"
    REFERENCE = "reference"
    APPENDIX = "appendix"
    GLOSSARY = "glossary"
    INDEX = "index"


class BoundingBox(BaseModel):
    """Bounding box for spatial information."""
    left: float
    top: float
    width: float
    height: float
    page_num: int
    
    def get_area(self) -> float:
        """Calculate bounding box area."""
        return self.width * self.height
    
    def get_center(self) -> tuple[float, float]:
        """Get center coordinates."""
        return (self.left + self.width / 2, self.top + self.height / 2)


class SemanticMetadata(BaseModel):
    """Semantic metadata for LDU."""
    topic: Optional[str] = None
    keywords: List[str] = Field(default_factory=list)
    entities: List[Dict[str, Any]] = Field(default_factory=list)
    sentiment: Optional[float] = Field(None, ge=-1.0, le=1.0)
    language: Optional[str] = None
    readability_score: Optional[float] = Field(None, ge=0.0, le=100.0)


class StructuralMetadata(BaseModel):
    """Structural metadata for LDU."""
    level: Optional[int] = None  # Header level, list level, etc.
    parent_id: Optional[str] = None
    child_ids: List[str] = Field(default_factory=list)
    sibling_ids: List[str] = Field(default_factory=list)
    section_number: Optional[str] = None
    outline_path: Optional[str] = None


class LDU(BaseModel):
    """Logical Document Unit - a semantic chunk of a document."""
    
    # Basic identification
    ldu_id: str
    document_id: str
    ldu_type: LDUType
    role: LDURole
    
    # Content
    text: str
    text_length: int
    confidence: float = Field(ge=0.0, le=1.0)
    
    # Position and structure
    page_num: int
    bbox: Optional[BoundingBox] = None
    position_in_document: int  # Sequential position
    structural_metadata: StructuralMetadata = Field(default_factory=StructuralMetadata)
    
    # Semantic information
    semantic_metadata: SemanticMetadata = Field(default_factory=SemanticMetadata)
    
    # Processing metadata
    extraction_method: str
    created_at: datetime = Field(default_factory=datetime.now)
    processing_version: str = "1.0"
    
    # Relationships
    related_ldus: List[str] = Field(default_factory=list)
    references: List[str] = Field(default_factory=list)
    
    class Config:
        use_enum_values = True
        
    def get_text_preview(self, max_length: int = 100) -> str:
        """Get a preview of the LDU text."""
        if len(self.text) <= max_length:
            return self.text
        return self.text[:max_length] + "..."
    
    def is_content_ldu(self) -> bool:
        """Check if this LDU contains main content."""
        return self.role in [LDURole.CONTENT, LDURole.TITLE]
    
    def is_metadata_ldu(self) -> bool:
        """Check if this LDU contains metadata."""
        return self.role in [LDURole.METADATA, LDURole.NAVIGATION, LDURole.REFERENCE]
    
    def has_spatial_info(self) -> bool:
        """Check if LDU has spatial/bounding box information."""
        return self.bbox is not None
    
    def get_hierarchy_level(self) -> int:
        """Get the hierarchy level of this LDU."""
        return self.structural_metadata.level or 0
    
    def add_child(self, child_id: str):
        """Add a child LDU ID."""
        if child_id not in self.structural_metadata.child_ids:
            self.structural_metadata.child_ids.append(child_id)
    
    def add_sibling(self, sibling_id: str):
        """Add a sibling LDU ID."""
        if sibling_id not in self.structural_metadata.sibling_ids:
            self.structural_metadata.sibling_ids.append(sibling_id)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "ldu_id": self.ldu_id,
            "document_id": self.document_id,
            "ldu_type": self.ldu_type.value,
            "role": self.role.value,
            "text": self.text,
            "text_length": self.text_length,
            "confidence": self.confidence,
            "page_num": self.page_num,
            "position": self.position_in_document,
            "extraction_method": self.extraction_method,
            "created_at": self.created_at.isoformat(),
            "bbox": self.bbox.dict() if self.bbox else None,
            "preview": self.get_text_preview()
        }
