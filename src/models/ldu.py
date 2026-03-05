"""
LDU (Logical Document Unit) Pydantic model for document chunking.
"""

from datetime import datetime
from typing import List, Dict, Any, Optional
from enum import Enum

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, field_validator, model_validator


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
    x: float = Field(validation_alias=AliasChoices("x", "left"))
    y: float = Field(validation_alias=AliasChoices("y", "top"))
    width: float = Field(gt=0.0)
    height: float = Field(gt=0.0)
    page_num: int = Field(ge=1)
    page_width: Optional[float] = Field(default=None, gt=0.0)
    page_height: Optional[float] = Field(default=None, gt=0.0)

    model_config = ConfigDict(populate_by_name=True)

    @model_validator(mode="after")
    def validate_within_page(self) -> "BoundingBox":
        """Enforce geometric constraints when page bounds are provided."""
        if self.page_width is not None and (self.x + self.width) > self.page_width:
            raise ValueError("Bounding box exceeds page width")
        if self.page_height is not None and (self.y + self.height) > self.page_height:
            raise ValueError("Bounding box exceeds page height")
        return self
    
    def get_area(self) -> float:
        """Calculate bounding box area."""
        return self.width * self.height
    
    def get_center(self) -> tuple[float, float]:
        """Get center coordinates."""
        return (self.x + self.width / 2, self.y + self.height / 2)


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


class ChunkRelationType(str, Enum):
    """Supported LDU-to-LDU chunk relationships."""
    PARENT = "parent"
    CHILD = "child"
    PREVIOUS = "previous"
    NEXT = "next"
    RELATED = "related"
    SOURCE = "source"
    DERIVED = "derived"


class ChunkRelationship(BaseModel):
    """Explicit typed relationship between LDUs/chunks."""
    relation_type: ChunkRelationType
    target_ldu_id: str = Field(min_length=1)
    confidence: Optional[float] = Field(default=None, ge=0.0, le=1.0)


class LDU(BaseModel):
    """Logical Document Unit - a semantic chunk of a document."""
    
    # Basic identification
    ldu_id: str
    document_id: str
    ldu_type: LDUType
    role: LDURole
    
    # Content
    text: str = Field(min_length=1)
    text_length: int = Field(ge=0)
    content_hash: str = Field(min_length=16)
    confidence: float = Field(ge=0.0, le=1.0)
    
    # Position and structure
    page_num: int = Field(ge=1)
    page_refs: List[int] = Field(default_factory=list)
    bounding_box: Optional[BoundingBox] = None
    # Backward-compatible alias for older payloads.
    bbox: Optional[BoundingBox] = None
    position_in_document: int = Field(ge=0)  # Sequential position
    parent_section: Optional[str] = None
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
    chunk_relationships: List[ChunkRelationship] = Field(default_factory=list)
    
    model_config = ConfigDict(use_enum_values=True)

    @field_validator("text")
    @classmethod
    def validate_text_non_empty(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("text must not be blank")
        return value

    @field_validator("page_refs")
    @classmethod
    def validate_page_refs(cls, value: List[int]) -> List[int]:
        if any(page <= 0 for page in value):
            raise ValueError("page_refs must contain only positive page numbers")
        return sorted(set(value))

    @model_validator(mode="after")
    def normalize_fields(self) -> "LDU":
        """Keep legacy and canonical fields in sync and enforce invariants."""
        if self.bbox and not self.bounding_box:
            self.bounding_box = self.bbox
        elif self.bounding_box and not self.bbox:
            self.bbox = self.bounding_box

        if not self.page_refs:
            self.page_refs = [self.page_num]
        elif self.page_num not in self.page_refs:
            self.page_refs = sorted({*self.page_refs, self.page_num})

        if self.text_length != len(self.text):
            self.text_length = len(self.text)
        return self
        
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
        return self.bounding_box is not None or self.bbox is not None
    
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
            "ldu_type": self.ldu_type.value if hasattr(self.ldu_type, "value") else self.ldu_type,
            "role": self.role.value if hasattr(self.role, "value") else self.role,
            "text": self.text,
            "text_length": self.text_length,
            "confidence": self.confidence,
            "page_num": self.page_num,
            "page_refs": self.page_refs,
            "position": self.position_in_document,
            "parent_section": self.parent_section,
            "extraction_method": self.extraction_method,
            "created_at": self.created_at.isoformat(),
            "bounding_box": self.bounding_box.model_dump() if self.bounding_box else None,
            "bbox": self.bbox.model_dump() if self.bbox else None,
            "content_hash": self.content_hash,
            "preview": self.get_text_preview()
        }
