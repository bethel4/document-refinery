"""
Document Triage Engine
Classifies documents based on calibrated thresholds and lightweight analysis.
"""

import json
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime
from enum import Enum

import yaml
from pydantic import BaseModel, Field

class OriginType(str, Enum):
    SCANNED = "scanned"
    DIGITAL_NATIVE = "digital_native"
    CONVERTED = "converted"
    UNKNOWN = "unknown"

class LayoutComplexity(str, Enum):
    SIMPLE = "simple"
    MODERATE = "moderate"
    COMPLEX = "complex"
    VERY_COMPLEX = "very_complex"

class ProcessingPriority(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"

class DocumentProfile(BaseModel):
    """Pydantic model for document classification results."""
    
    # Basic metadata
    document_id: str
    filename: str
    file_path: str
    file_size_bytes: int
    analyzed_at: datetime = Field(default_factory=datetime.now)
    
    # Classification results
    origin_type: str  # native_digital, scanned_image, mixed
    layout_complexity: str  # single_column, multi_column, table_heavy, figure_heavy, mixed
    language: str
    language_confidence: float = Field(ge=0.0, le=1.0)
    
    domain_hint: str
    domain_confidence: float = Field(ge=0.0, le=1.0)
    
    # Category assignment from YAML
    category: str  # high_complexity, moderate_complexity, simple_text
    category_confidence: float = Field(ge=0.0, le=1.0)
    
    # Processing recommendations
    recommended_strategy: str
    estimated_extraction_cost: str  # fast_text, needs_layout_model, needs_vision_model
    
    # Document metrics (computed)
    total_pages: int
    total_chars: int
    avg_chars_per_page: float
    image_area_ratio: float
    detected_table_count: int
    x_cluster_count: int
    
    # Mixed document metrics (optional)
    scanned_page_ratio: Optional[float] = None
    digital_page_ratio: Optional[float] = None
    
    # Quality scores
    text_quality_score: float = Field(ge=0.0, le=1.0)
    structure_quality_score: float = Field(ge=0.0, le=1.0)
    overall_quality_score: float = Field(ge=0.0, le=1.0)
    
    # Additional document properties
    detected_fonts: List[str] = Field(default_factory=list)
    has_watermarks: bool = False
    has_signatures: bool = False
    is_searchable: bool = True
    
    class Config:
        use_enum_values = True

class TriageClassifier:
    """Document triage classifier using calibrated thresholds."""
    
    def __init__(self, rules_file: str, profiles_dir: str):
        self.rules_file = Path(rules_file)
        self.profiles_dir = Path(profiles_dir)
        self.rules = self._load_rules()
        
        # Ensure profiles directory exists
        self.profiles_dir.mkdir(parents=True, exist_ok=True)
        
        self.logger = logging.getLogger(__name__)
        
        # Domain keywords for classification
        self.domain_keywords = {
            'legal': ['contract', 'agreement', 'legal', 'law', 'court', 'judge', 'attorney'],
            'financial': ['invoice', 'payment', 'balance', 'account', 'bank', 'financial', 'tax'],
            'medical': ['patient', 'medical', 'diagnosis', 'treatment', 'prescription', 'hospital'],
            'technical': ['specification', 'manual', 'technical', 'engineering', 'diagram'],
            'business': ['report', 'meeting', 'proposal', 'business', 'company', 'corporate'],
            'academic': ['research', 'study', 'university', 'paper', 'journal', 'academic'],
            'government': ['government', 'official', 'permit', 'license', 'regulation'],
            'personal': ['letter', 'personal', 'family', 'individual', 'private']
        }
    
    def _load_rules(self) -> Dict[str, Any]:
        """Load extraction rules from YAML file."""
        try:
            with open(self.rules_file, 'r') as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            self.logger.warning(f"Rules file not found: {self.rules_file}")
            return self._get_default_rules()
        except Exception as e:
            self.logger.error(f"Error loading rules: {e}")
            return self._get_default_rules()
    
    def _get_default_rules(self) -> Dict[str, Any]:
        """Get default rules if no rules file is available."""
        return {
            'document_categories': {
                'simple_text': {
                    'criteria': {
                        'avg_chars_per_page_max': 1000,
                        'image_area_ratio_max': 0.1,
                        'detected_table_count_max': 1,
                        'x_cluster_count_max': 2
                    },
                    'recommended_strategy': 'fast_text',
                    'confidence_threshold': 0.8
                },
                'moderate_complexity': {
                    'criteria': {
                        'avg_chars_per_page_min': 1000,
                        'avg_chars_per_page_max': 2000,
                        'image_area_ratio_max': 0.3,
                        'detected_table_count_max': 3,
                        'x_cluster_count_max': 3
                    },
                    'recommended_strategy': 'layout',
                    'confidence_threshold': 0.7
                },
                'high_complexity': {
                    'criteria': {
                        'avg_chars_per_page_min': 2000,
                        'image_area_ratio_min': 0.3,
                        'detected_table_count_min': 3,
                        'x_cluster_count_min': 3
                    },
                    'recommended_strategy': 'vision',
                    'confidence_threshold': 0.6
                }
            }
        }
    
    def classify_document(self, pdf_path: str) -> DocumentProfile:
        """Classify a single PDF document using calibrated YAML rules."""
        pdf_path = Path(pdf_path)
        
        # Generate document ID
        document_id = f"{pdf_path.stem}_{int(datetime.now().timestamp())}"
        
        # Extract lightweight metrics
        metrics = self._extract_lightweight_metrics(pdf_path)
        
        # Classify origin type
        origin_type = self._classify_origin_type(metrics)
        
        # Classify layout complexity
        layout_complexity = self._classify_layout_complexity(metrics)
        
        # Detect language
        language, language_confidence = self._detect_language(pdf_path)
        
        # Classify domain
        domain_hint, domain_confidence = self._classify_domain(pdf_path)
        
        # Assign category using YAML rules
        category, category_confidence, recommended_strategy = self._assign_category_from_rules(metrics)
        
        # Determine extraction cost level
        extraction_cost = self._determine_extraction_cost(recommended_strategy)
        
        # Calculate quality scores
        text_quality, structure_quality, overall_quality = self._calculate_quality_scores(metrics)
        
        # Create document profile
        profile = DocumentProfile(
            document_id=document_id,
            filename=pdf_path.name,
            file_path=str(pdf_path),
            file_size_bytes=metrics.get('file_size', 0),
            origin_type=origin_type,
            layout_complexity=layout_complexity,
            language=language,
            language_confidence=language_confidence,
            domain_hint=domain_hint,
            domain_confidence=domain_confidence,
            category=category,
            category_confidence=category_confidence,
            recommended_strategy=recommended_strategy,
            estimated_extraction_cost=extraction_cost,
            total_pages=metrics.get('total_pages', 0),
            total_chars=metrics.get('total_chars', 0),
            avg_chars_per_page=metrics.get('avg_chars_per_page', 0),
            image_area_ratio=metrics.get('image_area_ratio', 0),
            detected_table_count=metrics.get('table_count', 0),
            x_cluster_count=metrics.get('column_count', 1),
            scanned_page_ratio=metrics.get('scanned_page_ratio'),
            digital_page_ratio=metrics.get('digital_page_ratio'),
            text_quality_score=text_quality,
            structure_quality_score=structure_quality,
            overall_quality_score=overall_quality,
            detected_fonts=metrics.get('fonts', []),
            has_watermarks=metrics.get('has_watermarks', False),
            has_signatures=metrics.get('has_signatures', False),
            is_searchable=metrics.get('is_searchable', True)
        )
        
        # Save profile
        self._save_profile(profile)
        
        return profile
    
    def _extract_lightweight_metrics(self, pdf_path: Path) -> Dict[str, Any]:
        """Extract lightweight metrics for triage classification with page-level analysis."""
        try:
            import fitz  # PyMuPDF
            
            doc = fitz.open(str(pdf_path))
            
            total_pages = len(doc)
            total_chars = 0
            total_image_area = 0
            total_page_area = 0
            table_count = 0
            x_positions = []
            fonts = set()
            is_searchable = True
            has_watermarks = False
            has_signatures = False
            
            # Page-level metrics for mixed document detection
            page_metrics = []
            
            for page_num in range(total_pages):
                page = doc[page_num]
                
                # Get text and characters
                text = page.get_text()
                page_chars = len(text)
                total_chars += page_chars
                
                # Check if page is searchable
                if len(text.strip()) == 0:
                    is_searchable = False
                
                # Get page dimensions
                rect = page.rect
                page_area = rect.width * rect.height
                total_page_area += page_area
                
                # Analyze images - enhanced detection for scanned documents
                image_list = page.get_images()
                page_image_area = 0
                
                # Method 1: Try to get image bounding boxes
                for img_index, img in enumerate(image_list):
                    try:
                        img_rect = page.get_image_bbox(img[0])
                        if img_rect:
                            img_area = img_rect.width * img_rect.height
                            page_image_area += img_area
                    except:
                        continue
                
                # Method 2: Enhanced detection for scanned documents
                if len(text.strip()) == 0 and len(image_list) > 0:
                    # No text but has images → assume entire page is image
                    page_image_area = page_area
                elif len(text.strip()) < 100 and len(image_list) > 0:
                    # Very little text with images → likely scanned with OCR
                    page_image_area = page_area * 0.7
                elif len(text.strip()) < 50:
                    # Almost no text → probably scanned regardless of image_list
                    page_image_area = page_area * 0.8
                # Additional check: try alternative image detection
                elif page_image_area == 0 and len(text.strip()) < 200:
                    # Try to detect images using page.get_drawings() for vector graphics
                    drawings = page.get_drawings()
                    if drawings:
                        page_image_area = page_area * 0.5
                    # Also check if page has any embedded content
                    page_dict = page.get_text("dict")
                    if page_dict and any(block.get("type") == 0 for block in page_dict.get("blocks", [])):
                        page_image_area = page_area * 0.6
                
                # Store page-level metrics
                page_image_ratio = page_image_area / page_area if page_area > 0 else 0
                
                # Better searchability check: needs meaningful text content
                meaningful_text = len(text.strip()) > 100  # At least 100 characters
                page_metrics.append({
                    'page_num': page_num + 1,
                    'chars': page_chars,
                    'image_area_ratio': page_image_ratio,
                    'is_searchable': meaningful_text
                })
                
                total_image_area += page_image_area
                
                # Detect tables and collect fonts
                text_dict = page.get_text("dict")
                blocks = text_dict.get("blocks", [])
                
                for block in blocks:
                    if "lines" in block:
                        lines = block["lines"]
                        
                        # Table detection
                        if len(lines) > 2:
                            line_x_positions = []
                            for line in lines:
                                for span in line.get("spans", []):
                                    x_positions.append(span["origin"][0])
                                    line_x_positions.append(span["origin"][0])
                                    
                                    # Collect fonts
                                    if "font" in span:
                                        fonts.add(span["font"])
                            
                            # Simple table detection
                            if len(set(round(x/20) for x in line_x_positions)) >= 3:
                                table_count += 1
                
                # Check for watermarks and signatures
                if "watermark" in text.lower() or "confidential" in text.lower():
                    has_watermarks = True
                if "signature" in text.lower() or "signed" in text.lower():
                    has_signatures = True
            
            doc.close()
            
            # Compute derived metrics
            avg_chars_per_page = total_chars / total_pages if total_pages > 0 else 0
            image_area_ratio = total_image_area / total_page_area if total_page_area > 0 else 0
            column_count = self._estimate_columns(x_positions)
            
            return {
                'total_pages': total_pages,
                'total_chars': total_chars,
                'avg_chars_per_page': avg_chars_per_page,
                'image_area_ratio': image_area_ratio,
                'table_count': table_count,
                'column_count': column_count,
                'fonts': list(fonts),
                'is_searchable': is_searchable,
                'has_watermarks': has_watermarks,
                'has_signatures': has_signatures,
                'file_size': pdf_path.stat().st_size,
                'page_metrics': page_metrics  # New: page-level data
            }
            
        except ImportError:
            self.logger.error("PyMuPDF not installed")
            return {}
        except Exception as e:
            self.logger.error(f"Error extracting metrics: {e}")
            return {}
    
    def _classify_origin_type(self, metrics: Dict[str, Any]) -> str:
        """Classify document origin type with page-level mixed document detection."""
        page_metrics = metrics.get('page_metrics', [])
        total_pages = metrics.get('total_pages', 1)
        
        if not page_metrics:
            # Fallback to document-level logic
            return self._classify_origin_type_document_level(metrics)
        
        # Page-level classification
        scanned_pages = 0
        digital_pages = 0
        
        # First pass: initial classification
        for page in page_metrics:
            chars = page.get('chars', 0)
            is_searchable = page.get('is_searchable', False)
            
            if chars < 50 or not is_searchable:
                scanned_pages += 1
            else:
                digital_pages += 1
        
        # Calculate initial ratio
        scanned_ratio = scanned_pages / total_pages
        
        # Second pass: reclassify edge cases based on document pattern
        if scanned_ratio > 0.8:  # If mostly scanned, be stricter
            for page in page_metrics:
                chars = page.get('chars', 0)
                is_searchable = page.get('is_searchable', False)
                
                # Reclassify low-char pages as scanned
                if chars >= 50 and chars < 200 and is_searchable:
                    scanned_pages += 1
                    digital_pages -= 1
        
        # Recalculate final ratios
        scanned_ratio = scanned_pages / total_pages
        digital_ratio = digital_pages / total_pages
        
        # Store mixed document ratios for later use
        metrics['scanned_page_ratio'] = scanned_ratio
        metrics['digital_page_ratio'] = digital_ratio
        
        # Debug: Print page-level analysis with details
        print(f"\n🔍 Page-Level Analysis for {metrics.get('total_pages', 0)} pages:")
        print(f"   Scanned pages: {scanned_pages} ({scanned_ratio:.1%})")
        print(f"   Digital pages: {digital_pages} ({digital_ratio:.1%})")
        print(f"   Avg chars/page: {metrics.get('avg_chars_per_page', 0):.1f}")
        
        # Debug: Show which pages are classified as digital
        page_metrics = metrics.get('page_metrics', [])
        digital_pages_list = []
        scanned_pages_list = []
        for page in page_metrics:
            chars = page.get('chars', 0)
            is_searchable = page.get('is_searchable', False)
            
            if chars >= 20 and is_searchable:
                digital_pages_list.append(f"Page {page.get('page_num')}: {chars} chars (searchable)")
            else:
                scanned_pages_list.append(f"Page {page.get('page_num')}: {chars} chars (not searchable)")
        
        if digital_pages_list:
            print(f"   📄 Digital pages found:")
            for page_info in digital_pages_list[:5]:  # Show first 5
                print(f"      {page_info}")
            if len(digital_pages_list) > 5:
                print(f"      ... and {len(digital_pages_list) - 5} more")
        
        if len(scanned_pages_list) <= 5:  # Show scanned pages if few
            print(f"   🔍 Scanned pages (sample):")
            for page_info in scanned_pages_list[:3]:
                print(f"      {page_info}")
            if len(scanned_pages_list) > 3:
                print(f"      ... and {len(scanned_pages_list) - 3} more")
        
        # Determine origin type
        if scanned_ratio == 0:
            result = "native_digital"
        elif scanned_ratio == 1:
            result = "scanned_image"
        else:
            result = "mixed"
        
        print(f"   → Classification: {result}")
        return result
    
    def _classify_origin_type_document_level(self, metrics: Dict[str, Any]) -> str:
        """Fallback document-level classification with hard guards for edge cases."""
        avg_chars = metrics.get('avg_chars_per_page', 0)
        total_chars = metrics.get('total_chars', 0)
        pages = metrics.get('total_pages', 1)
        file_size = metrics.get('file_size', 0)
        is_searchable = metrics.get('is_searchable', True)
        
        # HARD GUARD: Extremely low character count = scanned document
        if avg_chars < 100:
            return "scanned_image"
        
        # HARD GUARD: Large file + no text = scanned document  
        if file_size > 10_000_000 and total_chars < 1000 and not is_searchable:
            return "scanned_image"
        
        # Normal classification logic
        if is_searchable and avg_chars > 500:
            return "native_digital"
        elif not is_searchable and avg_chars < 50:
            return "scanned_image"
        else:
            return "unknown"
    
    def _classify_layout_complexity(self, metrics: Dict[str, Any]) -> str:
        """Classify layout complexity based on metrics."""
        columns = metrics.get('column_count', 1)
        tables = metrics.get('table_count', 0)
        image_ratio = metrics.get('image_area_ratio', 0)
        
        if tables > 3:
            return "table_heavy"
        elif image_ratio > 0.3:
            return "figure_heavy"
        elif columns > 2:
            return "multi_column"
        else:
            return "single_column"
    
    def _assign_category_from_rules(self, metrics: Dict[str, Any]) -> Tuple[str, float, str]:
        """Assign category using calibrated YAML rules with mixed document handling."""
        categories = self.rules.get('document_categories', {})
        origin_type = metrics.get('origin_type', '')
        
        # HARD GUARD: Mixed documents get special handling
        if origin_type == "mixed":
            scanned_ratio = metrics.get('scanned_page_ratio', 0)
            
            # If mostly scanned, treat as high_complexity
            if scanned_ratio > 0.3:
                return "high_complexity", scanned_ratio, "hybrid"
            else:
                # Mostly digital, use normal classification
                pass
        
        # HARD GUARD: Extremely low character count = high_complexity (scanned)
        avg_chars = metrics.get('avg_chars_per_page', 0)
        if avg_chars < 100:
            return "high_complexity", 1.0, "vision"
        
        category_scores = {}
        
        for category_name, category_config in categories.items():
            criteria = category_config.get('criteria', {})
            confidence_threshold = category_config.get('confidence_threshold', 0.7)
            
            matches = 0
            total_criteria = 0
            
            # Check each criterion against metrics
            if 'avg_chars_per_page_min' in criteria:
                total_criteria += 1
                if metrics.get('avg_chars_per_page', 0) >= criteria['avg_chars_per_page_min']:
                    matches += 1
            
            if 'avg_chars_per_page_max' in criteria:
                total_criteria += 1
                if metrics.get('avg_chars_per_page', 0) <= criteria['avg_chars_per_page_max']:
                    matches += 1
            
            if 'detected_table_count_min' in criteria:
                total_criteria += 1
                if metrics.get('table_count', 0) >= criteria['detected_table_count_min']:
                    matches += 1
            
            if 'detected_table_count_max' in criteria:
                total_criteria += 1
                if metrics.get('table_count', 0) <= criteria['detected_table_count_max']:
                    matches += 1
            
            if 'image_area_ratio_min' in criteria:
                total_criteria += 1
                if metrics.get('image_area_ratio', 0) >= criteria['image_area_ratio_min']:
                    matches += 1
            
            if 'image_area_ratio_max' in criteria:
                total_criteria += 1
                if metrics.get('image_area_ratio', 0) <= criteria['image_area_ratio_max']:
                    matches += 1
            
            if 'x_cluster_count_min' in criteria:
                total_criteria += 1
                if metrics.get('column_count', 1) >= criteria['x_cluster_count_min']:
                    matches += 1
            
            if 'x_cluster_count_max' in criteria:
                total_criteria += 1
                if metrics.get('column_count', 1) <= criteria['x_cluster_count_max']:
                    matches += 1
            
            # Calculate confidence
            confidence = matches / total_criteria if total_criteria > 0 else 0
            
            # Only consider if above threshold
            if confidence >= confidence_threshold:
                category_scores[category_name] = confidence
        
        # Select best category
        if category_scores:
            best_category = max(category_scores, key=category_scores.get)
            best_confidence = category_scores[best_category]
            recommended_strategy = categories[best_category].get('recommended_strategy', 'layout')
        else:
            # Default fallback
            best_category = 'moderate_complexity'
            best_confidence = 0.5
            recommended_strategy = 'layout'
        
        return best_category, best_confidence, recommended_strategy
    
    def _determine_extraction_cost(self, recommended_strategy: str) -> str:
        """Determine extraction cost level based on strategy."""
        cost_mapping = {
            'fast_text': 'fast_text',
            'layout': 'needs_layout_model',
            'vision': 'needs_vision_model'
        }
        return cost_mapping.get(recommended_strategy, 'needs_layout_model')
    
    def _estimate_columns(self, x_positions: List[float]) -> int:
        """Estimate number of columns from x-positions."""
        if len(x_positions) < 10:
            return 1
        
        # Simple clustering
        clustered_positions = {}
        for x in x_positions:
            cluster_key = round(x / 20) * 20
            clustered_positions[cluster_key] = clustered_positions.get(cluster_key, 0) + 1
        
        threshold = len(x_positions) * 0.05
        significant_clusters = [count for count in clustered_positions.values() if count >= threshold]
        
        return len(significant_clusters)
    
    def _detect_language(self, pdf_path: Path) -> Tuple[str, float]:
        """Detect document language."""
        try:
            import fitz
            
            doc = fitz.open(str(pdf_path))
            text = ""
            
            # Sample text from first few pages
            for page_num in range(min(3, len(doc))):
                page = doc[page_num]
                page_text = page.get_text()
                text += page_text + " "
            
            doc.close()
            
            # Simple language detection based on common words
            text_lower = text.lower()
            
            # English indicators
            english_words = ['the', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by']
            english_matches = sum(1 for word in english_words if word in text_lower)
            
            # Simple heuristic
            if english_matches >= 5:
                return "en", 0.8
            else:
                return "unknown", 0.5
                
        except Exception as e:
            self.logger.error(f"Language detection failed: {e}")
            return "unknown", 0.5
    
    def _classify_domain(self, pdf_path: Path) -> Tuple[str, float]:
        """Classify document domain."""
        try:
            import fitz
            
            doc = fitz.open(str(pdf_path))
            text = ""
            
            # Sample text from first few pages
            for page_num in range(min(3, len(doc))):
                page = doc[page_num]
                page_text = page.get_text()
                text += page_text + " "
            
            doc.close()
            
            text_lower = text.lower()
            domain_scores = {}
            
            # Score each domain
            for domain, keywords in self.domain_keywords.items():
                matches = sum(1 for keyword in keywords if keyword in text_lower)
                domain_scores[domain] = matches
            
            if domain_scores:
                best_domain = max(domain_scores, key=domain_scores.get)
                score = domain_scores[best_domain]
                confidence = min(score / 3.0, 1.0)  # Normalize to 0-1
                
                if confidence > 0.3:
                    return best_domain, confidence
            
            return "general", 0.5
            
        except Exception as e:
            self.logger.error(f"Domain classification failed: {e}")
            return "general", 0.5
    
    def _determine_processing_params(self, layout_complexity: LayoutComplexity, 
                                   metrics: Dict[str, Any], 
                                   domain: str) -> Tuple[str, float, ProcessingPriority]:
        """Determine processing strategy, cost, and priority."""
        
        # Base strategy from layout complexity
        strategy_mapping = {
            LayoutComplexity.SIMPLE: "fast_text",
            LayoutComplexity.MODERATE: "layout",
            LayoutComplexity.COMPLEX: "vision",
            LayoutComplexity.VERY_COMPLEX: "vision"
        }
        
        strategy = strategy_mapping.get(layout_complexity, "layout")
        
        # Estimate cost (in seconds)
        base_costs = {
            "fast_text": 0.5,
            "layout": 2.0,
            "vision": 8.0
        }
        
        base_cost = base_costs.get(strategy, 2.0)
        
        # Adjust for document size
        page_multiplier = 1 + (metrics.get('total_pages', 1) - 1) * 0.1
        complexity_multiplier = {
            LayoutComplexity.SIMPLE: 1.0,
            LayoutComplexity.MODERATE: 1.5,
            LayoutComplexity.COMPLEX: 2.0,
            LayoutComplexity.VERY_COMPLEX: 3.0
        }.get(layout_complexity, 1.0)
        
        estimated_cost = base_cost * page_multiplier * complexity_multiplier
        
        # Determine priority
        priority_factors = {
            'legal': ProcessingPriority.HIGH,
            'financial': ProcessingPriority.HIGH,
            'medical': ProcessingPriority.HIGH,
            'government': ProcessingPriority.MEDIUM,
            'business': ProcessingPriority.MEDIUM,
            'technical': ProcessingPriority.MEDIUM,
            'academic': ProcessingPriority.LOW,
            'personal': ProcessingPriority.LOW,
            'general': ProcessingPriority.LOW
        }
        
        priority = priority_factors.get(domain, ProcessingPriority.MEDIUM)
        
        return strategy, estimated_cost, priority
    
    def _calculate_quality_scores(self, metrics: Dict[str, Any]) -> Tuple[float, float, float]:
        """Calculate text, structure, and overall quality scores."""
        
        # Text quality (based on searchability and character density)
        text_quality = 0.5  # Base score
        if metrics.get('is_searchable', False):
            text_quality += 0.3
        
        chars_per_page = metrics.get('avg_chars_per_page', 0)
        if 100 <= chars_per_page <= 3000:  # Reasonable range
            text_quality += 0.2
        
        text_quality = min(text_quality, 1.0)
        
        # Structure quality (based on layout indicators)
        structure_quality = 0.5  # Base score
        
        # Penalize excessive images (likely scanned)
        image_ratio = metrics.get('image_area_ratio', 0)
        if image_ratio < 0.3:
            structure_quality += 0.2
        
        # Reward reasonable table count
        table_count = metrics.get('table_count', 0)
        if 0 <= table_count <= 5:
            structure_quality += 0.2
        
        # Reward reasonable column count
        columns = metrics.get('column_count', 1)
        if 1 <= columns <= 3:
            structure_quality += 0.1
        
        structure_quality = min(structure_quality, 1.0)
        
        # Overall quality (weighted average)
        overall_quality = (text_quality * 0.6 + structure_quality * 0.4)
        
        return text_quality, structure_quality, overall_quality
    
    def _save_profile(self, profile: DocumentProfile):
        """Save document profile to JSON file."""
        profile_file = self.profiles_dir / f"{profile.document_id}.json"
        
        with open(profile_file, 'w') as f:
            json.dump(profile.dict(), f, indent=2, default=str)
        
        self.logger.info(f"Profile saved: {profile_file}")

if __name__ == "__main__":
    # Example usage
    classifier = TriageClassifier(
        rules_file=".refinery/rules/extraction_rules.yaml",
        profiles_dir=".refinery/profiles"
    )
    
    # Classify a document
    profile = classifier.classify_document("data/raw/example.pdf")
    print(f"Classification complete: {profile.document_id}")
