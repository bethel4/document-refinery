"""
Document Triage Engine
Classifies documents based on calibrated thresholds and lightweight analysis.
"""

import json
import logging
import time
from pathlib import Path
from typing import Dict, List, Any, Tuple
from datetime import datetime
from enum import Enum

import yaml

# Import from new models
from src.models.document_profile import DocumentProfile, OriginType, LayoutComplexity, DocumentCategory, ExtractionStrategy
from src.domain_analysis.triage.domain_classifier import DomainClassifier, KeywordDomainClassifier

class ProcessingPriority(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"

class TriageClassifier:
    """Document triage classifier using calibrated thresholds."""
    
    def __init__(
        self,
        rules_file: str,
        profiles_dir: str,
        domain_classifier: DomainClassifier | None = None,
    ):
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
        self.domain_classifier = domain_classifier or KeywordDomainClassifier(self.domain_keywords)
    
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
        metrics["origin_type"] = origin_type
        
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
            is_searchable=metrics.get('is_searchable', True),
            confidence=category_confidence,  # Use category confidence as overall confidence
            pages=metrics.get('total_pages', 0)
        )
        
        # Save profile
        self._save_profile(profile)
        
        return profile
    
    def _extract_lightweight_metrics(self, pdf_path: Path) -> Dict[str, Any]:
        """Extract lightweight metrics for triage classification with page-level analysis."""
        try:
            import fitz  # PyMuPDF

            # NOTE: This stage must be lightweight. Avoid expensive operations like
            # `page.get_text("dict")` / `page.get_drawings()` across all pages.
            max_pages_analyzed = (
                int((self.rules.get("triage", {}) or {}).get("max_pages_analyzed", 12))
                if isinstance(self.rules, dict)
                else 12
            )
            max_pages_analyzed = max(1, max_pages_analyzed)

            started = time.time()
            with fitz.open(str(pdf_path)) as doc:
                total_pages = len(doc)
                sample_page_idxs = self._select_sample_pages(total_pages, max_pages_analyzed)

                total_chars_sample = 0
                total_image_ratio_sample = 0.0
                table_hits = 0
                x_positions: List[float] = []
                fonts = set()
                has_watermarks = False
                has_signatures = False

                page_metrics: List[Dict[str, Any]] = []

                for page_idx in sample_page_idxs:
                    page = doc[page_idx]
                    text = page.get_text("text") or ""
                    page_chars = len(text)

                    # Fast image dominance heuristic: avoid bbox computations (slow).
                    images = page.get_images(full=True)
                    image_ratio = 0.0
                    if page_chars < 50:
                        image_ratio = 0.8
                    elif images and page_chars < 200:
                        image_ratio = 0.6
                    elif images:
                        image_ratio = 0.3

                    meaningful_text = len(text.strip()) > 100
                    page_metrics.append(
                        {
                            "page_num": int(page_idx + 1),
                            "chars": int(page_chars),
                            "image_area_ratio": float(image_ratio),
                            "is_searchable": bool(meaningful_text),
                        }
                    )

                    total_chars_sample += page_chars
                    total_image_ratio_sample += image_ratio

                    # Cheap table heuristic: many aligned columns/spaces and digits.
                    if ("  " in text and sum(ch.isdigit() for ch in text) > 30) or ("\t" in text):
                        table_hits += 1

                    # Cheap layout heuristic for columns: use block x0 positions (faster than dict/spans).
                    try:
                        blocks = page.get_text("blocks") or []
                        for b in blocks:
                            if isinstance(b, (list, tuple)) and len(b) >= 5:
                                x_positions.append(float(b[0]))
                    except Exception:
                        pass

                    # Font detection (optional): only sample spans when present, but keep bounded.
                    try:
                        raw = page.get_text("rawdict") or {}
                        for block in raw.get("blocks", [])[:50]:
                            for line in block.get("lines", [])[:50]:
                                for span in line.get("spans", [])[:50]:
                                    font = span.get("font")
                                    if font:
                                        fonts.add(font)
                    except Exception:
                        pass

                    lower = text.lower()
                    if "watermark" in lower or "confidential" in lower:
                        has_watermarks = True
                    if "signature" in lower or "signed" in lower:
                        has_signatures = True

                sampled_pages = max(1, len(sample_page_idxs))
                avg_chars_per_page = float(total_chars_sample) / float(sampled_pages)
                image_area_ratio = float(total_image_ratio_sample) / float(sampled_pages)
                column_count = self._estimate_columns(x_positions)

            file_size = pdf_path.stat().st_size
            self.logger.info(
                "Triage metrics extracted: pages=%s sampled=%s in %.2fs",
                total_pages,
                len(sample_page_idxs),
                time.time() - started,
            )

            return {
                "total_pages": int(total_pages),
                # Estimated totals derived from samples (keep triage lightweight).
                "total_chars": int(avg_chars_per_page * max(1, int(total_pages))),
                "avg_chars_per_page": float(avg_chars_per_page),
                "image_area_ratio": float(image_area_ratio),
                "table_count": int(table_hits),  # sample-based proxy
                "column_count": int(column_count),
                "fonts": list(fonts),
                # Searchable if a majority of sampled pages have meaningful text.
                "is_searchable": bool(sum(1 for p in page_metrics if p.get("is_searchable")) >= max(1, len(page_metrics) // 2)),
                "has_watermarks": bool(has_watermarks),
                "has_signatures": bool(has_signatures),
                "file_size": int(file_size),
                "page_metrics": page_metrics,
            }
            
        except ImportError:
            self.logger.error("PyMuPDF not installed")
            return {}
        except Exception as e:
            self.logger.error(f"Error extracting metrics: {e}")
            return {}

    def _select_sample_pages(self, total_pages: int, max_pages_analyzed: int) -> List[int]:
        """Pick a small, deterministic set of pages for triage sampling."""
        if total_pages <= 0:
            return [0]

        max_pages_analyzed = max(1, int(max_pages_analyzed))
        if total_pages <= max_pages_analyzed:
            return list(range(total_pages))

        # Always include first pages + last page, then evenly spaced interior pages.
        picks = set()
        for i in range(min(3, total_pages)):
            picks.add(i)
        picks.add(total_pages - 1)

        remaining = max_pages_analyzed - len(picks)
        if remaining > 0:
            step = max(1, (total_pages - 1) // (remaining + 1))
            idx = step
            while len(picks) < max_pages_analyzed and idx < total_pages - 1:
                picks.add(idx)
                idx += step

        return sorted(picks)[:max_pages_analyzed]
    
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
            image_ratio = float(page.get('image_area_ratio', 0.0))
            
            # Use character stream + image dominance per page.
            if chars < 50 or not is_searchable or image_ratio > 0.65:
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
        fonts = metrics.get("fonts", [])
        image_ratio = float(metrics.get("image_area_ratio", 0.0))
        avg_chars = float(metrics.get("avg_chars_per_page", 0.0))
        
        # Store mixed document ratios for later use
        metrics['scanned_page_ratio'] = scanned_ratio
        metrics['digital_page_ratio'] = digital_ratio
        
        # Determine origin type
        # Guardrails using document-level image ratio + font metadata + character density.
        if scanned_ratio == 0 and image_ratio < 0.35 and bool(fonts) and avg_chars > 300:
            result = "native_digital"
        elif scanned_ratio == 1 or (image_ratio > 0.7 and avg_chars < 120 and not fonts):
            result = "scanned_image"
        else:
            result = "mixed"
        
        self.logger.info(
            "Origin classification: %s (scanned_ratio=%.1f%%, avg_chars=%.1f, image_ratio=%.2f)",
            result,
            scanned_ratio * 100.0,
            avg_chars,
            image_ratio,
        )
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
        layout_complexity = self._classify_layout_complexity(metrics)
        
        # Policy guard: mixed-origin docs should default to layout-aware extraction,
        # with page-level escalation handled later in the router.
        if origin_type == "mixed":
            scanned_ratio = metrics.get('scanned_page_ratio', 0)
            if scanned_ratio >= 0.5:
                return "high_complexity", scanned_ratio, "hybrid"
            return "moderate_complexity", max(0.7, 1.0 - scanned_ratio), "layout"

        # Policy guard: structurally complex digital docs use layout-aware extraction first.
        if layout_complexity in {"multi_column", "table_heavy"} and origin_type != "scanned_image":
            return "moderate_complexity", 0.8, "layout"
        
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
            text = self._extract_sample_text(pdf_path, sample_pages=3)
            return self.domain_classifier.classify(text)
            
        except Exception as e:
            self.logger.error(f"Domain classification failed: {e}")
            return "general", 0.5

    def _extract_sample_text(self, pdf_path: Path, sample_pages: int = 3) -> str:
        """Extract a short text sample for language/domain classification."""
        import fitz

        doc = fitz.open(str(pdf_path))
        try:
            text_parts: List[str] = []
            for page_num in range(min(sample_pages, len(doc))):
                text_parts.append(doc[page_num].get_text())
            return " ".join(text_parts)
        finally:
            doc.close()
    
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
            json.dump(profile.model_dump(mode="json"), f, indent=2, default=str)
        
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
