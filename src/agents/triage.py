"""
Triage Agent - Document characterization and strategy recommendation.
"""

from typing import Dict, Any, List
import logging
from pathlib import Path

from ..models.document_models import DocumentProfile, OriginType, LayoutComplexity, ExtractionStrategy
from ..domain_analysis.triage.document_classifier import TriageClassifier

logger = logging.getLogger(__name__)

class TriageAgent:
    """Working Triage Agent for document characterization."""
    
    def __init__(self, rules_file: str = ".refinery/rules/extraction_rules.yaml"):
        self.rules_file = Path(rules_file)
        self.profiles_dir = Path(".refinery/profiles")
        
        # Initialize classifier
        self.classifier = TriageClassifier(
            rules_file=str(self.rules_file),
            profiles_dir=str(self.profiles_dir)
        )
        
        logger.info("TriageAgent initialized")
    
    def analyze_document(self, pdf_path: str) -> DocumentProfile:
        """
        Complete document analysis and characterization.
        
        Args:
            pdf_path: Path to PDF file
            
        Returns:
            DocumentProfile with complete characterization
        """
        logger.info(f"Analyzing document: {pdf_path}")
        
        try:
            # Use existing classifier for characterization
            profile = self.classifier.classify_document(pdf_path)
            
            # Convert to Pydantic model
            doc_profile = DocumentProfile(
                document_id=profile.document_id,
                filename=profile.filename,
                file_path=profile.file_path,
                file_size_bytes=profile.file_size_bytes,
                analyzed_at=profile.analyzed_at,
                
                # Classification results
                origin_type=OriginType(profile.origin_type),
                layout_complexity=LayoutComplexity(profile.layout_complexity),
                language=profile.language,
                language_confidence=profile.language_confidence,
                
                domain_hint=profile.domain_hint,
                domain_confidence=profile.domain_confidence,
                
                category=profile.category,
                category_confidence=profile.category_confidence,
                
                # Processing recommendations
                recommended_strategy=ExtractionStrategy(profile.recommended_strategy),
                estimated_extraction_cost=profile.estimated_extraction_cost,
                
                # Document metrics
                total_pages=profile.total_pages,
                total_chars=profile.total_chars,
                avg_chars_per_page=profile.avg_chars_per_page,
                image_area_ratio=profile.image_area_ratio,
                detected_table_count=profile.detected_table_count,
                x_cluster_count=profile.x_cluster_count,
                
                # Mixed document metrics
                scanned_page_ratio=getattr(profile, 'scanned_page_ratio', None),
                digital_page_ratio=getattr(profile, 'digital_page_ratio', None),
                
                # Quality scores
                text_quality_score=profile.text_quality_score,
                structure_quality_score=profile.structure_quality_score,
                overall_quality_score=profile.overall_quality_score,
                
                # Additional properties
                detected_fonts=profile.detected_fonts,
                has_watermarks=profile.has_watermarks,
                has_signatures=profile.has_signatures,
                is_searchable=profile.is_searchable
            )
            
            logger.info(f"Document analysis completed: {profile.document_id} → {profile.recommended_strategy}")
            return doc_profile
            
        except Exception as e:
            logger.error(f"Document analysis failed: {e}")
            raise
    
    def batch_analyze(self, pdf_folder: str = "data/raw") -> List[DocumentProfile]:
        """
        Analyze all documents in folder.
        
        Args:
            pdf_folder: Path to PDF folder
            
        Returns:
            List of DocumentProfile objects
        """
        pdf_path = Path(pdf_folder)
        pdf_files = list(pdf_path.glob("*.pdf"))
        
        profiles = []
        for pdf_file in pdf_files:
            try:
                profile = self.analyze_document(str(pdf_file))
                profiles.append(profile)
            except Exception as e:
                logger.error(f"Failed to analyze {pdf_file.name}: {e}")
        
        logger.info(f"Batch analysis completed: {len(profiles)} documents")
        return profiles
    
    def get_strategy_stats(self, profiles: List[DocumentProfile]) -> Dict[str, Any]:
        """Get strategy distribution statistics."""
        strategy_counts = {}
        origin_counts = {}
        complexity_counts = {}
        
        for profile in profiles:
            # Strategy distribution
            strategy = profile.recommended_strategy.value
            strategy_counts[strategy] = strategy_counts.get(strategy, 0) + 1
            
            # Origin type distribution
            origin = profile.origin_type.value
            origin_counts[origin] = origin_counts.get(origin, 0) + 1
            
            # Complexity distribution
            complexity = profile.layout_complexity.value
            complexity_counts[complexity] = complexity_counts.get(complexity, 0) + 1
        
        return {
            "total_documents": len(profiles),
            "strategy_distribution": strategy_counts,
            "origin_distribution": origin_counts,
            "complexity_distribution": complexity_counts
        }
