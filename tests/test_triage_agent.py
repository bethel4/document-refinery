"""
Unit tests for Triage Agent classification logic.
"""

import pytest
import tempfile
import shutil
from pathlib import Path
from datetime import datetime
from unittest.mock import Mock, patch

from src.domain_analysis.triage.document_classifier import TriageClassifier
from src.models.document_profile import DocumentProfile, OriginType, DocumentCategory, ExtractionStrategy


class TestTriageAgent:
    """Test suite for Triage Agent classification functionality."""
    
    @pytest.fixture
    def temp_dirs(self):
        """Create temporary directories for testing."""
        temp_dir = tempfile.mkdtemp()
        profiles_dir = Path(temp_dir) / "profiles"
        profiles_dir.mkdir(parents=True, exist_ok=True)
        
        rules_file = Path(temp_dir) / "rules.yaml"
        
        # Create minimal rules file
        rules_content = """
document_categories:
  high_complexity:
    confidence_threshold: 0.7
    criteria:
      avg_chars_per_page_min: 1000
      image_area_ratio_min: 0.3
  moderate_complexity:
    confidence_threshold: 0.6
    criteria:
      avg_chars_per_page_min: 500
      image_area_ratio_max: 0.3
  simple_text:
    confidence_threshold: 0.8
    criteria:
      avg_chars_per_page_max: 500
      image_area_ratio_max: 0.1
"""
        rules_file.write_text(rules_content)
        
        yield str(rules_file), str(profiles_dir)
        
        shutil.rmtree(temp_dir)
    
    @pytest.fixture
    def triage_agent(self, temp_dirs):
        """Create TriageAgent instance for testing."""
        rules_file, profiles_dir = temp_dirs
        return TriageClassifier(rules_file, profiles_dir)
    
    def test_initialization(self, triage_agent):
        """Test TriageAgent initialization."""
        assert triage_agent.rules_file.exists()
        assert triage_agent.profiles_dir.exists()
        assert triage_agent.rules is not None
        assert "document_categories" in triage_agent.rules
    
    def test_classify_native_digital_document(self, triage_agent):
        """Test classification of native digital document."""
        # Mock metrics for native digital document
        metrics = {
            "total_pages": 10,
            "total_chars": 15000,
            "avg_chars_per_page": 1500,
            "image_area_ratio": 0.05,
            "detected_table_count": 2,
            "x_cluster_count": 2,
            "fonts": ["Arial", "Times New Roman"],
            "is_searchable": True,
            "has_watermarks": False,
            "has_signatures": False,
            "file_size": 500000,
            "page_metrics": [
                {"page_num": i + 1, "chars": 1500, "image_area_ratio": 0.05, "is_searchable": True}
                for i in range(10)
            ]
        }
        
        profile = triage_agent._create_profile("test_doc", "test.pdf", "/path/to/test.pdf", metrics)
        
        # Verify classification
        assert profile.origin_type == OriginType.NATIVE_DIGITAL
        assert profile.category == DocumentCategory.MODERATE_COMPLEXITY
        assert profile.recommended_strategy == ExtractionStrategy.LAYOUT
        assert profile.confidence > 0.5
        assert profile.scanned_page_ratio == 0.0
        assert profile.digital_page_ratio == 1.0
    
    def test_classify_scanned_document(self, triage_agent):
        """Test classification of scanned document."""
        # Mock metrics for scanned document
        metrics = {
            "total_pages": 5,
            "total_chars": 250,
            "avg_chars_per_page": 50,
            "image_area_ratio": 0.95,
            "detected_table_count": 0,
            "x_cluster_count": 1,
            "fonts": [],
            "is_searchable": False,
            "has_watermarks": False,
            "has_signatures": False,
            "file_size": 2000000,
            "page_metrics": [
                {"page_num": i + 1, "chars": 50, "image_area_ratio": 0.95, "is_searchable": False}
                for i in range(5)
            ]
        }
        
        profile = triage_agent._create_profile("scanned_doc", "scanned.pdf", "/path/to/scanned.pdf", metrics)
        
        # Verify classification
        assert profile.origin_type == OriginType.SCANNED_IMAGE
        assert profile.category == DocumentCategory.HIGH_COMPLEXITY
        assert profile.recommended_strategy == ExtractionStrategy.VISION
        assert profile.scanned_page_ratio == 1.0
        assert profile.digital_page_ratio == 0.0
        assert not profile.is_searchable
    
    def test_classify_mixed_document(self, triage_agent):
        """Test classification of mixed document."""
        # Mock metrics for mixed document
        metrics = {
            "total_pages": 10,
            "total_chars": 8000,
            "avg_chars_per_page": 800,
            "image_area_ratio": 0.6,
            "detected_table_count": 3,
            "x_cluster_count": 2,
            "fonts": ["Arial"],
            "is_searchable": True,
            "has_watermarks": False,
            "has_signatures": False,
            "file_size": 1500000,
            "page_metrics": [
                {"page_num": i + 1, "chars": 1500 if i < 3 else 50, "image_area_ratio": 0.1 if i < 3 else 0.9, "is_searchable": i < 3}
                for i in range(10)
            ]
        }
        
        profile = triage_agent._create_profile("mixed_doc", "mixed.pdf", "/path/to/mixed.pdf", metrics)
        
        # Verify classification
        assert profile.origin_type == OriginType.MIXED
        assert profile.scanned_page_ratio == 0.7  # 7 out of 10 pages scanned
        assert profile.digital_page_ratio == 0.3   # 3 out of 10 pages digital
        assert profile.category == DocumentCategory.HIGH_COMPLEXITY  # Mixed with high scanned ratio
    
    def test_domain_classification(self, triage_agent):
        """Test domain hint classification."""
        # Test financial document
        financial_text = "This invoice shows payment balance and bank account details for tax purposes."
        domain, confidence = triage_agent._classify_domain(financial_text)
        
        assert domain == "financial"
        assert confidence > 0.5
        
        # Test legal document
        legal_text = "This contract agreement is governed by law and court jurisdiction."
        domain, confidence = triage_agent._classify_domain(legal_text)
        
        assert domain == "legal"
        assert confidence > 0.5
    
    def test_layout_complexity_detection(self, triage_agent):
        """Test layout complexity detection."""
        # Test simple layout
        simple_metrics = {
            "x_cluster_count": 1,
            "detected_table_count": 0,
            "avg_chars_per_page": 300
        }
        complexity = triage_agent._classify_layout_complexity(simple_metrics)
        assert complexity in ["single_column", "simple"]
        
        # Test complex layout
        complex_metrics = {
            "x_cluster_count": 4,
            "detected_table_count": 5,
            "avg_chars_per_page": 2000
        }
        complexity = triage_agent._classify_layout_complexity(complex_metrics)
        assert complexity in ["multi_column", "table_heavy", "complex"]
    
    def test_confidence_scoring(self, triage_agent):
        """Test confidence scoring for classifications."""
        # Test high confidence case
        clear_metrics = {
            "total_pages": 5,
            "total_chars": 10000,
            "avg_chars_per_page": 2000,
            "image_area_ratio": 0.02,
            "detected_table_count": 0,
            "is_searchable": True,
            "fonts": ["Arial", "Times"],
            "page_metrics": [
                {"page_num": i + 1, "chars": 2000, "image_area_ratio": 0.02, "is_searchable": True}
                for i in range(5)
            ]
        }
        
        profile = triage_agent._create_profile("clear_doc", "clear.pdf", "/path/to/clear.pdf", clear_metrics)
        assert profile.confidence > 0.8
        
        # Test low confidence case
        ambiguous_metrics = {
            "total_pages": 2,
            "total_chars": 400,
            "avg_chars_per_page": 200,
            "image_area_ratio": 0.5,
            "detected_table_count": 1,
            "is_searchable": True,
            "fonts": [],
            "page_metrics": [
                {"page_num": i + 1, "chars": 200, "image_area_ratio": 0.5, "is_searchable": True}
                for i in range(2)
            ]
        }
        
        profile = triage_agent._create_profile("ambiguous_doc", "ambiguous.pdf", "/path/to/ambiguous.pdf", ambiguous_metrics)
        assert profile.confidence < 0.8
    
    def test_profile_serialization(self, triage_agent):
        """Test DocumentProfile serialization."""
        metrics = {
            "total_pages": 3,
            "total_chars": 1500,
            "avg_chars_per_page": 500,
            "image_area_ratio": 0.2,
            "detected_table_count": 1,
            "x_cluster_count": 2,
            "fonts": ["Arial"],
            "is_searchable": True,
            "has_watermarks": False,
            "has_signatures": False,
            "file_size": 300000,
            "page_metrics": [
                {"page_num": i + 1, "chars": 500, "image_area_ratio": 0.2, "is_searchable": True}
                for i in range(3)
            ]
        }
        
        profile = triage_agent._create_profile("test_doc", "test.pdf", "/path/to/test.pdf", metrics)
        
        # Test to_dict method
        profile_dict = profile.to_dict()
        assert "document_id" in profile_dict
        assert "origin_type" in profile_dict
        assert "category" in profile_dict
        assert "confidence" in profile_dict
        
        # Test JSON serialization
        import json
        json_str = profile.model_dump_json()
        assert isinstance(json_str, str)
        
        # Test deserialization
        loaded_profile = DocumentProfile.model_validate_json(json_str)
        assert loaded_profile.document_id == profile.document_id
        assert loaded_profile.origin_type == profile.origin_type
    
    def test_edge_cases(self, triage_agent):
        """Test edge cases and error handling."""
        # Test empty metrics
        empty_metrics = {}
        profile = triage_agent._create_profile("empty_doc", "empty.pdf", "/path/to/empty.pdf", empty_metrics)
        
        # Should handle gracefully with default values
        assert profile.total_pages == 0
        assert profile.total_chars == 0
        assert profile.avg_chars_per_page == 0.0
        
        # Test very large document
        large_metrics = {
            "total_pages": 1000,
            "total_chars": 5000000,
            "avg_chars_per_page": 5000,
            "image_area_ratio": 0.1,
            "detected_table_count": 50,
            "x_cluster_count": 3,
            "fonts": ["Arial"],
            "is_searchable": True,
            "has_watermarks": False,
            "has_signatures": False,
            "file_size": 10000000,
            "page_metrics": [
                {"page_num": i + 1, "chars": 5000, "image_area_ratio": 0.1, "is_searchable": True}
                for i in range(1000)
            ]
        }
        
        profile = triage_agent._create_profile("large_doc", "large.pdf", "/path/to/large.pdf", large_metrics)
        assert profile.total_pages == 1000
        assert profile.category == DocumentCategory.HIGH_COMPLEXITY
