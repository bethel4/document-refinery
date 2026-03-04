"""
Unit tests for extraction confidence scoring and routing logic.
"""

import pytest
import tempfile
import shutil
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

from src.extraction.extraction_router import ExtractionRouter
from src.models.extracted_document import ExtractedDocument, PageExtraction, ExtractionMethod
from src.models.document_profile import DocumentProfile, OriginType, DocumentCategory, ExtractionStrategy


class TestExtraction:
    """Test suite for extraction confidence scoring and routing."""
    
    @pytest.fixture
    def temp_dirs(self):
        """Create temporary directories for testing."""
        temp_dir = tempfile.mkdtemp()
        rules_file = Path(temp_dir) / "rules.yaml"
        
        # Create extraction rules
        rules_content = """
extraction_strategies:
  fast_text:
    confidence_threshold: 0.8
    max_pages: 50
    cost_per_page: 0.1
  layout:
    confidence_threshold: 0.7
    max_pages: 100
    cost_per_page: 0.5
  vision:
    confidence_threshold: 0.6
    max_pages: 10
    cost_per_page: 2.0

escalation_rules:
  fast_text_to_layout:
    confidence_threshold: 0.7
    max_pages: 100
  layout_to_vision:
    confidence_threshold: 0.6
    max_pages: 10
"""
        rules_file.write_text(rules_content)
        
        yield str(rules_file)
        
        shutil.rmtree(temp_dir)
    
    @pytest.fixture
    def extraction_router(self, temp_dirs):
        """Create ExtractionRouter instance for testing."""
        rules_file = temp_dirs
        return ExtractionRouter(rules_file)
    
    def test_router_initialization(self, extraction_router):
        """Test ExtractionRouter initialization."""
        assert extraction_router.rules is not None
        assert "extraction_strategies" in extraction_router.rules
        assert "escalation_rules" in extraction_router.rules
        assert extraction_router.confidence_threshold == 0.7
    
    def test_high_confidence_fast_text_extraction(self, extraction_router):
        """Test high confidence fast text extraction (no escalation)."""
        # Create mock profile for digital document
        profile = Mock()
        profile.recommended_strategy = ExtractionStrategy.FAST_TEXT
        profile.category = DocumentCategory.SIMPLE_TEXT
        profile.origin_type = OriginType.NATIVE_DIGITAL
        
        # Mock high confidence pages
        pages = [
            {
                "page_num": 1,
                "text": "This is clear text content with good structure.",
                "text_length": 50,
                "confidence": 0.95,
                "extraction_method": "fast_text",
                "tables": []
            },
            {
                "page_num": 2,
                "text": "Another page with clear text and proper formatting.",
                "text_length": 55,
                "confidence": 0.92,
                "extraction_method": "fast_text",
                "tables": []
            }
        ]
        
        # Mock fast text extractor
        extraction_router.fast_extractor.extract = Mock(return_value={"pages": pages})
        extraction_router.layout_extractor.extract = Mock()
        extraction_router.vision_extractor.extract = Mock()
        
        result = extraction_router.route(profile, "/path/to/doc.pdf")
        
        # Verify no escalation occurred
        assert result["strategy_used"] == "fast_text"
        assert result["routing_metadata"]["escalated"] == False
        assert result["routing_metadata"]["average_confidence"] == 0.935  # (0.95 + 0.92) / 2
        
        # Verify only fast extractor was called
        extraction_router.fast_extractor.extract.assert_called_once()
        extraction_router.layout_extractor.extract.assert_not_called()
        extraction_router.vision_extractor.extract.assert_not_called()
    
    def test_low_confidence_fast_text_escalation(self, extraction_router):
        """Test low confidence fast text extraction with escalation to layout."""
        # Create mock profile
        profile = Mock()
        profile.recommended_strategy = ExtractionStrategy.FAST_TEXT
        profile.category = DocumentCategory.MODERATE_COMPLEXITY
        profile.origin_type = OriginType.NATIVE_DIGITAL
        
        # Mock low confidence fast text pages
        fast_pages = [
            {
                "page_num": 1,
                "text": "Poor quality text with formatting issues.",
                "text_length": 40,
                "confidence": 0.65,  # Below threshold
                "extraction_method": "fast_text",
                "tables": []
            }
        ]
        
        # Mock higher confidence layout pages
        layout_pages = [
            {
                "page_num": 1,
                "text": "Better structured text with layout analysis.",
                "text_length": 45,
                "confidence": 0.85,  # Above threshold
                "extraction_method": "layout",
                "escalated_from": "fast_text",
                "tables": []
            }
        ]
        
        # Mock extractors
        extraction_router.fast_extractor.extract = Mock(return_value={"pages": fast_pages})
        extraction_router.layout_extractor.extract = Mock(return_value={"pages": layout_pages})
        extraction_router.vision_extractor.extract = Mock()
        
        result = extraction_router.route(profile, "/path/to/doc.pdf")
        
        # Verify escalation occurred
        assert result["strategy_used"] == "layout"
        assert result["routing_metadata"]["escalated"] == True
        assert result["routing_metadata"]["average_confidence"] == 0.85
        
        # Verify both extractors were called
        extraction_router.fast_extractor.extract.assert_called_once()
        extraction_router.layout_extractor.extract.assert_called_once()
        extraction_router.vision_extractor.extract.assert_not_called()
    
    def test_vision_extraction_for_scanned_document(self, extraction_router):
        """Test vision extraction for scanned document."""
        # Create mock profile for scanned document
        profile = Mock()
        profile.recommended_strategy = ExtractionStrategy.VISION
        profile.category = DocumentCategory.HIGH_COMPLEXITY
        profile.origin_type = OriginType.SCANNED_IMAGE
        
        # Mock vision extraction pages
        pages = [
            {
                "page_num": 1,
                "text": "OCR extracted text from scanned document.",
                "text_length": 40,
                "confidence": 0.88,
                "extraction_method": "vision",
                "page_metadata": {
                    "image_path": "/path/to/page_1.png",
                    "ocr_path": "/path/to/page_1.txt",
                    "word_count": 8,
                    "line_count": 1
                }
            }
        ]
        
        # Mock vision extractor
        extraction_router.vision_extractor.extract = Mock(return_value={"pages": pages})
        extraction_router.fast_extractor.extract = Mock()
        extraction_router.layout_extractor.extract = Mock()
        
        result = extraction_router.route(profile, "/path/to/doc.pdf")
        
        # Verify vision extraction was used
        assert result["strategy_used"] == "vision"
        assert result["routing_metadata"]["escalated"] == False
        assert result["routing_metadata"]["average_confidence"] == 0.88
        
        # Verify only vision extractor was called
        extraction_router.vision_extractor.extract.assert_called_once()
        extraction_router.fast_extractor.extract.assert_not_called()
        extraction_router.layout_extractor.extract.assert_not_called()
    
    def test_confidence_calculation(self, extraction_router):
        """Test confidence score calculation across pages."""
        # Test pages with varying confidence scores
        pages = [
            {"page_num": 1, "confidence": 0.95},
            {"page_num": 2, "confidence": 0.85},
            {"page_num": 3, "confidence": 0.75},
            {"page_num": 4, "confidence": 0.90}
        ]
        
        avg_confidence = extraction_router._calculate_average_confidence(pages)
        expected = (0.95 + 0.85 + 0.75 + 0.90) / 4
        assert abs(avg_confidence - expected) < 0.001
    
    def test_escalation_decision_logic(self, extraction_router):
        """Test escalation decision logic."""
        # Test no escalation needed
        pages = [
            {"page_num": 1, "confidence": 0.85},
            {"page_num": 2, "confidence": 0.90}
        ]
        
        should_escalate = extraction_router._should_escalate("fast_text", pages, 0.7)
        assert should_escalate == False
        
        # Test escalation needed
        pages = [
            {"page_num": 1, "confidence": 0.65},
            {"page_num": 2, "confidence": 0.60}
        ]
        
        should_escalate = extraction_router._should_escalate("fast_text", pages, 0.7)
        assert should_escalate == True
    
    def test_strategy_selection_by_document_type(self, extraction_router):
        """Test strategy selection based on document characteristics."""
        # Test native digital document
        digital_profile = Mock()
        digital_profile.origin_type = OriginType.NATIVE_DIGITAL
        digital_profile.category = DocumentCategory.SIMPLE_TEXT
        digital_profile.avg_chars_per_page = 800
        
        strategy = extraction_router._select_initial_strategy(digital_profile)
        assert strategy in ["fast_text", "layout"]
        
        # Test scanned document
        scanned_profile = Mock()
        scanned_profile.origin_type = OriginType.SCANNED_IMAGE
        scanned_profile.category = DocumentCategory.HIGH_COMPLEXITY
        scanned_profile.avg_chars_per_page = 50
        
        strategy = extraction_router._select_initial_strategy(scanned_profile)
        assert strategy == "vision"
        
        # Test mixed document
        mixed_profile = Mock()
        mixed_profile.origin_type = OriginType.MIXED
        mixed_profile.category = DocumentCategory.HIGH_COMPLEXITY
        mixed_profile.scanned_page_ratio = 0.7
        
        strategy = extraction_router._select_initial_strategy(mixed_profile)
        assert strategy == "vision"
    
    def test_cost_estimation(self, extraction_router):
        """Test cost estimation for different strategies."""
        # Test fast text cost
        cost = extraction_router._estimate_cost("fast_text", 10, 5.0)
        assert cost["total_cost"] > 0
        assert cost["strategy"] == "fast_text"
        assert cost["pages"] == 10
        
        # Test layout cost (should be higher)
        layout_cost = extraction_router._estimate_cost("layout", 10, 5.0)
        assert layout_cost["total_cost"] > cost["total_cost"]
        
        # Test vision cost (should be highest)
        vision_cost = extraction_router._estimate_cost("vision", 10, 5.0)
        assert vision_cost["total_cost"] > layout_cost["total_cost"]
    
    def test_quality_metrics_calculation(self, extraction_router):
        """Test quality metrics calculation."""
        pages = [
            {
                "page_num": 1,
                "text_length": 150,
                "confidence": 0.95,
                "tables": 1
            },
            {
                "page_num": 2,
                "text_length": 200,
                "confidence": 0.85,
                "tables": 0
            }
        ]
        
        metrics = extraction_router._calculate_quality_metrics(pages)
        
        assert metrics["total_text_length"] == 350
        assert metrics["average_confidence"] == 0.9
        assert metrics["total_tables"] == 1
        assert metrics["pages_processed"] == 2
    
    def test_error_handling_in_extraction(self, extraction_router):
        """Test error handling during extraction."""
        profile = Mock()
        profile.recommended_strategy = ExtractionStrategy.FAST_TEXT
        
        # Mock extractor failure
        extraction_router.fast_extractor.extract = Mock(side_effect=Exception("Extraction failed"))
        
        result = extraction_router.route(profile, "/path/to/doc.pdf")
        
        # Should handle error gracefully
        assert "error" in result
        assert result["strategy_used"] == "fast_text"
        assert result["pages"] == []
    
    def test_page_level_confidence_evaluation(self, extraction_router):
        """Test page-level confidence evaluation."""
        pages = [
            {"page_num": 1, "confidence": 0.95, "text_length": 100},
            {"page_num": 2, "confidence": 0.60, "text_length": 50},  # Low confidence
            {"page_num": 3, "confidence": 0.85, "text_length": 80}
        ]
        
        # Test confidence threshold filtering
        high_confidence_pages = extraction_router._filter_by_confidence(pages, 0.7)
        assert len(high_confidence_pages) == 2
        assert all(page["confidence"] >= 0.7 for page in high_confidence_pages)
        
        # Test confidence statistics
        stats = extraction_router._get_confidence_stats(pages)
        assert stats["average"] == (0.95 + 0.60 + 0.85) / 3
        assert stats["min"] == 0.60
        assert stats["max"] == 0.95
        assert stats["below_threshold"] == 1
