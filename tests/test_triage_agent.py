"""Unit tests for Stage 1 triage classification."""

from __future__ import annotations

from pathlib import Path

import pytest

from src.domain_analysis.triage.document_classifier import TriageClassifier
from src.domain_analysis.triage.domain_classifier import DomainClassifier


@pytest.fixture
def triage_classifier(tmp_path: Path) -> TriageClassifier:
    rules_file = tmp_path / "rules.yaml"
    profiles_dir = tmp_path / "profiles"
    rules_file.write_text(
        """
document_categories:
  high_complexity:
    confidence_threshold: 0.6
    criteria:
      avg_chars_per_page_min: 1200
      image_area_ratio_min: 0.3
      detected_table_count_min: 3
      x_cluster_count_min: 3
    recommended_strategy: vision
  moderate_complexity:
    confidence_threshold: 0.7
    criteria:
      avg_chars_per_page_min: 600
      avg_chars_per_page_max: 2000
      image_area_ratio_max: 0.3
      detected_table_count_max: 3
      x_cluster_count_max: 3
    recommended_strategy: layout
  simple_text:
    confidence_threshold: 0.8
    criteria:
      avg_chars_per_page_max: 600
      image_area_ratio_max: 0.1
      detected_table_count_max: 1
      x_cluster_count_max: 2
    recommended_strategy: fast_text
""".strip(),
        encoding="utf-8",
    )
    return TriageClassifier(rules_file=str(rules_file), profiles_dir=str(profiles_dir))


def test_origin_type_digital_vs_scanned_vs_mixed(triage_classifier: TriageClassifier) -> None:
    digital_metrics = {
        "total_pages": 4,
        "avg_chars_per_page": 1800,
        "image_area_ratio": 0.05,
        "fonts": ["Arial"],
        "page_metrics": [
            {"page_num": 1, "chars": 1500, "image_area_ratio": 0.01, "is_searchable": True},
            {"page_num": 2, "chars": 1700, "image_area_ratio": 0.02, "is_searchable": True},
            {"page_num": 3, "chars": 1600, "image_area_ratio": 0.01, "is_searchable": True},
            {"page_num": 4, "chars": 1800, "image_area_ratio": 0.03, "is_searchable": True},
        ],
    }
    assert triage_classifier._classify_origin_type(digital_metrics) == "native_digital"

    scanned_metrics = {
        "total_pages": 3,
        "avg_chars_per_page": 20,
        "image_area_ratio": 0.9,
        "fonts": [],
        "page_metrics": [
            {"page_num": 1, "chars": 10, "image_area_ratio": 0.98, "is_searchable": False},
            {"page_num": 2, "chars": 8, "image_area_ratio": 0.95, "is_searchable": False},
            {"page_num": 3, "chars": 11, "image_area_ratio": 0.97, "is_searchable": False},
        ],
    }
    assert triage_classifier._classify_origin_type(scanned_metrics) == "scanned_image"

    mixed_metrics = {
        "total_pages": 4,
        "avg_chars_per_page": 700,
        "image_area_ratio": 0.45,
        "fonts": ["Arial"],
        "page_metrics": [
            {"page_num": 1, "chars": 1500, "image_area_ratio": 0.05, "is_searchable": True},
            {"page_num": 2, "chars": 1200, "image_area_ratio": 0.06, "is_searchable": True},
            {"page_num": 3, "chars": 15, "image_area_ratio": 0.9, "is_searchable": False},
            {"page_num": 4, "chars": 20, "image_area_ratio": 0.88, "is_searchable": False},
        ],
    }
    assert triage_classifier._classify_origin_type(mixed_metrics) == "mixed"


def test_layout_complexity_heuristics(triage_classifier: TriageClassifier) -> None:
    assert triage_classifier._classify_layout_complexity({"column_count": 1, "table_count": 0, "image_area_ratio": 0.05}) == "single_column"
    assert triage_classifier._classify_layout_complexity({"column_count": 3, "table_count": 0, "image_area_ratio": 0.05}) == "multi_column"
    assert triage_classifier._classify_layout_complexity({"column_count": 2, "table_count": 6, "image_area_ratio": 0.05}) == "table_heavy"
    assert triage_classifier._classify_layout_complexity({"column_count": 1, "table_count": 0, "image_area_ratio": 0.5}) == "figure_heavy"


def test_domain_hint_is_pluggable(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    class StubDomainClassifier(DomainClassifier):
        def classify(self, text: str) -> tuple[str, float]:
            return "medical", 0.99

    rules_file = tmp_path / "rules.yaml"
    profiles_dir = tmp_path / "profiles"
    rules_file.write_text("document_categories: {}", encoding="utf-8")
    classifier = TriageClassifier(
        rules_file=str(rules_file),
        profiles_dir=str(profiles_dir),
        domain_classifier=StubDomainClassifier(),
    )

    pdf_path = tmp_path / "doc.pdf"
    pdf_path.write_bytes(b"%PDF-1.4\n%EOF\n")

    monkeypatch.setattr(
        classifier,
        "_extract_lightweight_metrics",
        lambda _: {
            "total_pages": 2,
            "total_chars": 2000,
            "avg_chars_per_page": 1000,
            "image_area_ratio": 0.05,
            "table_count": 1,
            "column_count": 1,
            "fonts": ["Arial"],
            "is_searchable": True,
            "has_watermarks": False,
            "has_signatures": False,
            "file_size": 1000,
            "page_metrics": [
                {"page_num": 1, "chars": 1000, "image_area_ratio": 0.05, "is_searchable": True},
                {"page_num": 2, "chars": 1000, "image_area_ratio": 0.05, "is_searchable": True},
            ],
        },
    )
    monkeypatch.setattr(classifier, "_detect_language", lambda _: ("en", 0.9))
    monkeypatch.setattr(classifier, "_extract_sample_text", lambda *_args, **_kwargs: "patient diagnosis hospital")

    profile = classifier.classify_document(str(pdf_path))
    assert profile.domain_hint == "medical"
    assert profile.domain_confidence == pytest.approx(0.99)


class TestTriageAgent:
    """Compatibility wrapper for tests/__init__.py imports."""

    def test_origin_type_digital_vs_scanned_vs_mixed(self, triage_classifier: TriageClassifier) -> None:
        test_origin_type_digital_vs_scanned_vs_mixed(triage_classifier)

    def test_layout_complexity_heuristics(self, triage_classifier: TriageClassifier) -> None:
        test_layout_complexity_heuristics(triage_classifier)

    def test_domain_hint_is_pluggable(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        test_domain_hint_is_pluggable(tmp_path, monkeypatch)
