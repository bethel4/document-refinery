"""Pluggable domain classification strategies for triage."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Dict, List, Tuple


class DomainClassifier(ABC):
    """Strategy interface for domain classification."""

    @abstractmethod
    def classify(self, text: str) -> Tuple[str, float]:
        """Return (domain_hint, confidence)."""


class KeywordDomainClassifier(DomainClassifier):
    """Keyword-based default classifier; lightweight and fast."""

    def __init__(self, domain_keywords: Dict[str, List[str]] | None = None) -> None:
        self.domain_keywords = domain_keywords or {
            "legal": ["contract", "agreement", "legal", "law", "court", "judge", "attorney"],
            "financial": ["invoice", "payment", "balance", "account", "bank", "financial", "tax", "revenue"],
            "medical": ["patient", "medical", "diagnosis", "treatment", "prescription", "hospital"],
            "technical": ["specification", "manual", "technical", "engineering", "diagram"],
            "business": ["report", "meeting", "proposal", "business", "company", "corporate"],
            "academic": ["research", "study", "university", "paper", "journal", "academic"],
            "government": ["government", "official", "permit", "license", "regulation"],
            "personal": ["letter", "personal", "family", "individual", "private"],
        }

    def classify(self, text: str) -> Tuple[str, float]:
        lowered = (text or "").lower()
        if not lowered.strip():
            return "general", 0.5

        domain_scores: Dict[str, int] = {}
        for domain, keywords in self.domain_keywords.items():
            domain_scores[domain] = sum(1 for keyword in keywords if keyword in lowered)

        best_domain = max(domain_scores, key=domain_scores.get)
        score = domain_scores[best_domain]
        confidence = min(score / 3.0, 1.0)
        if confidence <= 0.3:
            return "general", 0.5
        return best_domain, confidence

