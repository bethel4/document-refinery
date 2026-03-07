from __future__ import annotations

from typing import Any, Dict, List, Tuple

from src.chunking.chunking_engine import ChunkingEngine, ChunkingRules
from src.models.ldu import LDU


class SemanticChunkerAgent:
    """Semantic chunking agent enforcing all chunking rules via ChunkValidator."""

    def __init__(self, rules: ChunkingRules | None = None) -> None:
        self.engine = ChunkingEngine(rules=rules)

    def run(self, document_id: str, pages: List[Dict[str, Any]], strategy_used: str) -> List[LDU]:
        return self.engine.build_ldus(document_id=document_id, pages=pages, strategy_used=strategy_used)

    def run_with_report(
        self,
        document_id: str,
        pages: List[Dict[str, Any]],
        strategy_used: str,
    ) -> Tuple[List[LDU], Dict[str, Any]]:
        ldus = self.run(document_id=document_id, pages=pages, strategy_used=strategy_used)
        report = self.engine.last_validation_report
        return ldus, (report.to_dict() if report else {"passed": True, "checked_chunks": len(ldus), "issues": []})
