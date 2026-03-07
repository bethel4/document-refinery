from __future__ import annotations

import math
import re
from collections import Counter
from typing import Dict, List, Sequence, Tuple


def _tokenize(text: str) -> List[str]:
    return [t for t in re.findall(r"[a-zA-Z0-9]+", text.lower()) if len(t) > 2]


def _cosine(a: Counter, b: Counter) -> float:
    if not a or not b:
        return 0.0
    dot = sum(a[k] * b.get(k, 0) for k in a)
    norm_a = math.sqrt(sum(v * v for v in a.values()))
    norm_b = math.sqrt(sum(v * v for v in b.values()))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


class PageIndexQuery:
    """Topic traversal over PageIndex tree returning top-k sections."""

    def top_k_sections(self, tree: Dict, topic: str, k: int = 3) -> List[Dict]:
        query_vec = Counter(_tokenize(topic))
        scored: List[Tuple[float, Dict]] = []

        section_nodes = self._flatten_sections(tree.get("children") or tree.get("sections") or [])
        for section in section_nodes:
            text = f"{section.get('title', '')} {section.get('summary', '')}"
            score = _cosine(query_vec, Counter(_tokenize(text)))
            scored.append((score, section))

        scored.sort(key=lambda x: x[0], reverse=True)
        return [sec for _, sec in scored[:k]]

    def _flatten_sections(self, sections: List[Dict]) -> List[Dict]:
        out: List[Dict] = []
        for section in sections:
            out.append(section)
            children = section.get("children") or section.get("child_nodes") or []
            out.extend(self._flatten_sections(children))
        return out


def precision_at_k(predicted: Sequence[str], relevant: Sequence[str], k: int = 3) -> float:
    if k <= 0:
        return 0.0
    pred_k = list(predicted)[:k]
    if not pred_k:
        return 0.0
    rel = set(relevant)
    hits = sum(1 for item in pred_k if item in rel)
    return hits / len(pred_k)
