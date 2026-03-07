from __future__ import annotations

import html
import re
from dataclasses import dataclass, field
from typing import Dict, List

from src.models.ldu import LDU
from src.pageindex.llm_client import GeminiSummarizer


@dataclass
class SectionNode:
    section_id: str
    title: str
    page_refs: List[int] = field(default_factory=list)
    ldu_ids: List[str] = field(default_factory=list)
    children: List["SectionNode"] = field(default_factory=list)
    summary: str = ""

    def to_dict(self) -> Dict:
        return {
            "section_id": self.section_id,
            "title": self.title,
            "page_refs": sorted(set(self.page_refs)),
            "ldu_ids": self.ldu_ids,
            "summary": self.summary,
            "children": [child.to_dict() for child in self.children],
        }


class PageIndexTreeBuilder:
    """Build simple section tree from chunk stream and add summaries."""

    def __init__(self) -> None:
        self.summarizer = GeminiSummarizer()

    def build(self, document_id: str, ldus: List[LDU]) -> SectionNode:
        root = SectionNode(section_id=f"{document_id}_root", title="Document Root")
        current = root
        section_idx = 0

        for ldu in ldus:
            title = self._detect_section_title(ldu.text)
            if title:
                section_idx += 1
                current = SectionNode(
                    section_id=f"{document_id}_section_{section_idx}",
                    title=title,
                )
                root.children.append(current)

            current.ldu_ids.append(ldu.ldu_id)
            current.page_refs.extend(ldu.page_refs)

        if not root.children:
            fallback = SectionNode(
                section_id=f"{document_id}_section_0",
                title="Document Body",
                page_refs=list(root.page_refs),
                ldu_ids=list(root.ldu_ids),
            )
            root.children.append(fallback)

        self._promote_document_title(root, document_id)
        self._rebalance_known_financial_sections(root)
        self._summarize_tree(root, ldu_map={ldu.ldu_id: ldu for ldu in ldus})
        return root

    def _promote_document_title(self, root: SectionNode, document_id: str) -> None:
        if not root.children:
            root.title = self._humanize_document_id(document_id)
            return

        first_child = root.children[0]
        if self._looks_like_document_title(first_child.title, document_id):
            root.title = first_child.title
            root.children = root.children[1:]
        else:
            root.title = self._humanize_document_id(document_id)

    def _humanize_document_id(self, document_id: str) -> str:
        cleaned = document_id.replace("_", " ").replace("-", " ").strip()
        cleaned = re.sub(r"\s+", " ", cleaned)
        return cleaned or "Document"

    def _looks_like_document_title(self, title: str, document_id: str) -> bool:
        normalized_title = self._normalize_title(title)
        normalized_doc = self._normalize_title(self._humanize_document_id(document_id))
        if normalized_title == normalized_doc:
            return True
        if normalized_title and normalized_title in normalized_doc:
            return True
        title_lower = title.lower()
        return any(token in title_lower for token in ["report", "statement", "bulletin", "review", "plan"])

    def _rebalance_known_financial_sections(self, root: SectionNode) -> None:
        if not root.children:
            return

        grouped: Dict[str, SectionNode] = {}
        reordered: List[SectionNode] = []

        def ensure_parent(title: str) -> SectionNode:
            if title not in grouped:
                node = SectionNode(section_id=f"{root.section_id}_{len(grouped) + 1}", title=title)
                grouped[title] = node
                reordered.append(node)
            return grouped[title]

        for child in root.children:
            title_lower = child.title.lower()
            parent_title = None
            if "chairman" in title_lower or "president" in title_lower or "message" in title_lower:
                parent_title = "Chairman's Message"
            elif any(term in title_lower for term in ["profit or loss", "income statement", "financial position", "balance sheet", "cash flow"]):
                parent_title = "Financial Performance"
            elif "risk" in title_lower:
                parent_title = "Risk Management"
            elif any(term in title_lower for term in ["governance", "board", "corporate governance"]):
                parent_title = "Corporate Governance"

            if not parent_title:
                reordered.append(child)
                continue

            parent = ensure_parent(parent_title)
            parent.page_refs.extend(child.page_refs)
            parent.ldu_ids.extend(child.ldu_ids)
            if self._normalize_title(child.title) != self._normalize_title(parent_title):
                parent.children.append(child)

        root.children = reordered

    def _detect_section_title(self, text: str) -> str | None:
        first_line = text.strip().splitlines()[0] if text.strip() else ""
        if not first_line:
            return None

        # Normalize markdown/header noise from Docling markdown output.
        cleaned = html.unescape(first_line)
        cleaned = re.sub(r"^\s*#+\s*", "", cleaned)  # markdown heading
        cleaned = re.sub(r"^\s*[-*]\s+", "", cleaned)  # bullets
        cleaned = re.sub(r"\s+", " ", cleaned).strip(" :-")
        if not cleaned:
            return None

        lower = cleaned.lower()
        if lower in {"content", "statistical bulletin"}:
            return None

        # Numbered headings: "1. Summary", "3: 12-Month ...", "2) ..."
        if re.match(r"^\d+([.:)\-]\s*|\s+)", cleaned):
            return cleaned[:120]

        # Section keyword headings.
        if re.match(r"^(chapter|section|appendix|glossary)\b", lower):
            return cleaned[:120]

        # Upper/title case short headings.
        words = cleaned.split()
        alpha = re.sub(r"[^A-Za-z]", "", cleaned)
        if 2 <= len(words) <= 14:
            if alpha and cleaned == cleaned.upper():
                return cleaned[:120]
            titleish_ratio = sum(1 for w in words if w[:1].isupper()) / max(1, len(words))
            if titleish_ratio >= 0.7 and len(cleaned) <= 110:
                return cleaned[:120]

        if len(words) <= 12 and cleaned.endswith(":"):
            return cleaned[:120]

        return None

    def _summarize_tree(self, root: SectionNode, ldu_map: Dict[str, LDU]) -> None:
        for node in root.children:
            sample_text = "\n\n".join(
                ldu_map[ldu_id].text for ldu_id in node.ldu_ids[:4] if ldu_id in ldu_map
            )
            node.summary = self.summarizer.summarize(sample_text)

    def _normalize_title(self, text: str) -> str:
        text = text.lower().strip()
        text = re.sub(r"^section\s+\d+\s*[:.\-]?\s*", "", text)
        text = re.sub(r"^\d+\s*[:.\-]?\s*", "", text)
        text = re.sub(r"[^a-z0-9]+", " ", text)
        return re.sub(r"\s+", " ", text).strip()
