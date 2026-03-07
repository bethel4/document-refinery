from __future__ import annotations

import json
import hashlib
from pathlib import Path
from typing import Dict, List

from src.models.ldu import LDU
from src.pageindex.tree_builder import PageIndexTreeBuilder


class PageIndexerAgent:
    """Build and persist PageIndex section trees with LLM summaries."""

    def __init__(self, out_dir: str = ".refinery/pageindex") -> None:
        self.builder = PageIndexTreeBuilder()
        self.out_dir = Path(out_dir)
        self.out_dir.mkdir(parents=True, exist_ok=True)

    def run(self, document_id: str, ldus: List[LDU]) -> Dict:
        root = self.builder.build(document_id=document_id, ldus=ldus)
        tree = self._serialize_tree(document_id=document_id, ldus=ldus, tree=root.to_dict())
        out_path = self.out_dir / f"{document_id}_pageindex.json"
        out_path.write_text(json.dumps(tree, ensure_ascii=False, indent=2), encoding="utf-8")
        return tree

    def _serialize_tree(self, document_id: str, ldus: List[LDU], tree: Dict) -> Dict:
        section_lookup: Dict[str, Dict] = {}
        ldu_to_section: Dict[str, Dict] = {}

        for section in tree.get("children", []):
            section_title = section.get("title", "Untitled Section")
            section_content = " ".join(
                ldu.text for ldu in ldus if ldu.ldu_id in set(section.get("ldu_ids", []))
            )
            normalized = {
                "section_id": section.get("section_id"),
                "title": section_title,
                "section_title": section_title,
                "summary": section.get("summary", ""),
                "page_refs": sorted(set(section.get("page_refs", []))),
                "ldu_ids": list(section.get("ldu_ids", [])),
                "content_hash": hashlib.sha256(section_content.encode("utf-8")).hexdigest() if section_content else "",
                "child_nodes": section.get("children", []),
                "children": section.get("children", []),
            }
            section_lookup[normalized["section_id"]] = normalized
            for ldu_id in normalized["ldu_ids"]:
                ldu_to_section[ldu_id] = normalized

        pages: List[Dict] = []
        for page_num in sorted({ldu.page_num for ldu in ldus}):
            page_ldus = [ldu for ldu in ldus if ldu.page_num == page_num]
            if not page_ldus:
                continue
            first_section = ldu_to_section.get(page_ldus[0].ldu_id, {})
            page_text = "\n\n".join(ldu.text for ldu in page_ldus)
            pages.append(
                {
                    "page_number": page_num,
                    "section_title": first_section.get("section_title", "Document Root"),
                    "section_id": first_section.get("section_id"),
                    "content_hash": hashlib.sha256(page_text.encode("utf-8")).hexdigest(),
                    "provenance": [f"{document_id}_page_{page_num}"],
                    "ldus": [
                        {
                            "ldu_id": ldu.ldu_id,
                            "page_number": ldu.page_num,
                            "section_title": ldu_to_section.get(ldu.ldu_id, {}).get("section_title", "Document Root"),
                            "text": ldu.text,
                            "content_hash": ldu.content_hash,
                            "provenance": [f"{document_id}_page_{ldu.page_num}", ldu.ldu_id],
                            "bbox": (ldu.bbox or ldu.bounding_box).model_dump()
                            if (ldu.bbox or ldu.bounding_box)
                            else None,
                        }
                        for ldu in page_ldus
                    ],
                }
            )

        return {
            "document_id": document_id,
            "title": document_id,
            "pages": pages,
            "sections": list(section_lookup.values()),
            "children": list(section_lookup.values()),
        }
