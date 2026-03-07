from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List

from src.models.ldu import LDU, LDURole, LDUType


@dataclass(frozen=True)
class ChunkingRules:
    # Rule 1: minimum chunk length for semantic usefulness.
    min_chars: int = 200
    # Rule 2: hard cap chunk size for retrieval efficiency.
    max_chars: int = 1200
    # Rule 3: keep chunk structure bounded.
    max_paragraphs: int = 6
    # Rule 4: keep contextual continuity between adjacent chunks.
    overlap_words: int = 30
    # Rule 5: preserve table-heavy page chunks as page-contained units.
    preserve_table_pages: bool = True


@dataclass(frozen=True)
class ValidationIssue:
    rule_name: str
    ldu_id: str
    message: str


@dataclass
class ValidationReport:
    passed: bool
    issues: List[ValidationIssue] = field(default_factory=list)
    checked_chunks: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "passed": self.passed,
            "checked_chunks": self.checked_chunks,
            "issues": [
                {"rule_name": i.rule_name, "ldu_id": i.ldu_id, "message": i.message}
                for i in self.issues
            ],
        }


class ChunkValidator:
    """Validate that emitted chunks satisfy all chunking rules."""

    def __init__(self, rules: ChunkingRules | None = None) -> None:
        self.rules = rules or ChunkingRules()

    def validate_ldus(self, ldus: List[LDU]) -> None:
        report = self.validate_with_report(ldus)
        if not report.passed:
            first = report.issues[0]
            raise ValueError(f"Chunk validation failed [{first.rule_name}] {first.ldu_id}: {first.message}")

    def validate_with_report(self, ldus: List[LDU]) -> ValidationReport:
        issues: List[ValidationIssue] = []
        prev_ldu: LDU | None = None

        for ldu in ldus:
            text = ldu.text.strip()
            is_table_chunk = ldu.ldu_type == LDUType.TABLE

            # Rule 1: size constraints.
            if not text:
                issues.append(ValidationIssue("size_constraints", ldu.ldu_id, "empty text"))
            if not is_table_chunk and len(ldu.text) < self.rules.min_chars:
                issues.append(
                    ValidationIssue(
                        "size_constraints",
                        ldu.ldu_id,
                        f"text_length={len(ldu.text)} below min {self.rules.min_chars}",
                    )
                )
            if len(ldu.text) > self.rules.max_chars + 200:
                issues.append(
                    ValidationIssue(
                        "size_constraints",
                        ldu.ldu_id,
                        f"text_length={len(ldu.text)} exceeds hard max {self.rules.max_chars + 200}",
                    )
                )

            # Rule 2: semantic completeness (heuristic).
            if not is_table_chunk and text:
                bad_start = bool(re.match(r"^[,.;:)\"]", text))
                bad_end = text.endswith(("-", "/", "(", "\\"))
                if bad_start:
                    issues.append(
                        ValidationIssue(
                            "semantic_completeness",
                            ldu.ldu_id,
                            "chunk appears to start with punctuation fragment",
                        )
                    )
                if bad_end:
                    issues.append(
                        ValidationIssue(
                            "semantic_completeness",
                            ldu.ldu_id,
                            "chunk appears to end with truncation marker",
                        )
                    )

            # Rule 3: hierarchical respect.
            paragraph_count = len([p for p in ldu.text.split("\n\n") if p.strip()])
            if not is_table_chunk and paragraph_count > self.rules.max_paragraphs:
                issues.append(
                    ValidationIssue(
                        "hierarchical_respect",
                        ldu.ldu_id,
                        f"paragraph_count={paragraph_count} exceeds {self.rules.max_paragraphs}",
                    )
                )
            if ldu.parent_section is None:
                issues.append(ValidationIssue("hierarchical_respect", ldu.ldu_id, "missing parent_section"))

            # Rule 4: overlap boundaries / ordering continuity.
            if prev_ldu is not None:
                if ldu.position_in_document <= prev_ldu.position_in_document:
                    issues.append(
                        ValidationIssue(
                            "overlap_boundaries",
                            ldu.ldu_id,
                            "position_in_document is not strictly increasing",
                        )
                    )
                if ldu.page_num < prev_ldu.page_num:
                    issues.append(
                        ValidationIssue(
                            "overlap_boundaries",
                            ldu.ldu_id,
                            "page_num regressed compared to previous chunk",
                        )
                    )
            prev_ldu = ldu

            # Rule 5: content integrity.
            expected_hash = hashlib.sha256(ldu.text.encode("utf-8")).hexdigest()
            if ldu.content_hash != expected_hash:
                issues.append(ValidationIssue("content_integrity", ldu.ldu_id, "content_hash mismatch"))

        return ValidationReport(
            passed=len(issues) == 0,
            issues=issues,
            checked_chunks=len(ldus),
        )


class ChunkingEngine:
    """Create validated LDUs from extraction page payloads."""

    def __init__(self, rules: ChunkingRules | None = None) -> None:
        self.rules = rules or ChunkingRules()
        self.validator = ChunkValidator(self.rules)
        self.last_validation_report: ValidationReport | None = None

    @property
    def hard_max_chars(self) -> int:
        return self.rules.max_chars + 200

    def build_ldus(self, document_id: str, pages: List[Dict[str, Any]], strategy_used: str) -> List[LDU]:
        ldus: List[LDU] = []
        position = 0

        for page in pages:
            page_num = int(page.get("page_num", 1))
            tables = page.get("tables", []) or []
            confidence = float(page.get("confidence", 0.0))
            text = (page.get("text", "") or "").strip()

            if not text:
                continue

            # Rule 5: table-heavy pages stay page-contained to protect structure.
            if self.rules.preserve_table_pages and tables:
                table_chunks = [text] if len(text) <= self.hard_max_chars else self._hard_split(text)
                for chunk in table_chunks:
                    if not chunk.strip():
                        continue
                    ldu = self._make_ldu(
                        document_id=document_id,
                        text=chunk,
                        page_num=page_num,
                        confidence=confidence,
                        strategy_used=strategy_used,
                        position=position,
                        ldu_type=LDUType.TABLE,
                    )
                    ldus.append(ldu)
                    position += 1
                continue

            chunks = self._chunk_text(text)
            for chunk in chunks:
                if len(chunk.strip()) < self.rules.min_chars:
                    # Drop residual tiny non-table fragments (headers/footers/noise).
                    continue
                ldu = self._make_ldu(
                    document_id=document_id,
                    text=chunk,
                    page_num=page_num,
                    confidence=confidence,
                    strategy_used=strategy_used,
                    position=position,
                    ldu_type=LDUType.PARAGRAPH,
                )
                ldus.append(ldu)
                position += 1

        self.last_validation_report = self.validator.validate_with_report(ldus)
        if not self.last_validation_report.passed:
            first = self.last_validation_report.issues[0]
            raise ValueError(f"Chunk validation failed [{first.rule_name}] {first.ldu_id}: {first.message}")
        return ldus

    def _chunk_text(self, text: str) -> List[str]:
        paragraphs = [p.strip() for p in text.split("\n\n") if p.strip()]
        if not paragraphs:
            return []

        chunks: List[str] = []
        current_parts: List[str] = []
        current_len = 0

        for para in paragraphs:
            para_len = len(para)
            next_para_count = len(current_parts) + 1
            if (
                current_parts
                and (current_len + para_len > self.rules.max_chars or next_para_count > self.rules.max_paragraphs)
            ):
                chunks.append("\n\n".join(current_parts))
                current_parts = [self._overlap_prefix(chunks[-1]), para]
                current_len = sum(len(part) for part in current_parts)
            else:
                current_parts.append(para)
                current_len += para_len

        if current_parts:
            chunks.append("\n\n".join(current_parts))

        normalized: List[str] = []
        for chunk in chunks:
            if len(chunk) <= self.rules.max_chars:
                normalized.append(chunk)
            else:
                normalized.extend(self._hard_split(chunk))

        merged = self._merge_short_chunks([c for c in normalized if c.strip()])
        return [c for c in merged if c.strip()]

    def _merge_short_chunks(self, chunks: List[str]) -> List[str]:
        """Merge undersized chunks with adjacent chunks to satisfy min length."""
        if not chunks:
            return []

        merged: List[str] = []
        i = 0
        while i < len(chunks):
            current = chunks[i]
            if len(current) >= self.rules.min_chars or i == len(chunks) - 1:
                merged.append(current)
                i += 1
                continue

            candidate = f"{current}\n\n{chunks[i + 1]}".strip()
            if self._can_merge_candidate(candidate):
                merged.append(candidate)
                i += 2
            else:
                merged.append(current)
                i += 1

        # Backward pass: if the final chunk is still short, append to previous.
        if len(merged) >= 2 and len(merged[-1]) < self.rules.min_chars:
            candidate = f"{merged[-2]}\n\n{merged[-1]}".strip()
            if self._can_merge_candidate(candidate):
                merged[-2] = candidate
                merged.pop()

        return merged

    def _can_merge_candidate(self, text: str) -> bool:
        if len(text) > self.hard_max_chars:
            return False
        paragraph_count = len([p for p in text.split("\n\n") if p.strip()])
        return paragraph_count <= self.rules.max_paragraphs

    def _hard_split(self, text: str) -> List[str]:
        sentences = [s.strip() for s in re.split(r"(?<=[.!?])\s+", text) if s.strip()]
        if not sentences:
            return [text]

        chunks: List[str] = []
        current: List[str] = []
        current_len = 0
        for sentence in sentences:
            # Break extremely long sentence fragments by words so we always respect max_chars.
            if len(sentence) > self.rules.max_chars:
                words = sentence.split()
                long_parts: List[str] = []
                current_part: List[str] = []
                current_part_len = 0
                for word in words:
                    word_len = len(word) + (1 if current_part else 0)
                    if current_part and current_part_len + word_len > self.rules.max_chars:
                        long_parts.append(" ".join(current_part).strip())
                        current_part = [word]
                        current_part_len = len(word)
                    else:
                        current_part.append(word)
                        current_part_len += word_len
                if current_part:
                    long_parts.append(" ".join(current_part).strip())

                for part in long_parts:
                    if current and current_len + len(part) > self.hard_max_chars:
                        chunks.append(" ".join(current).strip())
                        current = [part]
                        current_len = len(part)
                    else:
                        current.append(part)
                        current_len += len(part)
                continue

            if current and current_len + len(sentence) > self.hard_max_chars:
                chunks.append(" ".join(current).strip())
                current = [sentence]
                current_len = len(sentence)
            else:
                current.append(sentence)
                current_len += len(sentence)

        if current:
            chunks.append(" ".join(current).strip())
        finalized: List[str] = []
        for chunk in chunks:
            if len(chunk) <= self.hard_max_chars:
                finalized.append(chunk)
                continue

            words = chunk.split()
            current_words: List[str] = []
            current_len = 0
            for word in words:
                word_len = len(word) + (1 if current_words else 0)
                if current_words and current_len + word_len > self.hard_max_chars:
                    finalized.append(" ".join(current_words).strip())
                    current_words = [word]
                    current_len = len(word)
                else:
                    current_words.append(word)
                    current_len += word_len
            if current_words:
                finalized.append(" ".join(current_words).strip())
        return finalized

    def _overlap_prefix(self, previous_chunk: str) -> str:
        words = previous_chunk.split()
        if not words:
            return ""
        return " ".join(words[-self.rules.overlap_words :])

    def _make_ldu(
        self,
        document_id: str,
        text: str,
        page_num: int,
        confidence: float,
        strategy_used: str,
        position: int,
        ldu_type: LDUType,
    ) -> LDU:
        content_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()
        return LDU(
            ldu_id=f"{document_id}_ldu_{position + 1}",
            document_id=document_id,
            ldu_type=ldu_type,
            role=LDURole.CONTENT,
            text=text,
            text_length=len(text),
            content_hash=content_hash,
            confidence=confidence,
            page_num=page_num,
            page_refs=[page_num],
            position_in_document=position,
            parent_section=f"{document_id}_section_root",
            extraction_method=strategy_used,
        )
