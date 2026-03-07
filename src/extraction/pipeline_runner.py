"""
Parallel Pipeline Runner - Integrates triage and extraction with ThreadPool execution.
Phase 1: Document triage and characterization
Phase 2: Strategy-based extraction with confidence-gated escalation
"""

import sys
import logging
import time
import json
import hashlib
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List
from concurrent.futures import ThreadPoolExecutor, as_completed

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.domain_analysis.triage.document_classifier import TriageClassifier
from src.agents.chunker import SemanticChunkerAgent
from src.agents.indexer import PageIndexerAgent
from src.agents.query_agent import QueryAgent
from src.data_layer.fact_table_extractor import FactTableExtractor
from src.extraction.extraction_router import ExtractionRouter
from src.models.extracted_document import ExtractedDocument, PerformanceMetrics
from src.models.ldu import BoundingBox, LDU, LDURole, LDUType
from src.models.page_index import ContentType, PageFeatures, PageIndex, PageNavigation, PageQuality, PageType
from src.models.provenance_chain import (
    AgentInfo,
    AgentType,
    ProcessingMetrics,
    ProcessingStatus,
    ProcessingStep,
    ProcessingStepType,
    ProvenanceChain,
    ProvenanceCitation,
)
from src.query.pageindex_query import PageIndexQuery, precision_at_k
from src.query.vector_store import VectorStoreIngestor

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ExtractionPipeline:
    """Production-ready extraction pipeline with parallel processing."""
    
    def __init__(self, 
                 max_workers: int = 4,
                 confidence_threshold: float = 0.7,
                 extract_only: bool = False,
                 forced_strategy: str | None = None,
                 extraction_config: Dict[str, Any] = None):
        """
        Initialize extraction pipeline.
        
        Args:
            max_workers: Number of parallel workers
            confidence_threshold: Minimum confidence to avoid escalation
            extraction_config: Configuration for extractors
        """
        self.max_workers = max_workers
        self.confidence_threshold = confidence_threshold
        self.extract_only = bool(extract_only)
        self.forced_strategy = forced_strategy
        
        # Initialize components
        self.rules_file = Path(".refinery/rules/extraction_rules.yaml")
        self.profiles_dir = Path(".refinery/profiles")
        self.extractions_dir = Path(".refinery/extractions")
        self.logs_dir = Path(".refinery/extraction_logs")
        self.pageindex_dir = Path(".refinery/pageindex")
        # Write ledger entries into the extraction_logs directory.
        self.ledger_file = self.logs_dir / "extraction_ledger.jsonl"

        # Create directories
        for directory in [self.profiles_dir, self.extractions_dir, self.logs_dir, self.pageindex_dir]:
            directory.mkdir(parents=True, exist_ok=True)
        self.ledger_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Initialize triage classifier
        self.classifier = TriageClassifier(
            rules_file=str(self.rules_file),
            profiles_dir=str(self.profiles_dir)
        )
        
        # Initialize extraction router
        self.router = ExtractionRouter(max_workers=max_workers)
        self.chunker_agent = SemanticChunkerAgent()
        self.indexer_agent = PageIndexerAgent(out_dir=str(self.pageindex_dir))
        self.query_agent = QueryAgent()
        self.fact_extractor = FactTableExtractor()
        self.pageindex_query = PageIndexQuery()
        self.vector_store_ingestor = VectorStoreIngestor()
        
        # Set confidence threshold on the router.
        if hasattr(self.router, "conf_threshold"):
            self.router.conf_threshold = confidence_threshold
        
        logger.info(f"ExtractionPipeline initialized: {max_workers} workers, confidence_threshold={confidence_threshold}")
    
    def process_document(self, pdf_path: Path) -> Dict[str, Any]:
        """Process a single document: Phase 1 → Phase 2."""
        doc_id = pdf_path.stem
        logger.info(f"🔄 Processing {pdf_path.name}")
        start_time = time.time()
        
        try:
            # Phase 1: Document triage
            logger.info(f"[Phase1] Starting triage for {doc_id}")
            triage_start = time.time()
            profile = self.classifier.classify_document(str(pdf_path))
            triage_duration = time.time() - triage_start
            
            if not hasattr(profile, 'document_id'):
                raise ValueError(f"Triage failed for {doc_id}")
            
            logger.info(f"[Phase1] Triage completed: {profile.origin_type} → {profile.recommended_strategy} ({triage_duration:.2f}s)")
            
            # Phase 2: Extraction
            logger.info(f"[Phase2] Starting extraction for {doc_id}")
            extraction_start = time.time()
            
            profile_dict = {
                "document_id": profile.document_id,
                "filename": profile.filename,
                "file_path": str(pdf_path),
                "origin_type": profile.origin_type,
                "layout_complexity": getattr(profile, "layout_complexity", None),
                "category": profile.category,
                "recommended_strategy": profile.recommended_strategy,
                "force_strategy": None,
                "estimated_cost": profile.estimated_extraction_cost,
                "total_pages": profile.total_pages,
                "avg_chars_per_page": profile.avg_chars_per_page,
                "image_area_ratio": profile.image_area_ratio,
                "scanned_page_ratio": getattr(profile, 'scanned_page_ratio', None),
                "digital_page_ratio": getattr(profile, 'digital_page_ratio', None)
            }

            # Allow CLI to force the initial strategy for debugging.
            if hasattr(self, "forced_strategy") and self.forced_strategy:
                profile_dict["force_strategy"] = self.forced_strategy
            
            extraction_result = self.router.route(str(pdf_path), profile_dict)
            self._normalize_extraction_pages(extraction_result)
            self._normalize_extraction_metadata(extraction_result)
            extraction_duration = time.time() - extraction_start
            
            if "error" in extraction_result:
                raise ValueError(f"Extraction failed: {extraction_result['error']}")
            
            logger.info(f"[Phase2] Extraction completed: {extraction_result['strategy_used']} ({extraction_duration:.2f}s)")
            
            total_duration = time.time() - start_time

            triage_payload = {
                "duration": triage_duration,
                "profile": {
                    "document_id": profile.document_id,
                    "origin_type": profile.origin_type,
                    "category": profile.category,
                    "recommended_strategy": profile.recommended_strategy,
                    "confidence": profile.category_confidence,
                    "pages": profile.total_pages,
                    "file_size": profile.file_size_bytes,
                },
            }
            extraction_payload = {
                "duration": extraction_duration,
                "strategy_used": extraction_result["strategy_used"],
                "routing_metadata": extraction_result.get("routing_metadata", {}),
                "pages": extraction_result.get("pages", []),
                "extraction_metadata": extraction_result.get("extraction_metadata", {}),
                "document_elements": extraction_result.get("document_elements", {}),
            }

            # Fast path for debugging extraction: skip chunking/indexing/query stages.
            if self.extract_only:
                minimal = {
                    "document_id": doc_id,
                    "filename": pdf_path.name,
                    "file_path": str(pdf_path),
                    "total_duration": total_duration,
                    "triage": triage_payload,
                    "extraction": extraction_payload,
                }
                self._save_extraction_result(doc_id, minimal)
                logger.info(f"✅ Completed (extract-only) {pdf_path.name} in {total_duration:.2f}s")
                return minimal
            performance_payload = PerformanceMetrics(
                triage_speed=f"{profile.total_pages/triage_duration:.1f} pages/sec",
                extraction_speed=f"{len(extraction_result.get('pages', []))/extraction_duration:.1f} pages/sec",
                overall_efficiency="high" if total_duration < 30 else "medium" if total_duration < 60 else "low",
            )

            extracted_doc = ExtractedDocument(
                document_id=doc_id,
                filename=pdf_path.name,
                file_path=str(pdf_path),
                total_duration=total_duration,
                triage=triage_payload,
                extraction=extraction_payload,
                performance=performance_payload,
            )

            ldus = self.chunker_agent.run(
                document_id=doc_id,
                pages=extraction_result.get("pages", []),
                strategy_used=extracted_doc.get_strategy_used(),
            )
            page_index_nodes = self._build_page_index(doc_id, extraction_result.get("pages", []), extracted_doc.get_strategy_used())
            page_index_tree = self.indexer_agent.run(doc_id, ldus)
            topic_query = f"{profile.domain_hint} {profile.category}"
            top_sections = self.pageindex_query.top_k_sections(page_index_tree, topic_query, k=3)
            retrieval_metrics = self._measure_retrieval_precision(page_index_tree, topic_query, top_sections)
            vector_store_info = self.vector_store_ingestor.ingest(doc_id, ldus)
            fact_rows = self.fact_extractor.ingest_ldus(doc_id, ldus)
            audit_mode = self.query_agent.audit_claim(
                claim=f"{profile.domain_hint} key metrics and figures",
                document_id=doc_id,
                pageindex_tree=page_index_tree,
                document_name=pdf_path.name,
            )
            provenance_chain = self._build_provenance_chain(doc_id, profile.document_id, ldus, triage_duration, extraction_duration)

            result = extracted_doc.model_dump(mode="json")
            result["ldus"] = [ldu.model_dump(mode="json") for ldu in ldus]
            result["page_index"] = [page.model_dump(mode="json") for page in page_index_nodes]
            result["page_index_tree"] = page_index_tree
            result["page_index_top_sections"] = top_sections
            result["retrieval_metrics"] = retrieval_metrics
            result["vector_store"] = vector_store_info
            result["fact_table"] = {"sqlite_path": str(self.fact_extractor.db_path), "rows_inserted": fact_rows}
            result["audit_mode"] = audit_mode
            result["provenance_chain"] = provenance_chain.model_dump(mode="json")
            
            self._save_extraction_result(doc_id, result)
            logger.info(f"✅ Completed {pdf_path.name} in {total_duration:.2f}s")
            return result
            
        except Exception as e:
            logger.error(f"Document processing failed: {e}")
            return {
                "document_id": doc_id,
                "filename": pdf_path.name,
                "error": str(e),
                "total_duration": time.time() - start_time
            }

    def _normalize_extraction_pages(self, extraction_result: Dict[str, Any]) -> None:
        """Repair extractor payloads so they conform to the typed extraction schema."""
        for page_idx, page in enumerate(extraction_result.get("pages", []), start=1):
            page_num = int(page.get("page_num", page.get("page_number", page_idx)))
            page["page_num"] = page_num
            normalized_tables = []
            for table_idx, table in enumerate(page.get("tables", []) or [], start=1):
                if not isinstance(table, dict):
                    continue
                fixed = dict(table)
                fixed["table_id"] = str(fixed.get("table_id", f"table_{page_num}_{table_idx}"))
                fixed["page_num"] = int(fixed.get("page_num") or page_num)
                fixed["rows"] = int(fixed.get("rows", len(fixed.get("data", []) or [])))
                fixed["columns"] = int(fixed.get("columns", len(fixed.get("headers", []) or [])))
                fixed["headers"] = [str(v) for v in (fixed.get("headers") or [])]
                fixed["data"] = [
                    [str(cell) for cell in row]
                    for row in (fixed.get("data") or [])
                    if isinstance(row, list)
                ]
                fixed["confidence"] = float(fixed.get("confidence", 0.6))
                normalized_tables.append(fixed)
            page["tables"] = normalized_tables

    def _normalize_extraction_metadata(self, extraction_result: Dict[str, Any]) -> None:
        metadata = extraction_result.setdefault("extraction_metadata", {})
        pages = extraction_result.get("pages", [])
        total_tables = sum(len(page.get("tables", []) or []) for page in pages)
        metadata.setdefault("total_pages", len(pages))
        metadata.setdefault("total_text_length", sum(len(page.get("text", "") or "") for page in pages))
        metadata.setdefault("total_tables", total_tables)
        metadata.setdefault("average_confidence", extraction_result.get("routing_metadata", {}).get("average_confidence", 0.0))
        metadata.setdefault("extraction_cost", "medium")
        metadata.setdefault("processing_time_seconds", 0.0)
        metadata.setdefault("pages_processed", len(pages))
        metadata.setdefault("confidence_threshold", self.confidence_threshold)

    def _build_ldus(self, document_id: str, pages: List[Dict[str, Any]], strategy_used: str) -> List[LDU]:
        """Build validated logical document units from extracted pages."""
        ldus: List[LDU] = []
        for idx, page in enumerate(pages):
            text = page.get("text", "") or ""
            if not text.strip():
                continue

            page_num = int(page.get("page_num", idx + 1))
            bbox_data = page.get("bbox") or page.get("bounding_box")
            bbox = None
            if isinstance(bbox_data, dict):
                bbox_payload = {
                    "x": bbox_data.get("x", bbox_data.get("left", 0.0)),
                    "y": bbox_data.get("y", bbox_data.get("top", 0.0)),
                    "width": bbox_data.get("width", 1.0),
                    "height": bbox_data.get("height", 1.0),
                    "page_num": page_num,
                }
                bbox = BoundingBox(**bbox_payload)

            ldu = LDU(
                ldu_id=f"{document_id}_ldu_{idx+1}",
                document_id=document_id,
                ldu_type=LDUType.PARAGRAPH,
                role=LDURole.CONTENT,
                text=text,
                text_length=len(text),
                content_hash=hashlib.sha256(text.encode("utf-8")).hexdigest(),
                confidence=float(page.get("confidence", 0.0)),
                page_num=page_num,
                page_refs=[page_num],
                bounding_box=bbox,
                position_in_document=idx,
                parent_section=f"{document_id}_section_root",
                extraction_method=strategy_used,
            )
            ldus.append(ldu)
        return ldus

    def _measure_retrieval_precision(self, page_index_tree: Dict[str, Any], topic_query: str, top_sections: List[Dict[str, Any]]) -> Dict[str, float]:
        """Measure precision@3 with and without PageIndex traversal using proxy relevance."""
        sections = page_index_tree.get("children", [])
        if not sections:
            return {
                "precision_with_pageindex_p3": 0.0,
                "precision_without_pageindex_p3": 0.0,
            }

        topic_terms = {token for token in topic_query.lower().split() if token}
        scored = []
        for section in sections:
            text = f"{section.get('title', '')} {section.get('summary', '')}".lower()
            overlap = sum(1 for token in topic_terms if token in text)
            scored.append((overlap, section))
        scored.sort(key=lambda x: x[0], reverse=True)

        relevant_section_ids = [sec.get("section_id") for score, sec in scored if score > 0][:3]
        with_pageindex_ids = [sec.get("section_id") for sec in top_sections]
        without_pageindex_ids = [sec.get("section_id") for _, sec in scored[:3]]

        return {
            "precision_with_pageindex_p3": precision_at_k(with_pageindex_ids, relevant_section_ids, k=3),
            "precision_without_pageindex_p3": precision_at_k(without_pageindex_ids, relevant_section_ids, k=3),
        }

    def _build_page_index(self, document_id: str, pages: List[Dict[str, Any]], strategy_used: str) -> List[PageIndex]:
        """Build validated page index entries from extracted pages."""
        page_index_nodes: List[PageIndex] = []
        for idx, page in enumerate(pages):
            text = page.get("text", "") or ""
            page_num = int(page.get("page_num", idx + 1))
            table_count = len(page.get("tables", []) or [])
            confidence = float(page.get("confidence", 0.0))

            page_type = PageType.CONTENT
            content_type = ContentType.TABLE_HEAVY if table_count > 0 else ContentType.TEXT_ONLY

            words = len(text.split())
            lines = text.count("\n") + 1 if text else 0
            paragraphs = len([p for p in text.split("\n\n") if p.strip()]) if text else 0

            node = PageIndex(
                page_id=f"{document_id}_page_{page_num}",
                document_id=document_id,
                page_num=page_num,
                page_type=page_type,
                content_type=content_type,
                primary_language="unknown",
                language_confidence=0.5,
                title=None,
                summary=text[:200] if text else None,
                keywords=[],
                features=PageFeatures(
                    word_count=words,
                    line_count=lines,
                    paragraph_count=paragraphs,
                    table_count=table_count,
                    figure_count=0,
                    image_count=0,
                    font_count=0,
                    column_count=1,
                    text_density=min(1.0, words / 1000.0),
                    image_coverage=0.0,
                ),
                quality=PageQuality(
                    ocr_confidence=confidence,
                    text_clarity=confidence,
                    layout_quality=confidence,
                    noise_level=max(0.0, 1.0 - confidence),
                ),
                navigation=PageNavigation(page_number=page_num, outline_level=0),
                extraction_method=strategy_used,
                parent_section=f"{document_id}_section_root",
                children=[],
            )
            page_index_nodes.append(node)
        return page_index_nodes

    def _build_provenance_chain(
        self,
        chain_id_prefix: str,
        source_document_id: str,
        ldus: List[LDU],
        triage_duration: float,
        extraction_duration: float,
    ) -> ProvenanceChain:
        """Build a provenance chain capturing triage and extraction lineage."""
        all_pages = sorted({page for ldu in ldus for page in ldu.page_refs})
        combined_hash = hashlib.sha256("".join(ldu.content_hash for ldu in ldus).encode("utf-8")).hexdigest() if ldus else None

        citations = [
            ProvenanceCitation(
                citation_id=f"{chain_id_prefix}_citation_{idx+1}",
                page_refs=ldu.page_refs,
                bbox=ldu.bounding_box,
                content_hash=ldu.content_hash,
                source_artifact=source_document_id,
            )
            for idx, ldu in enumerate(ldus)
        ]

        agent = AgentInfo(
            agent_id="document_refinery_pipeline",
            agent_type=AgentType.AUTOMATED,
            agent_name="ExtractionPipeline",
            version="1.0",
        )

        triage_step = ProcessingStep(
            step_id=f"{chain_id_prefix}_triage",
            step_type=ProcessingStepType.TRIAGE,
            step_name="document_triage",
            sequence_order=1,
            agent=agent,
            started_at=datetime.now(),
            completed_at=datetime.now(),
            status=ProcessingStatus.COMPLETED,
            metrics=ProcessingMetrics(duration_seconds=triage_duration, pages_processed=len(all_pages)),
            page_refs=all_pages,
        )
        extraction_step = ProcessingStep(
            step_id=f"{chain_id_prefix}_extraction",
            step_type=ProcessingStepType.EXTRACTION,
            step_name="content_extraction",
            sequence_order=2,
            agent=agent,
            started_at=datetime.now(),
            completed_at=datetime.now(),
            status=ProcessingStatus.COMPLETED,
            metrics=ProcessingMetrics(duration_seconds=extraction_duration, pages_processed=len(all_pages)),
            page_refs=all_pages,
            citations=citations,
        )

        chain = ProvenanceChain(
            chain_id=f"{chain_id_prefix}_provenance",
            document_id=source_document_id,
            created_by="ExtractionPipeline",
            content_hash=combined_hash,
            page_refs=all_pages,
            citations=citations,
            status=ProcessingStatus.COMPLETED if ldus else ProcessingStatus.PENDING,
        )
        chain.add_step(triage_step)
        chain.add_step(extraction_step)
        return chain
    
    def process_batch(self, pdf_folder: str = "data/raw") -> List[Dict[str, Any]]:
        """Process all PDFs in folder using parallel ThreadPool."""
        pdf_path = Path(pdf_folder)
        pdf_files = list(pdf_path.glob("*.pdf"))
        
        if not pdf_files:
            logger.warning(f"No PDF files found in {pdf_folder}")
            return []
        
        logger.info(f"🚀 Starting batch processing: {len(pdf_files)} documents")
        start_time = time.time()
        results = []
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(self.process_document, pdf) for pdf in pdf_files]
            for future in as_completed(futures):
                try:
                    results.append(future.result())
                except Exception as e:
                    logger.error(f"Document processing failed: {e}")
        
        self._print_batch_summary(results, time.time() - start_time)
        return results
    
    def _save_extraction_result(self, doc_id: str, result: Dict[str, Any]):
        try:
            output_file = self.extractions_dir / f"{doc_id}_extraction.json"
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(result, f, indent=2, ensure_ascii=False)
            self._save_to_ledger(doc_id, result)
        except Exception as e:
            logger.error(f"Failed to save extraction result: {e}")
    
    def _save_to_ledger(self, doc_id: str, result: Dict[str, Any]):
        """Save extraction entry to ledger with cost estimation."""
        try:
            # Extract key metrics for ledger
            triage_profile = result.get("triage", {}).get("profile", {})
            extraction_meta = result.get("extraction", {})
            routing_meta = extraction_meta.get("routing_metadata", {})
            
            # Calculate cost estimate based on strategy and pages
            strategy_used = extraction_meta.get("strategy_used", "unknown")
            pages_processed = routing_meta.get("pages_processed", 0)
            confidence_score = routing_meta.get("average_confidence", 0.0)
            processing_time = result.get("total_duration", 0.0)

            # Cost estimation logic (based on your picture)
            cost_estimate = self._calculate_cost_estimate(strategy_used, pages_processed, processing_time)

            # Create comprehensive ledger entry
            ledger_entry = {
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "document_id": doc_id,
                "filename": result.get("filename", "unknown"),
                "origin_type": triage_profile.get("origin_type", "unknown"),
                "category": triage_profile.get("category", "unknown"),
                "strategy_used": strategy_used,
                "confidence_score": round(confidence_score, 3),
                "pages_processed": pages_processed,
                "processing_time": round(processing_time, 2),
                "cost_estimate": cost_estimate,
                "escalated": routing_meta.get("escalated", False),
                "status": "success" if "error" not in result else "failed"
            }

            # Append to ledger
            with open(self.ledger_file, 'a', encoding='utf-8') as f:
                f.write(json.dumps(ledger_entry) + '\n')
                
        except Exception as e:
            logger.error(f"Failed to save to ledger: {e}")
    
    def _calculate_cost_estimate(self, strategy: str, pages: int, processing_time: float) -> Dict[str, Any]:
        """Calculate cost estimate based on strategy and processing metrics."""
        # Local extractors (docling/layout, tesseract vision, native text) have no external API cost.
        local_free_strategies = {"fast_text", "layout", "vision", "hybrid"}
        if strategy in local_free_strategies:
            return 0.0
        return 0.0
    
    def _print_batch_summary(self, results: List[Dict[str, Any]], total_time: float):
        print("\n" + "="*80)
        print("📊 EXTRACTION PIPELINE SUMMARY")
        print("="*80)
        successful = [r for r in results if "error" not in r]
        failed = [r for r in results if "error" in r]
        print(f"\n✅ Successfully processed: {len(successful)} documents")
        print(f"❌ Failed: {len(failed)} documents")
        print(f"⏱️  Total time: {total_time:.2f}s")
        
        if successful:
            strategy_counts = {}
            escalation_counts = {"escalated": 0, "not_escalated": 0}
            for r in successful:
                strategy = r["extraction"]["strategy_used"]
                strategy_counts[strategy] = strategy_counts.get(strategy, 0) + 1
                routing_meta = r["extraction"].get("routing_metadata", {})
                if routing_meta.get("escalated", False):
                    escalation_counts["escalated"] += 1
                else:
                    escalation_counts["not_escalated"] += 1
            
            print(f"\n📋 Strategy Distribution:")
            for strategy, count in strategy_counts.items():
                print(f"   {strategy}: {count} documents")
            print(f"\n🔄 Escalation Summary:")
            print(f"   Escalated: {escalation_counts['escalated']} documents")
            print(f"   Not escalated: {escalation_counts['not_escalated']} documents")
        
        if failed:
            print(f"\n❌ Failed Documents:")
            for r in failed:
                print(f"   {r['filename']}: {r['error']}")
        
        print(f"\n📁 Output Locations:")
        print(f"   Profiles: {self.profiles_dir}")
        print(f"   Extractions: {self.extractions_dir}")
        print(f"   Ledger: {self.ledger_file}")
        print("="*80)

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Parallel Document Extraction Pipeline")
    parser.add_argument("--pdf-folder", default="data/raw", help="PDF folder path")
    parser.add_argument("--workers", type=int, default=3, help="Number of parallel workers")
    parser.add_argument("--confidence-threshold", type=float, default=0.7, help="Confidence threshold for escalation")
    parser.add_argument("--extract-only", action="store_true", help="Run only triage + extraction (skip chunking/indexing/query)")
    parser.add_argument(
        "--force-strategy",
        choices=["fast_text", "layout", "vision", "hybrid"],
        help="Force the initial extraction strategy (debug/testing)",
    )
    parser.add_argument("--single", help="Process single PDF file")
    parser.add_argument("--dpi", type=int, default=150, help="DPI for vision extraction")
    parser.add_argument("--max-vision-pages", type=int, default=5, help="Max pages to process in vision escalation")
    parser.add_argument("--torch-threads", type=int, default=2, help="Torch thread limit")
    args = parser.parse_args()
    
    try:
        import torch  # type: ignore

        torch.set_num_threads(args.torch_threads)
        print(f"🔧 Torch threads limited to {args.torch_threads}")
    except Exception as exc:
        print(f"ℹ️  Torch not available (or failed to import): {exc}. Skipping torch thread limits.")
    
    # Initialize pipeline
    extraction_config = {}  # populate if needed
    pipeline = ExtractionPipeline(
        max_workers=args.workers,
        confidence_threshold=args.confidence_threshold,
        extract_only=args.extract_only,
        forced_strategy=args.force_strategy,
        extraction_config=extraction_config
    )
    
    # Update vision extractor if present
    if hasattr(pipeline.router, 'vision_extractor'):
        pipeline.router.vision_extractor.dpi = args.dpi
        pipeline.router.vision_extractor.max_vision_pages = args.max_vision_pages
        print(f"🔧 Vision extractor configured: DPI={args.dpi}, max_pages={args.max_vision_pages}")
    
    print(f"🚀 Starting pipeline with {args.workers} workers")
    
    # Single or batch processing
    if args.single:
        pdf_path = Path(args.single)
        if pdf_path.exists():
            result = pipeline.process_document(pdf_path)
            print(json.dumps(result, indent=2))
        else:
            print(f"Error: File not found: {args.single}")
            return []
        return [result]
    else:
        return pipeline.process_batch(args.pdf_folder)

if __name__ == "__main__":
    main()
