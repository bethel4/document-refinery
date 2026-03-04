"""
Parallel Pipeline Runner - Integrates triage and extraction with ThreadPool execution.
Phase 1: Document triage and characterization
Phase 2: Strategy-based extraction with confidence-gated escalation
"""

import sys
import logging
import time
import json
from pathlib import Path
from typing import Dict, Any, List
from concurrent.futures import ThreadPoolExecutor, as_completed

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.domain_analysis.triage.document_classifier import TriageClassifier
from src.extraction.extraction_router import ExtractionRouter

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
        
        # Initialize components
        self.rules_file = Path(".refinery/rules/extraction_rules.yaml")
        self.profiles_dir = Path(".refinery/profiles")
        self.extractions_dir = Path(".refinery/extractions")
        self.logs_dir = Path(".refinery/extraction_logs")
        
        # Create directories
        for directory in [self.profiles_dir, self.extractions_dir, self.logs_dir]:
            directory.mkdir(parents=True, exist_ok=True)
        
        # Initialize triage classifier
        self.classifier = TriageClassifier(
            rules_file=str(self.rules_file),
            profiles_dir=str(self.profiles_dir)
        )
        
        # Initialize extraction router
        self.router = ExtractionRouter()
        
        # Set confidence_threshold if router has that attribute
        if hasattr(self.router, "confidence_threshold"):
            self.router.confidence_threshold = confidence_threshold
        
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
                "origin_type": profile.origin_type,
                "category": profile.category,
                "recommended_strategy": profile.recommended_strategy,
                "estimated_cost": profile.estimated_extraction_cost,
                "total_pages": profile.total_pages,
                "avg_chars_per_page": profile.avg_chars_per_page,
                "image_area_ratio": profile.image_area_ratio,
                "scanned_page_ratio": getattr(profile, 'scanned_page_ratio', None),
                "digital_page_ratio": getattr(profile, 'digital_page_ratio', None)
            }
            
            extraction_result = self.router.route(str(pdf_path), profile_dict)
            extraction_duration = time.time() - extraction_start
            
            if "error" in extraction_result:
                raise ValueError(f"Extraction failed: {extraction_result['error']}")
            
            logger.info(f"[Phase2] Extraction completed: {extraction_result['strategy_used']} ({extraction_duration:.2f}s)")
            
            total_duration = time.time() - start_time
            
            result = {
                "document_id": doc_id,
                "filename": pdf_path.name,
                "file_path": str(pdf_path),
                "total_duration": total_duration,
                "triage": {
                    "duration": triage_duration,
                    "profile": {
                        "document_id": profile.document_id,
                        "origin_type": profile.origin_type,
                        "category": profile.category,
                        "recommended_strategy": profile.recommended_strategy,
                        "confidence": profile.category_confidence,
                        "pages": profile.total_pages,
                        "file_size": profile.file_size_bytes
                    }
                },
                "extraction": {
                    "duration": extraction_duration,
                    "strategy_used": extraction_result["strategy_used"],
                    "routing_metadata": extraction_result.get("routing_metadata", {}),
                    "pages": extraction_result.get("pages", []),
                    "extraction_metadata": extraction_result.get("extraction_metadata", {}),
                    "document_elements": extraction_result.get("document_elements", {})
                },
                "performance": {
                    "triage_speed": f"{profile.total_pages/triage_duration:.1f} pages/sec",
                    "extraction_speed": f"{len(extraction_result.get('pages', []))/extraction_duration:.1f} pages/sec",
                    "overall_efficiency": "high" if total_duration < 30 else "medium" if total_duration < 60 else "low"
                }
            }
            
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
            ledger_file = self.logs_dir / "extraction_ledger.jsonl"
            
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
                "processing_time_seconds": round(processing_time, 2),
                "cost_estimate": cost_estimate,
                "escalated": routing_meta.get("escalated", False),
                "status": "success" if "error" not in result else "failed"
            }
            
            # Append to ledger
            with open(ledger_file, 'a', encoding='utf-8') as f:
                f.write(json.dumps(ledger_entry) + '\n')
                
        except Exception as e:
            logger.error(f"Failed to save to ledger: {e}")
    
    def _calculate_cost_estimate(self, strategy: str, pages: int, processing_time: float) -> Dict[str, Any]:
        """Calculate cost estimate based on strategy and processing metrics."""
        
        # Base costs per strategy (in credits/units)
        base_costs = {
            "fast_text": {"base": 1, "per_page": 0.1, "unit": "credits"},
            "layout": {"base": 5, "per_page": 0.5, "unit": "credits"}, 
            "vision": {"base": 10, "per_page": 2.0, "unit": "credits"}
        }
        
        # Get strategy cost info
        cost_info = base_costs.get(strategy, {"base": 10, "per_page": 1.0, "unit": "credits"})
        
        # Calculate total cost
        base_cost = cost_info["base"]
        page_cost = pages * cost_info["per_page"]
        total_cost = base_cost + page_cost
        
        # Time-based adjustment (processing efficiency)
        if processing_time > 0:
            pages_per_second = pages / processing_time
            if pages_per_second < 0.1:  # Very slow processing
                total_cost *= 1.5  # 50% penalty
            elif pages_per_second > 1.0:  # Very fast processing
                total_cost *= 0.8  # 20% discount
        
        return {
            "total_cost": round(total_cost, 2),
            "base_cost": base_cost,
            "page_cost": round(page_cost, 2),
            "pages": pages,
            "unit": cost_info["unit"],
            "processing_efficiency": round(pages / processing_time, 2) if processing_time > 0 else 0,
            "cost_breakdown": {
                "strategy": strategy,
                "base_rate": base_cost,
                "per_page_rate": cost_info["per_page"],
                "time_adjustment": "penalty" if processing_time > 0 and pages/processing_time < 0.1 else "discount" if processing_time > 0 and pages/processing_time > 1.0 else "none"
            }
        }
    
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
        print(f"   Ledger: {self.logs_dir}/extraction_ledger.jsonl")
        print("="*80)

def main():
    import argparse
    import torch
    
    parser = argparse.ArgumentParser(description="Parallel Document Extraction Pipeline")
    parser.add_argument("--pdf-folder", default="data/raw", help="PDF folder path")
    parser.add_argument("--workers", type=int, default=3, help="Number of parallel workers")
    parser.add_argument("--confidence-threshold", type=float, default=0.7, help="Confidence threshold for escalation")
    parser.add_argument("--single", help="Process single PDF file")
    parser.add_argument("--dpi", type=int, default=150, help="DPI for vision extraction")
    parser.add_argument("--max-vision-pages", type=int, default=5, help="Max pages to process in vision escalation")
    parser.add_argument("--torch-threads", type=int, default=2, help="Torch thread limit")
    args = parser.parse_args()
    
    torch.set_num_threads(args.torch_threads)
    print(f"🔧 Torch threads limited to {args.torch_threads}")
    
    # Initialize pipeline
    extraction_config = {}  # populate if needed
    pipeline = ExtractionPipeline(
        max_workers=args.workers,
        confidence_threshold=args.confidence_threshold,
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