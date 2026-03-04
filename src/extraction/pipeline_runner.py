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
        router_config = extraction_config or {}
        self.router = ExtractionRouter(
            confidence_threshold=confidence_threshold,
            fast_text_config=router_config.get("fast_text"),
            layout_config=router_config.get("layout"),
            vision_config=router_config.get("vision")
        )
        
        logger.info(f"ExtractionPipeline initialized: {max_workers} workers, confidence_threshold={confidence_threshold}")
    
    def process_document(self, pdf_path: Path) -> Dict[str, Any]:
        """
        Process a single document: Phase 1 (triage) → Phase 2 (extraction).
        
        Args:
            pdf_path: Path to PDF file
            
        Returns:
            Complete processing result with triage and extraction data
        """
        doc_id = pdf_path.stem
        logger.info(f"🔄 Processing {pdf_path.name}")
        start_time = time.time()
        
        try:
            # Phase 1: Document triage and characterization
            logger.info(f"[Phase1] Starting triage for {doc_id}")
            triage_start = time.time()
            
            profile = self.classifier.classify_document(str(pdf_path))
            triage_duration = time.time() - triage_start
            
            # Check triage success
            if not hasattr(profile, 'document_id'):
                raise ValueError(f"Triage failed for {doc_id}")
            
            logger.info(f"[Phase1] Triage completed: {profile.origin_type} → {profile.recommended_strategy} ({triage_duration:.2f}s)")
            
            # Phase 2: Strategy-based extraction
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
            
            # Check extraction success
            if "error" in extraction_result:
                raise ValueError(f"Extraction failed: {extraction_result['error']}")
            
            logger.info(f"[Phase2] Extraction completed: {extraction_result['strategy_used']} ({extraction_duration:.2f}s)")
            
            # Create comprehensive result
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
            
            # Save extraction result
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
        """
        Process all PDFs in folder using parallel ThreadPool.
        
        Args:
            pdf_folder: Path to PDF folder
            
        Returns:
            List of processing results
        """
        pdf_path = Path(pdf_folder)
        pdf_files = list(pdf_path.glob("*.pdf"))
        
        if not pdf_files:
            logger.warning(f"No PDF files found in {pdf_folder}")
            return []
        
        logger.info(f"🚀 Starting batch processing: {len(pdf_files)} documents")
        start_time = time.time()
        
        results = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all documents for parallel processing
            futures = [executor.submit(self.process_document, pdf) for pdf in pdf_files]
            
            # Collect results as they complete
            for future in as_completed(futures):
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    logger.error(f"Document processing failed: {e}")
        
        # Print comprehensive summary
        self._print_batch_summary(results, time.time() - start_time)
        return results
    
    def _save_extraction_result(self, doc_id: str, result: Dict[str, Any]):
        """Save extraction result to JSON file."""
        try:
            output_file = self.extractions_dir / f"{doc_id}_extraction.json"
            
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(result, f, indent=2, ensure_ascii=False)
            
            # Also save to extraction ledger
            self._save_to_ledger(doc_id, result)
            
        except Exception as e:
            logger.error(f"Failed to save extraction result: {e}")
    
    def _save_to_ledger(self, doc_id: str, result: Dict[str, Any]):
        """Save extraction entry to ledger file."""
        try:
            ledger_file = self.logs_dir / "extraction_ledger.jsonl"
            
            # Create ledger entry
            ledger_entry = {
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "document_id": doc_id,
                "filename": result.get("filename", ""),
                "origin_type": result.get("triage", {}).get("profile", {}).get("origin_type", ""),
                "category": result.get("triage", {}).get("profile", {}).get("category", ""),
                "recommended_strategy": result.get("triage", {}).get("profile", {}).get("recommended_strategy", ""),
                "actual_strategy": result.get("extraction", {}).get("strategy_used", ""),
                "escalated": result.get("extraction", {}).get("routing_metadata", {}).get("escalated", False),
                "confidence": result.get("extraction", {}).get("routing_metadata", {}).get("average_confidence", 0),
                "pages_processed": len(result.get("extraction", {}).get("pages", [])),
                "total_duration": result.get("total_duration", 0),
                "extraction_cost": result.get("extraction", {}).get("extraction_metadata", {}).get("extraction_cost", "unknown"),
                "status": "success" if "error" not in result else "failed"
            }
            
            # Append to ledger
            with open(ledger_file, 'a', encoding='utf-8') as f:
                f.write(json.dumps(ledger_entry) + '\n')
                
        except Exception as e:
            logger.error(f"Failed to save to ledger: {e}")
    
    def _print_batch_summary(self, results: List[Dict[str, Any]], total_time: float):
        """Print comprehensive batch processing summary."""
        print("\n" + "="*80)
        print("📊 EXTRACTION PIPELINE SUMMARY")
        print("="*80)
        
        successful = [r for r in results if "error" not in r]
        failed = [r for r in results if "error" in r]
        
        print(f"\n✅ Successfully processed: {len(successful)} documents")
        print(f"❌ Failed: {len(failed)} documents")
        print(f"⏱️  Total time: {total_time:.2f}s")
        
        if successful:
            # Strategy distribution
            strategy_counts = {}
            escalation_counts = {"escalated": 0, "not_escalated": 0}
            
            for result in successful:
                strategy = result["extraction"]["strategy_used"]
                strategy_counts[strategy] = strategy_counts.get(strategy, 0) + 1
                
                routing_meta = result["extraction"].get("routing_metadata", {})
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
            
            # Performance metrics
            avg_duration = sum(r["total_duration"] for r in successful) / len(successful)
            total_pages = sum(r["extraction"]["pages_processed"] for r in successful)
            
            print(f"\n⚡ Performance Metrics:")
            print(f"   Average duration: {avg_duration:.2f}s")
            print(f"   Total pages processed: {total_pages}")
            print(f"   Overall throughput: {total_pages/total_time:.1f} pages/sec")
            
            # Detailed results table
            print(f"\n📄 Processing Details:")
            print(f"{'Filename':<35} {'Strategy':<12} {'Escalated':<10} {'Confidence':<10} {'Pages':<6} {'Duration':<8}")
            print("-" * 85)
            
            for result in successful:
                triage_profile = result["triage"]["profile"]
                extraction_meta = result["extraction"]["routing_metadata"]
                
                print(f"{result['filename']:<35} {result['extraction']['strategy_used']:<12} "
                      f"{str(extraction_meta.get('escalated', False)):<10} "
                      f"{extraction_meta.get('average_confidence', 0):<10.2f} "
                      f"{result['extraction']['pages_processed']:<6} "
                      f"{result['total_duration']:<8.2f}")
        
        if failed:
            print(f"\n❌ Failed Documents:")
            for result in failed:
                print(f"   {result['filename']}: {result['error']}")
        
        print(f"\n📁 Output Locations:")
        print(f"   Profiles: {self.profiles_dir}")
        print(f"   Extractions: {self.extractions_dir}")
        print(f"   Ledger: {self.logs_dir}/extraction_ledger.jsonl")
        print("="*80)

def main():
    """Main entry point for extraction pipeline."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Parallel Document Extraction Pipeline")
    parser.add_argument("--pdf-folder", default="data/raw", help="PDF folder path")
    parser.add_argument("--workers", type=int, default=4, help="Number of parallel workers")
    parser.add_argument("--confidence-threshold", type=float, default=0.7, help="Confidence threshold for escalation")
    parser.add_argument("--single", help="Process single PDF file")
    
    args = parser.parse_args()
    
    # Initialize pipeline
    pipeline = ExtractionPipeline(
        max_workers=args.workers,
        confidence_threshold=args.confidence_threshold
    )
    
    # Process documents
    if args.single:
        # Process single document
        pdf_path = Path(args.single)
        if pdf_path.exists():
            result = pipeline.process_document(pdf_path)
            print(f"\nSingle document result:")
            print(json.dumps(result, indent=2))
        else:
            print(f"Error: File not found: {args.single}")
        return [result] if pdf_path.exists() else []
    else:
        # Process batch
        results = pipeline.process_batch(args.pdf_folder)
    
    return results

if __name__ == "__main__":
    main()
