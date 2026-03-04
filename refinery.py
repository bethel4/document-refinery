"""
Production Document Refinery Pipeline - Complete implementation with proper architecture.
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
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.models.document_models import DocumentProfile, ExtractedDocument, ExtractionLedgerEntry
from src.agents.triage import TriageAgent
from src.agents.extractor import ExtractionAgent

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DocumentRefinery:
    """Production-ready document refinery with proper architecture."""
    
    def __init__(self, 
                 max_workers: int = 4,
                 confidence_threshold: float = 0.7,
                 config_file: str = "rubric/extraction_rules.yaml"):
        """
        Initialize document refinery.
        
        Args:
            max_workers: Number of parallel workers
            confidence_threshold: Minimum confidence to avoid escalation
            config_file: Path to extraction rules configuration
        """
        self.max_workers = max_workers
        self.confidence_threshold = confidence_threshold
        self.config_file = Path(config_file)
        
        # Initialize directories
        self.profiles_dir = Path(".refinery/profiles")
        self.extractions_dir = Path(".refinery/extractions")
        self.logs_dir = Path(".refinery/logs")
        
        for directory in [self.profiles_dir, self.extractions_dir, self.logs_dir]:
            directory.mkdir(parents=True, exist_ok=True)
        
        # Initialize agents
        self.triage_agent = TriageAgent()
        self.extraction_agent = ExtractionAgent(
            confidence_threshold=confidence_threshold
        )
        
        # Initialize ledger
        self.ledger_file = self.logs_dir / "extraction_ledger.jsonl"
        
        logger.info(f"DocumentRefinery initialized: {max_workers} workers, confidence_threshold={confidence_threshold}")
    
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
            
            profile = self.triage_agent.analyze_document(str(pdf_path))
            triage_duration = time.time() - triage_start
            
            logger.info(f"[Phase1] Triage completed: {profile.origin_type.value} → {profile.recommended_strategy.value} ({triage_duration:.2f}s)")
            
            # Phase 2: Strategy-based extraction
            logger.info(f"[Phase2] Starting extraction for {doc_id}")
            extraction_start = time.time()
            
            profile_dict = {
                "document_id": profile.document_id,
                "filename": profile.filename,
                "origin_type": profile.origin_type.value,
                "category": profile.category,
                "recommended_strategy": profile.recommended_strategy.value,
                "estimated_cost": profile.estimated_extraction_cost,
                "total_pages": profile.total_pages,
                "avg_chars_per_page": profile.avg_chars_per_page,
                "image_area_ratio": profile.image_area_ratio,
                "scanned_page_ratio": profile.scanned_page_ratio,
                "digital_page_ratio": profile.digital_page_ratio
            }
            
            extraction_result = self.extraction_agent.extract_document(str(pdf_path), profile_dict)
            extraction_duration = time.time() - extraction_start
            
            # Check extraction success
            if hasattr(extraction_result, 'extraction_metadata') and extraction_result.extraction_metadata.get("error"):
                raise ValueError(f"Extraction failed: {extraction_result.extraction_metadata['error']}")
            
            logger.info(f"[Phase2] Extraction completed: {extraction_result.strategy_used} ({extraction_duration:.2f}s)")
            
            # Create comprehensive result
            total_duration = time.time() - start_time
            
            result = {
                "document_id": doc_id,
                "filename": pdf_path.name,
                "file_path": str(pdf_path),
                "total_duration": total_duration,
                "triage": {
                    "duration": triage_duration,
                    "profile": profile.dict()
                },
                "extraction": extraction_result.dict(),
                "performance": {
                    "triage_speed": f"{profile.total_pages/triage_duration:.1f} pages/sec",
                    "extraction_speed": f"{len(extraction_result.pages)/extraction_duration:.1f} pages/sec",
                    "overall_efficiency": "high" if total_duration < 30 else "medium" if total_duration < 60 else "low"
                }
            }
            
            # Save results
            self._save_results(doc_id, profile, extraction_result, total_duration)
            
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
    
    def _save_results(self, doc_id: str, profile: DocumentProfile, 
                     extraction: ExtractedDocument, total_duration: float):
        """Save all processing results."""
        try:
            # Save profile
            profile_file = self.profiles_dir / f"{doc_id}.json"
            with open(profile_file, 'w', encoding='utf-8') as f:
                json.dump(profile.dict(), f, indent=2, ensure_ascii=False, default=str)
            
            # Save extraction
            extraction_file = self.extractions_dir / f"{doc_id}_extraction.json"
            with open(extraction_file, 'w', encoding='utf-8') as f:
                json.dump(extraction.dict(), f, indent=2, ensure_ascii=False, default=str)
            
            # Save to ledger
            self._save_to_ledger(profile, extraction, total_duration)
            
        except Exception as e:
            logger.error(f"Failed to save results: {e}")
    
    def _save_to_ledger(self, profile: DocumentProfile, extraction: ExtractedDocument, 
                        total_duration: float):
        """Save extraction entry to ledger file."""
        try:
            ledger_entry = self.extraction_agent.create_ledger_entry(
                profile.dict(), extraction, total_duration
            )
            
            # Append to ledger
            with open(self.ledger_file, 'a', encoding='utf-8') as f:
                f.write(json.dumps(ledger_entry.dict(), default=str) + '\n')
                
        except Exception as e:
            logger.error(f"Failed to save to ledger: {e}")
    
    def _print_batch_summary(self, results: List[Dict[str, Any]], total_time: float):
        """Print comprehensive batch processing summary."""
        print("\n" + "="*80)
        print("📊 DOCUMENT REFINERY SUMMARY")
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
            total_pages = sum(len(r["extraction"]["pages"]) for r in successful)
            
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
                extraction_meta = result["extraction"].get("routing_metadata", {})
                
                print(f"{result['filename']:<35} {result['extraction']['strategy_used']:<12} "
                      f"{str(extraction_meta.get('escalated', False)):<10} "
                      f"{extraction_meta.get('average_confidence', 0):<10.2f} "
                      f"{len(result['extraction']['pages']):<6} "
                      f"{result['total_duration']:<8.2f}")
        
        if failed:
            print(f"\n❌ Failed Documents:")
            for result in failed:
                print(f"   {result['filename']}: {result['error']}")
        
        print(f"\n📁 Output Locations:")
        print(f"   Profiles: {self.profiles_dir}")
        print(f"   Extractions: {self.extractions_dir}")
        print(f"   Ledger: {self.ledger_file}")
        print("="*80)

def main():
    """Main entry point for document refinery."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Production Document Refinery")
    parser.add_argument("--pdf-folder", default="data/raw", help="PDF folder path")
    parser.add_argument("--workers", type=int, default=4, help="Number of parallel workers")
    parser.add_argument("--confidence-threshold", type=float, default=0.7, help="Confidence threshold for escalation")
    parser.add_argument("--single", help="Process single PDF file")
    parser.add_argument("--config", default="rubric/extraction_rules.yaml", help="Extraction rules configuration")
    
    args = parser.parse_args()
    
    # Initialize refinery
    refinery = DocumentRefinery(
        max_workers=args.workers,
        confidence_threshold=args.confidence_threshold,
        config_file=args.config
    )
    
    # Process documents
    if args.single:
        # Process single document
        pdf_path = Path(args.single)
        if pdf_path.exists():
            result = refinery.process_document(pdf_path)
            print(f"\nSingle document result:")
            print(json.dumps(result, indent=2, default=str))
        else:
            print(f"Error: File not found: {args.single}")
        return [result] if pdf_path.exists() else []
    else:
        # Process batch
        results = refinery.process_batch(args.pdf_folder)
    
    return results

if __name__ == "__main__":
    main()
