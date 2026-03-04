#!/usr/bin/env python3
"""
Parallel Document Refinery Pipeline - ThreadPool Architecture
Processes multiple documents in parallel, each with sequential Phase 1 → Phase 2.
"""

import sys
import logging
import time
from pathlib import Path
from typing import List, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.domain_analysis.triage.document_classifier import TriageClassifier

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DocumentRefinery:
    """Parallel document refinery with ThreadPool architecture."""
    
    def __init__(self, max_workers: int = 4):
        self.max_workers = max_workers
        self.rules_file = Path(".refinery/rules/extraction_rules.yaml")
        self.profiles_dir = Path(".refinery/profiles")
        self.extraction_dir = Path(".refinery/extractions")
        
        # Create directories
        self.profiles_dir.mkdir(parents=True, exist_ok=True)
        self.extraction_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize classifier
        self.classifier = TriageClassifier(
            rules_file=str(self.rules_file),
            profiles_dir=str(self.profiles_dir)
        )
        
        logger.info(f"DocumentRefinery initialized with {max_workers} workers")
    
    def characterize_document(self, pdf_path: Path) -> Dict[str, Any]:
        """Phase 1: Characterize document and determine extraction strategy."""
        logger.info(f"[Phase1] Characterizing {pdf_path.name}...")
        start_time = time.time()
        
        try:
            profile = self.classifier.classify_document(str(pdf_path))
            
            # Extract key decision metrics
            result = {
                "doc_id": profile.document_id,
                "filename": profile.filename,
                "pdf_path": str(pdf_path),
                "origin_type": profile.origin_type,
                "category": profile.category,
                "recommended_strategy": profile.recommended_strategy,
                "estimated_cost": profile.estimated_extraction_cost,
                "confidence": profile.category_confidence,
                "pages": profile.total_pages,
                "avg_chars_per_page": profile.avg_chars_per_page,
                "image_area_ratio": profile.image_area_ratio,
                "scanned_page_ratio": profile.scanned_page_ratio,
                "profile_path": f"{self.profiles_dir}/{profile.document_id}.json",
                "phase1_duration": time.time() - start_time
            }
            
            logger.info(f"[Phase1] Done: {pdf_path.name} → {profile.recommended_strategy} ({profile.category_confidence:.2f})")
            return result
            
        except Exception as e:
            logger.error(f"[Phase1] Failed: {pdf_path.name} - {e}")
            return {
                "doc_id": pdf_path.stem,
                "filename": pdf_path.name,
                "pdf_path": str(pdf_path),
                "error": str(e),
                "phase1_duration": time.time() - start_time
            }
    
    def extract_document(self, characterization: Dict[str, Any]) -> Dict[str, Any]:
        """Phase 2: Extract content using recommended strategy."""
        doc_id = characterization["doc_id"]
        strategy = characterization["recommended_strategy"]
        logger.info(f"[Phase2] Extracting {doc_id} using {strategy}...")
        start_time = time.time()
        
        try:
            # Mock extraction for now - will implement real strategies later
            if strategy == "vision":
                result = self._mock_vision_extraction(characterization)
            elif strategy == "layout":
                result = self._mock_layout_extraction(characterization)
            elif strategy == "fast_text":
                result = self._mock_fast_text_extraction(characterization)
            elif strategy == "hybrid":
                result = self._mock_hybrid_extraction(characterization)
            else:
                raise ValueError(f"Unknown strategy: {strategy}")
            
            result.update({
                "doc_id": doc_id,
                "strategy_used": strategy,
                "phase2_duration": time.time() - start_time,
                "extraction_path": f"{self.extraction_dir}/{doc_id}_extraction.json"
            })
            
            logger.info(f"[Phase2] Done extracting {doc_id}")
            return result
            
        except Exception as e:
            logger.error(f"[Phase2] Failed: {doc_id} - {e}")
            return {
                "doc_id": doc_id,
                "strategy_used": strategy,
                "error": str(e),
                "phase2_duration": time.time() - start_time
            }
    
    def _mock_vision_extraction(self, char: Dict[str, Any]) -> Dict[str, Any]:
        """Mock vision extraction for scanned documents."""
        time.sleep(2.0)  # Simulate OCR processing
        return {
            "extraction_method": "vision_ocr",
            "extracted_text_length": 15000,
            "tables_detected": 5,
            "confidence": 0.85,
            "processing_cost": "high"
        }
    
    def _mock_layout_extraction(self, char: Dict[str, Any]) -> Dict[str, Any]:
        """Mock layout extraction for structured documents."""
        time.sleep(1.5)  # Simulate layout analysis
        return {
            "extraction_method": "layout_analysis",
            "extracted_text_length": 25000,
            "tables_detected": 12,
            "confidence": 0.92,
            "processing_cost": "medium"
        }
    
    def _mock_fast_text_extraction(self, char: Dict[str, Any]) -> Dict[str, Any]:
        """Mock fast text extraction for simple documents."""
        time.sleep(0.5)  # Simulate fast processing
        return {
            "extraction_method": "fast_text",
            "extracted_text_length": 8000,
            "tables_detected": 2,
            "confidence": 0.95,
            "processing_cost": "low"
        }
    
    def _mock_hybrid_extraction(self, char: Dict[str, Any]) -> Dict[str, Any]:
        """Mock hybrid extraction for mixed documents."""
        time.sleep(3.0)  # Simulate complex processing
        return {
            "extraction_method": "hybrid_vision_layout",
            "extracted_text_length": 18000,
            "tables_detected": 8,
            "confidence": 0.78,
            "processing_cost": "high"
        }
    
    def process_document(self, pdf_path: Path) -> Dict[str, Any]:
        """Process a single document: Phase 1 → Phase 2."""
        logger.info(f"🔄 Processing {pdf_path.name}")
        start_time = time.time()
        
        # Phase 1: Characterization
        characterization = self.characterize_document(pdf_path)
        
        # Phase 2: Extraction (only if characterization succeeded)
        if "error" not in characterization:
            extraction = self.extract_document(characterization)
        else:
            extraction = {"error": "Skipped due to characterization failure"}
        
        # Combine results
        result = {
            "filename": pdf_path.name,
            "total_duration": time.time() - start_time,
            "characterization": characterization,
            "extraction": extraction
        }
        
        logger.info(f"✅ Completed {pdf_path.name} in {result['total_duration']:.2f}s")
        return result
    
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
            # Submit all documents for parallel processing
            futures = [executor.submit(self.process_document, pdf) for pdf in pdf_files]
            
            # Collect results as they complete
            for future in as_completed(futures):
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    logger.error(f"Document processing failed: {e}")
        
        # Print summary
        self._print_summary(results, time.time() - start_time)
        return results
    
    def _print_summary(self, results: List[Dict[str, Any]], total_time: float):
        """Print processing summary."""
        print("\n" + "="*80)
        print("📊 REFINERY BATCH SUMMARY")
        print("="*80)
        
        successful = [r for r in results if "error" not in r["characterization"]]
        failed = [r for r in results if "error" in r["characterization"]]
        
        print(f"\n✅ Successfully processed: {len(successful)} documents")
        print(f"❌ Failed: {len(failed)} documents")
        print(f"⏱️  Total time: {total_time:.2f}s")
        
        if successful:
            print(f"\n📋 Strategy Distribution:")
            strategy_counts = {}
            for result in successful:
                strategy = result["characterization"]["recommended_strategy"]
                strategy_counts[strategy] = strategy_counts.get(strategy, 0) + 1
            
            for strategy, count in strategy_counts.items():
                print(f"   {strategy}: {count} documents")
            
            print(f"\n📄 Processing Details:")
            print(f"{'Filename':<30} {'Strategy':<12} {'Confidence':<10} {'Pages':<6} {'Duration':<8}")
            print("-" * 75)
            
            for result in successful:
                char = result["characterization"]
                print(f"{char['filename']:<30} {char['recommended_strategy']:<12} "
                      f"{char['confidence']:<10.2f} {char['pages']:<6} "
                      f"{result['total_duration']:<8.2f}")
        
        if failed:
            print(f"\n❌ Failed Documents:")
            for result in failed:
                print(f"   {result['filename']}: {result['characterization']['error']}")
        
        print(f"\n📁 Outputs:")
        print(f"   Profiles: {self.profiles_dir}")
        print(f"   Extractions: {self.extraction_dir}")
        print("="*80)

def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Parallel Document Refinery")
    parser.add_argument("--pdf-folder", default="data/raw", help="PDF folder path")
    parser.add_argument("--workers", type=int, default=4, help="Number of parallel workers")
    
    args = parser.parse_args()
    
    # Initialize refinery
    refinery = DocumentRefinery(max_workers=args.workers)
    
    # Process batch
    results = refinery.process_batch(args.pdf_folder)
    
    return results

if __name__ == "__main__":
    main()
