#!/usr/bin/env python3
"""
Batch triage processor for all PDFs in raw folder.
"""

import sys
import logging
from pathlib import Path
from typing import List

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

def find_pdf_files(raw_dir: Path) -> List[Path]:
    """Find all PDF files in the raw directory."""
    pdf_files = []
    
    if not raw_dir.exists():
        logger.error(f"Raw directory does not exist: {raw_dir}")
        return pdf_files
    
    # Find all PDF files
    for pdf_file in raw_dir.glob("*.pdf"):
        pdf_files.append(pdf_file)
    
    logger.info(f"Found {len(pdf_files)} PDF files in {raw_dir}")
    return pdf_files

def process_all_pdfs(raw_dir: str = "data/raw", profiles_dir: str = ".refinery/profiles"):
    """Process all PDFs in the raw directory."""
    
    raw_path = Path(raw_dir)
    profiles_path = Path(profiles_dir)
    
    # Create profiles directory if it doesn't exist
    profiles_path.mkdir(parents=True, exist_ok=True)
    
    # Find all PDF files
    pdf_files = find_pdf_files(raw_path)
    
    if not pdf_files:
        logger.warning("No PDF files found to process")
        return
    
    # Initialize classifier
    rules_file = Path(".refinery/rules/extraction_rules.yaml")
    classifier = TriageClassifier(rules_file=str(rules_file), profiles_dir=str(profiles_path))
    
    # Process each PDF
    results = []
    failed_files = []
    
    for pdf_file in pdf_files:
        logger.info(f"Processing: {pdf_file.name}")
        
        try:
            profile = classifier.classify_document(str(pdf_file))
            results.append({
                'filename': pdf_file.name,
                'origin_type': profile.origin_type,
                'category': profile.category,
                'strategy': profile.recommended_strategy,
                'confidence': profile.category_confidence,
                'pages': profile.total_pages
            })
            logger.info(f"✅ Success: {profile.origin_type} → {profile.category}")
            
        except Exception as e:
            logger.error(f"❌ Failed to process {pdf_file.name}: {e}")
            failed_files.append({'filename': pdf_file.name, 'error': str(e)})
    
    # Print summary
    print("\n" + "="*80)
    print("📊 BATCH TRIAGE SUMMARY")
    print("="*80)
    
    print(f"\n✅ Successfully processed: {len(results)} files")
    print(f"❌ Failed: {len(failed_files)} files")
    
    if results:
        print("\n📋 Results:")
        print(f"{'Filename':<30} {'Origin':<15} {'Category':<18} {'Strategy':<12} {'Conf':<8} {'Pages':<6}")
        print("-" * 90)
        
        for result in results:
            print(f"{result['filename']:<30} {result['origin_type']:<15} "
                  f"{result['category']:<18} {result['strategy']:<12} "
                  f"{result['confidence']:<8.2f} {result['pages']:<6}")
    
    if failed_files:
        print(f"\n❌ Failed files:")
        for failed in failed_files:
            print(f"  {failed['filename']}: {failed['error']}")
    
    print(f"\n📁 Profiles saved to: {profiles_path}")
    print("="*80)

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Batch process all PDFs in raw folder")
    parser.add_argument("--raw-dir", default="data/raw", help="Raw directory path")
    parser.add_argument("--profiles-dir", default=".refinery/profiles", help="Profiles output directory")
    
    args = parser.parse_args()
    
    process_all_pdfs(args.raw_dir, args.profiles_dir)
