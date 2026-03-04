"""
Run document triage as a standalone script
"""

import logging
from pathlib import Path
import sys
import argparse

# Add parent directories to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.domain_analysis.triage.document_classifier import TriageClassifier

def main():
    """Run document triage on a single PDF."""
    
    parser = argparse.ArgumentParser(description='Classify a PDF document')
    parser.add_argument('pdf_path', help='Path to the PDF file to classify')
    parser.add_argument('--rules', default='.refinery/rules/extraction_rules.yaml', 
                       help='Path to extraction rules file')
    parser.add_argument('--profiles', default='.refinery/profiles',
                       help='Directory to save document profiles')
    
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    logger = logging.getLogger(__name__)
    
    # Validate input
    pdf_path = Path(args.pdf_path)
    if not pdf_path.exists():
        logger.error(f"PDF file not found: {pdf_path}")
        sys.exit(1)
    
    if not pdf_path.suffix.lower() == '.pdf':
        logger.error(f"File is not a PDF: {pdf_path}")
        sys.exit(1)
    
    # Initialize classifier
    classifier = TriageClassifier(
        rules_file=args.rules,
        profiles_dir=args.profiles
    )
    
    # Run classification
    try:
        profile = classifier.classify_document(str(pdf_path))
        
        logger.info(f"Triage completed successfully!")
        logger.info(f"Document ID: {profile.document_id}")
        logger.info(f"Profile saved to: {args.profiles}/{profile.document_id}.json")
        
        # Print summary
        print(f"\nDocument Classification Results:")
        print(f"  Filename: {profile.filename}")
        print(f"  Origin Type: {profile.origin_type}")
        print(f"  Layout Complexity: {profile.layout_complexity}")
        print(f"  Language: {profile.language} (confidence: {profile.language_confidence:.2f})")
        print(f"  Domain: {profile.domain_hint} (confidence: {profile.domain_confidence:.2f})")
        print(f"  Category: {profile.category} (confidence: {profile.category_confidence:.2f})")
        print(f"  Recommended Strategy: {profile.recommended_strategy}")
        print(f"  Estimated Cost: {profile.estimated_extraction_cost}")
        print(f"  Overall Quality Score: {profile.overall_quality_score:.2f}")
        
    except Exception as e:
        logger.error(f"Triage failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
