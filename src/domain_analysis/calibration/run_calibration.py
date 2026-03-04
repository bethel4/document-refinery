"""
Run corpus calibration as a standalone script
"""

import logging
from pathlib import Path
import sys

# Add parent directories to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.domain_analysis.calibration.corpus_analyzer import CorpusAnalyzer

def main():
    """Run corpus calibration on the document refinery."""
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    logger = logging.getLogger(__name__)
    
    # Initialize analyzer
    analyzer = CorpusAnalyzer(
        raw_data_dir="data/raw",
        logs_dir=".refinery/logs",
        rules_dir=".refinery/rules"
    )
    
    # Run analysis
    try:
        results = analyzer.analyze_corpus()
        logger.info(f"Calibration completed successfully!")
        logger.info(f"Documents analyzed: {results['documents_analyzed']}")
        logger.info(f"Rules saved to: {results['rules_file']}")
        
        # Print summary
        thresholds = results['thresholds']
        if 'total_pages' in thresholds:
            page_stats = thresholds['total_pages']
            print(f"\nCorpus Summary:")
            print(f"  Pages - Min: {page_stats.get('min', 0)}, Max: {page_stats.get('max', 0)}, Mean: {page_stats.get('mean', 0):.1f}")
        
        if 'avg_chars_per_page' in thresholds:
            char_stats = thresholds['avg_chars_per_page']
            print(f"  Chars/Page - Min: {char_stats.get('min', 0)}, Max: {char_stats.get('max', 0)}, Mean: {char_stats.get('mean', 0):.1f}")
        
    except Exception as e:
        logger.error(f"Calibration failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
