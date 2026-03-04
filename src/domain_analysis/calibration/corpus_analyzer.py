"""
Corpus Calibration Lab
Analyzes PDF corpus to compute statistical thresholds for document processing.
"""

import json
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional
import numpy as np
from dataclasses import dataclass, asdict
from datetime import datetime

@dataclass
class DocumentMetrics:
    """Metrics for a single document in the corpus."""
    filename: str
    total_pages: int
    total_chars: int
    avg_chars_per_page: float
    image_area_ratio: float
    detected_table_count: int
    tables_per_page: float
    x_cluster_count: int
    file_size_bytes: int
    char_variance_across_pages: float
    processing_timestamp: str

class CorpusAnalyzer:
    """Analyzes PDF corpus to generate extraction rules based on statistical distributions."""
    
    def __init__(self, raw_data_dir: str, logs_dir: str, rules_dir: str):
        self.raw_data_dir = Path(raw_data_dir)
        self.logs_dir = Path(logs_dir)
        self.rules_dir = Path(rules_dir)
        self.metrics_file = self.logs_dir / "corpus_metrics.jsonl"
        self.rules_file = self.rules_dir / "extraction_rules.yaml"
        
        # Ensure directories exist
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        self.rules_dir.mkdir(parents=True, exist_ok=True)
        
        self.logger = logging.getLogger(__name__)
    
    def analyze_corpus(self) -> Dict[str, Any]:
        """Main method to analyze the entire corpus and generate rules."""
        self.logger.info("Starting corpus analysis...")
        
        # Scan and analyze all PDFs
        document_metrics = self._scan_corpus()
        
        # Log metrics for each document
        self._log_metrics(document_metrics)
        
        # Compute statistical thresholds
        thresholds = self._compute_thresholds(document_metrics)
        
        # Generate extraction rules
        rules = self._generate_extraction_rules(thresholds)
        
        # Save rules to YAML file
        self._save_rules(rules)
        
        self.logger.info(f"Corpus analysis completed. Analyzed {len(document_metrics)} documents.")
        
        return {
            'documents_analyzed': len(document_metrics),
            'thresholds': thresholds,
            'rules_file': str(self.rules_file)
        }
    
    def _scan_corpus(self) -> List[DocumentMetrics]:
        """Scan all PDFs in raw data directory and compute metrics."""
        metrics = []
        pdf_files = list(self.raw_data_dir.glob("*.pdf"))
        
        self.logger.info(f"Found {len(pdf_files)} PDF files to analyze")
        
        for pdf_file in pdf_files:
            try:
                doc_metrics = self._analyze_single_pdf(pdf_file)
                if doc_metrics:
                    metrics.append(doc_metrics)
                    self.logger.debug(f"Analyzed: {pdf_file.name}")
            except Exception as e:
                self.logger.error(f"Failed to analyze {pdf_file.name}: {str(e)}")
        
        return metrics
    
    def _analyze_single_pdf(self, pdf_path: Path) -> Optional[DocumentMetrics]:
        """Analyze a single PDF file to extract metrics."""
        try:
            # Import here to avoid dependency issues if not available
            import fitz  # PyMuPDF
            
            # Get file size
            file_size = pdf_path.stat().st_size
            
            # Open PDF and analyze
            doc = fitz.open(str(pdf_path))
            
            total_pages = len(doc)
            total_chars = 0
            total_image_area = 0
            total_page_area = 0
            table_count = 0
            x_positions = []
            page_char_counts = []  # For variance calculation
            
            for page_num in range(total_pages):
                page = doc[page_num]
                
                # Get text and characters
                text = page.get_text()
                page_chars = len(text)
                total_chars += page_chars
                page_char_counts.append(page_chars)
                
                # Get page dimensions
                rect = page.rect
                page_area = rect.width * rect.height
                total_page_area += page_area
                
                # Analyze images - FIXED: Proper image area calculation
                image_list = page.get_images()
                page_image_area = 0
                
                # Get image rectangles for accurate area calculation
                for img_index, img in enumerate(image_list):
                    try:
                        # Get the image rectangle from the page
                        img_rect = page.get_image_bbox(img[0])
                        if img_rect:  # Check if rectangle is valid
                            img_area = img_rect.width * img_rect.height
                            page_image_area += img_area
                    except Exception as e:
                        # Fallback: try to get image info differently
                        try:
                            xref = img[0]
                            pix = fitz.Pixmap(doc, xref)
                            if pix and pix.width > 0 and pix.height > 0:
                                # Estimate image area based on pixel dimensions and page DPI
                                # This is a rough approximation
                                estimated_area = (pix.width * pix.height) / (72 * 72)  # Assuming 72 DPI
                                page_image_area += min(estimated_area, page_area * 0.8)  # Cap at 80% of page
                            pix = None  # Free memory
                        except:
                            continue
                
                total_image_area += page_image_area
                
                # Detect tables (simple heuristic based on text layout)
                text_dict = page.get_text("dict")
                blocks = text_dict.get("blocks", [])
                page_tables = self._detect_tables_in_blocks(blocks)
                table_count += page_tables
                
                # Collect x-positions for text clustering
                for block in blocks:
                    if "lines" in block:
                        for line in block["lines"]:
                            for span in line.get("spans", []):
                                x_positions.append(span["origin"][0])
            
            doc.close()
            
            # Compute derived metrics
            avg_chars_per_page = total_chars / total_pages if total_pages > 0 else 0
            
            # FIXED: Proper image area ratio calculation
            image_area_ratio = total_image_area / total_page_area if total_page_area > 0 else 0
            
            # NEW: Tables per page
            tables_per_page = table_count / total_pages if total_pages > 0 else 0
            
            # NEW: Character variance across pages
            char_variance_across_pages = self._calculate_char_variance(page_char_counts)
            
            x_cluster_count = self._estimate_columns(x_positions)
            
            return DocumentMetrics(
                filename=pdf_path.name,
                total_pages=total_pages,
                total_chars=total_chars,
                avg_chars_per_page=avg_chars_per_page,
                image_area_ratio=image_area_ratio,
                detected_table_count=table_count,
                tables_per_page=tables_per_page,
                x_cluster_count=x_cluster_count,
                file_size_bytes=file_size,
                char_variance_across_pages=char_variance_across_pages,
                processing_timestamp=datetime.now().isoformat()
            )
            
        except ImportError:
            self.logger.error("PyMuPDF (fitz) not installed. Cannot analyze PDFs.")
            return None
        except Exception as e:
            self.logger.error(f"Error analyzing PDF {pdf_path}: {str(e)}")
            return None
    
    def _calculate_char_variance(self, page_char_counts: List[int]) -> float:
        """Calculate variance of character counts across pages."""
        if len(page_char_counts) < 2:
            return 0.0
        
        mean_chars = sum(page_char_counts) / len(page_char_counts)
        variance = sum((count - mean_chars) ** 2 for count in page_char_counts) / len(page_char_counts)
        
        # Normalize by mean to get coefficient of variation
        if mean_chars > 0:
            normalized_variance = variance / (mean_chars ** 2)
        else:
            normalized_variance = 0.0
        
        return normalized_variance
    
    def _detect_tables_in_blocks(self, blocks: List[Dict]) -> int:
        """Simple table detection based on text block patterns."""
        table_count = 0
        
        for block in blocks:
            if "lines" in block:
                lines = block["lines"]
                
                # Simple heuristic: multiple lines with similar x positions suggest table
                if len(lines) > 2:
                    x_positions = []
                    for line in lines:
                        for span in line.get("spans", []):
                            x_positions.append(span["origin"][0])
                    
                    # Cluster x positions to detect columns
                    if len(set(round(x/10) for x in x_positions)) >= 3:
                        table_count += 1
        
        return table_count
    
    def _estimate_columns(self, x_positions: List[float]) -> int:
        """Estimate number of columns using x-position clustering."""
        if len(x_positions) < 10:
            return 1
        
        # Simple clustering using rounding to nearest 10 pixels
        clustered_positions = {}
        for x in x_positions:
            cluster_key = round(x / 20) * 20  # 20-pixel clusters
            clustered_positions[cluster_key] = clustered_positions.get(cluster_key, 0) + 1
        
        # Filter out small clusters (less than 5% of total)
        threshold = len(x_positions) * 0.05
        significant_clusters = [count for count in clustered_positions.values() if count >= threshold]
        
        return len(significant_clusters)
    
    def _log_metrics(self, metrics: List[DocumentMetrics]):
        """Log metrics for each document to JSONL file."""
        with open(self.metrics_file, 'w') as f:
            for metric in metrics:
                f.write(json.dumps(asdict(metric)) + '\n')
        
        self.logger.info(f"Metrics logged to {self.metrics_file}")
    
    def _compute_thresholds(self, metrics: List[DocumentMetrics]) -> Dict[str, Dict[str, float]]:
        """Compute percentile-based thresholds for document classification."""
        if not metrics:
            return {}
        
        # Extract metric arrays
        pages = [m.total_pages for m in metrics]
        chars = [m.total_chars for m in metrics]
        chars_per_page = [m.avg_chars_per_page for m in metrics]
        image_ratios = [m.image_area_ratio for m in metrics]
        tables_per_page = [m.tables_per_page for m in metrics]  # NEW: Use tables_per_page instead of total
        column_counts = [m.x_cluster_count for m in metrics]
        file_sizes = [m.file_size_bytes for m in metrics]
        char_variance = [m.char_variance_across_pages for m in metrics]  # NEW: Character variance
        
        thresholds = {
            'total_pages': self._compute_percentile_thresholds(pages),
            'total_chars': self._compute_percentile_thresholds(chars),
            'avg_chars_per_page': self._compute_percentile_thresholds(chars_per_page),
            'image_area_ratio': self._compute_percentile_thresholds(image_ratios),
            'tables_per_page': self._compute_percentile_thresholds(tables_per_page),  # NEW: Tables per page
            'detected_table_count': self._compute_percentile_thresholds([m.detected_table_count for m in metrics]),  # Keep for backward compatibility
            'x_cluster_count': self._compute_percentile_thresholds(column_counts),
            'file_size_bytes': self._compute_percentile_thresholds(file_sizes),
            'char_variance_across_pages': self._compute_percentile_thresholds(char_variance)  # NEW: Character variance
        }
        
        return thresholds
    
    def _compute_percentile_thresholds(self, values: List[float]) -> Dict[str, float]:
        """Compute percentile thresholds for a given metric."""
        if not values:
            return {}
        
        values_array = np.array(values)
        
        return {
            'min': float(np.percentile(values_array, 0)),
            'p10': float(np.percentile(values_array, 10)),
            'p25': float(np.percentile(values_array, 25)),
            'p50': float(np.percentile(values_array, 50)),
            'p75': float(np.percentile(values_array, 75)),
            'p90': float(np.percentile(values_array, 90)),
            'p95': float(np.percentile(values_array, 95)),
            'max': float(np.percentile(values_array, 100)),
            'mean': float(np.mean(values_array)),
            'std': float(np.std(values_array))
        }
    
    def _generate_extraction_rules(self, thresholds: Dict[str, Dict[str, float]]) -> Dict[str, Any]:
        """Generate extraction rules based on computed thresholds."""
        
        # Define document categories based on statistical distributions
        rules = {
            'document_categories': {
                'simple_text': {
                    'description': 'Simple text documents with low complexity',
                    'criteria': {
                        'avg_chars_per_page_max': thresholds.get('avg_chars_per_page', {}).get('p50', 1000),
                        'image_area_ratio_max': thresholds.get('image_area_ratio', {}).get('p25', 0.1),
                        'detected_table_count_max': thresholds.get('detected_table_count', {}).get('p25', 1),
                        'x_cluster_count_max': 2
                    },
                    'recommended_strategy': 'fast_text',
                    'confidence_threshold': 0.8
                },
                'moderate_complexity': {
                    'description': 'Documents with moderate layout complexity',
                    'criteria': {
                        'avg_chars_per_page_min': thresholds.get('avg_chars_per_page', {}).get('p50', 1000),
                        'avg_chars_per_page_max': thresholds.get('avg_chars_per_page', {}).get('p75', 2000),
                        'image_area_ratio_max': thresholds.get('image_area_ratio', {}).get('p50', 0.3),
                        'detected_table_count_max': thresholds.get('detected_table_count', {}).get('p50', 3),
                        'x_cluster_count_max': 3
                    },
                    'recommended_strategy': 'layout',
                    'confidence_threshold': 0.7
                },
                'high_complexity': {
                    'description': 'Complex documents with rich layout and content',
                    'criteria': {
                        'avg_chars_per_page_min': thresholds.get('avg_chars_per_page', {}).get('p75', 2000),
                        'image_area_ratio_min': thresholds.get('image_area_ratio', {}).get('p50', 0.3),
                        'detected_table_count_min': thresholds.get('detected_table_count', {}).get('p50', 3),
                        'x_cluster_count_min': 3
                    },
                    'recommended_strategy': 'vision',
                    'confidence_threshold': 0.6
                }
            },
            'thresholds': thresholds,
            'strategy_priorities': {
                'fast_text': 1,
                'layout': 2,
                'vision': 3
            },
            'escalation_rules': {
                'max_attempts_per_strategy': 3,
                'confidence_escalation_threshold': 0.7,
                'fallback_to_manual_after': 9  # 3 strategies * 3 attempts
            },
            'metadata': {
                'generated_at': datetime.now().isoformat(),
                'corpus_size': len(thresholds.get('total_pages', {}).get('values', [])),
                'analyzer_version': '1.0'
            }
        }
        
        return rules
    
    def _save_rules(self, rules: Dict[str, Any]):
        """Save extraction rules to YAML file."""
        import yaml
        
        with open(self.rules_file, 'w') as f:
            yaml.dump(rules, f, default_flow_style=False, indent=2)
        
        self.logger.info(f"Extraction rules saved to {self.rules_file}")

if __name__ == "__main__":
    # Example usage
    analyzer = CorpusAnalyzer(
        raw_data_dir="data/raw",
        logs_dir=".refinery/logs",
        rules_dir=".refinery/rules"
    )
    
    results = analyzer.analyze_corpus()
    print(f"Analysis complete: {results}")
