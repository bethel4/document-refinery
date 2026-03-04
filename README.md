# Document Refinery

Production document refinery with intelligent triage and multi-strategy extraction.

## 🏗️ Architecture

### Phase 1: Document Triage
- **Origin Type Detection**: native_digital, scanned_image, mixed
- **Layout Complexity**: single_column, multi_column, table_heavy, figure_heavy
- **Domain Classification**: financial, legal, technical, general
- **Strategy Recommendation**: fast_text, layout, vision, hybrid

### Phase 2: Strategy-Based Extraction
- **FastTextExtractor**: pdfplumber for simple digital documents
- **LayoutExtractor**: Docling for structured documents with tables
- **VisionExtractor**: Tesseract OCR for scanned documents
- **Confidence-Gated Escalation**: Automatic strategy escalation when confidence is low

### Phase 3: Parallel Processing
- **ThreadPool Architecture**: Process multiple documents simultaneously
- **Per-Document Phases**: Phase 1 → Phase 2 dependency maintained
- **Scalable**: Configurable worker count

## 🚀 Quick Start

### Installation

```bash
# Clone repository
git clone https://github.com/your-org/document-refinery.git
cd document-refinery

# Install dependencies
pip install -e .

# For development dependencies
pip install -e ".[dev]"

# For vision features
pip install -e ".[vision]"
```

### Basic Usage

```bash
# Process single document
python refinery.py --single "data/raw/your_document.pdf"

# Process batch of documents
python refinery.py --pdf-folder "data/raw" --workers 4

# Custom confidence threshold
python refinery.py --confidence-threshold 0.8 --workers 2
```

### Advanced Usage

```bash
# Use custom configuration
python refinery.py --config "custom_rules.yaml" --workers 8

# Process specific document types
python refinery.py --pdf-folder "data/financial" --confidence-threshold 0.9
```

## 📁 Directory Structure

```
document-refinery/
├── refinery.py                    # Main pipeline runner
├── src/
│   ├── models/                   # Pydantic schemas
│   │   ├── document_models.py    # DocumentProfile, ExtractedDocument, etc.
│   │   └── __init__.py
│   ├── agents/                   # Processing agents
│   │   ├── triage.py          # TriageAgent
│   │   ├── extractor.py        # ExtractionAgent
│   │   └── __init__.py
│   ├── strategies/               # Extraction strategies
│   │   ├── extractor_base.py   # BaseExtractor interface
│   │   ├── fast_text_extractor.py
│   │   ├── layout_extractor.py
│   │   ├── vision_extractor.py
│   │   └── __init__.py
│   └── domain_analysis/         # Existing triage logic
│       └── triage/
├── rubric/                      # Configuration
│   └── extraction_rules.yaml   # Strategy rules and thresholds
├── .refinery/                   # Output directory
│   ├── profiles/               # DocumentProfile JSON files
│   ├── extractions/            # ExtractedDocument JSON files
│   ├── pages/                  # Generated page images
│   ├── ocr/                    # OCR text files
│   └── logs/                   # Processing logs and ledger
├── data/
│   └── raw/                   # Input PDF files
└── pyproject.toml               # Dependencies and configuration
```

## 📊 Output Formats

### DocumentProfile (Triage Phase)
```json
{
  "document_id": "example_123456789",
  "origin_type": "scanned_image",
  "layout_complexity": "figure_heavy",
  "recommended_strategy": "vision",
  "confidence": 0.95,
  "total_pages": 25,
  "scanned_page_ratio": 0.88
}
```

### ExtractedDocument (Extraction Phase)
```json
{
  "document_id": "example_123456789",
  "strategy_used": "vision",
  "pages": [
    {
      "page_num": 1,
      "text": "Extracted text content...",
      "confidence": 0.82,
      "tables": [...]
    }
  ],
  "routing_metadata": {
    "escalated": false,
    "average_confidence": 0.79
  }
}
```

## ⚙️ Configuration

### Extraction Rules (`rubric/extraction_rules.yaml`)

```yaml
# Strategy assignment rules
strategy_assignment:
  fast_text:
    conditions:
      - avg_chars_per_page >= 500
      - image_area_ratio <= 0.2
    confidence_threshold: 0.8
    
  vision:
    conditions:
      - avg_chars_per_page < 200
      - image_area_ratio > 0.3
    confidence_threshold: 0.6
```

## 🎯 Strategy Selection Logic

### Fast Text Strategy
- **Best for**: Native digital documents with clear text
- **Indicators**: High character density, low image ratio, searchable
- **Speed**: Fast (~0.5s per page)
- **Cost**: Low

### Layout Strategy  
- **Best for**: Structured documents with tables and forms
- **Indicators**: Moderate text, some images, table structures
- **Speed**: Medium (~1.5s per page)
- **Cost**: Medium

### Vision Strategy
- **Best for**: Scanned documents and image-heavy content
- **Indicators**: Low text, high image ratio, not searchable
- **Speed**: Slow (~2.0s per page)
- **Cost**: High

## 🔄 Escalation Logic

```
Low Confidence (< threshold)?
    ↓
Fast Text → Layout → Vision
    ↓
Budget Guard (Vision)
    ↓
Stop at Budget Cap
```

## 📈 Performance

### Benchmarks
- **Simple Text**: ~50 pages/second
- **Layout Documents**: ~20 pages/second  
- **Scanned Documents**: ~10 pages/second
- **Mixed Documents**: ~15 pages/second

## 🔍 Monitoring

### Logs
- **Processing logs**: `.refinery/logs/`
- **Extraction ledger**: `.refinery/logs/extraction_ledger.jsonl`
- **Error tracking**: Detailed error messages and stack traces

## 🚨 Troubleshooting

### Common Issues

**Low OCR Quality**
- Increase DPI in vision extractor
- Check Tesseract installation
- Verify image preprocessing

**Memory Issues**
- Reduce worker count
- Lower DPI for vision processing
- Enable memory limits in config

## 📄 License

MIT License - see LICENSE file for details.

## Architecture

PDF → Triage → Extraction (A/B/C) → Chunking → PageIndex → Query Agent

## Domain Analysis Subsystems

### Corpus Calibration Lab

The Corpus Calibration Lab analyzes your PDF corpus to generate statistical thresholds for document processing:

**Location**: `src/domain_analysis/calibration/`

**Features**:
- Scans all PDFs in `data/raw/`
- Computes metrics: total_pages, total_chars, avg_chars_per_page, image_area_ratio, detected_table_count, x_cluster_count
- Logs per-document metrics to `.refinery/logs/corpus_metrics.jsonl`
- Generates percentile-based thresholds in `.refinery/rules/extraction_rules.yaml`
- Uses statistical distributions (20th/80th percentile separation) - no hardcoded thresholds

**Usage**:
```bash
python src/domain_analysis/calibration/run_calibration.py
```

### Document Triage Engine

The Document Triage Engine classifies individual documents using calibrated thresholds:

**Location**: `src/domain_analysis/triage/`

**Features**:
- Lightweight classification of single PDFs
- Loads thresholds from `.refinery/rules/extraction_rules.yaml`
- Classifies: origin_type, layout_complexity, language + confidence, domain_hint, estimated_extraction_cost
- Uses Pydantic DocumentProfile model
- Saves profiles to `.refinery/profiles/{doc_id}.json`
- No full extraction - only classification logic

**Usage**:
```bash
python src/domain_analysis/triage/run_triage.py path/to/document.pdf
```

## Installation

```bash
pip install -e .
```

## Project Structure

```
document-refinery/
├── .refinery/
│   ├── profiles/              # DocumentProfile JSON outputs
│   ├── logs/                  # Extraction metrics
│   └── rules/
│       └── extraction_rules.yaml
├── data/
│   ├── raw/                   # Input PDFs
│   └── processed/             # Extracted JSON outputs
├── src/
│   ├── domain_analysis/       # NEW: Corpus calibration and triage
│   │   ├── calibration/        # Corpus analysis lab
│   │   └── triage/           # Document classification engine
│   ├── triage/               # Original triage (legacy)
│   ├── extraction/           # Multi-strategy extraction
│   ├── models/               # Pydantic models
│   ├── chunking/             # LDU chunking engine
│   ├── pageindex/            # Hierarchical indexing
│   ├── query/                # Query interface
│   └── main.py
├── tests/
├── pyproject.toml
└── README.md
```