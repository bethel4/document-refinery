# Document Refinery

A multi-stage document intelligence system:

- Stage 1: Document Triage (classification)
- Stage 2: Multi-strategy extraction with escalation guard
- Stage 3: Semantic chunking (LDUs)
- Stage 4: Hierarchical PageIndex builder
- Stage 5: Query interface agent (navigation + semantic + structured)

## Architecture

```mermaid
flowchart TB

    %% =========================
    %% Stage 0
    %% =========================
    subgraph S0["Stage 0 · Corpus Calibration"]
        C1["Corpus Sampling"]
        C2["Metric Distribution Analysis"]
        C3["Threshold Definition"]
    end

    %% =========================
    %% Stage 1
    %% =========================
    subgraph S1["Stage 1 · Triage Engine"]
        T1["Text Layer Detection"]
        T2["Image Ratio Computation"]
        T3["Layout Entropy Score"]
        T4["Document Profile"]
    end

    %% =========================
    %% Stage 2
    %% =========================
    subgraph S2["Stage 2 · Strategy Routing"]

        R1{"Initial Strategy Selection"}

        A["Strategy A<br/>FastTextExtractor<br/>(pdfplumber)"]

        ACONF{"Confidence >= 0.80"}

        B["Strategy B<br/>LayoutExtractor<br/>(MinerU / Docling)"]

        BCONF{"Confidence >= 0.70"}

        C["Strategy C<br/>VisionExtractor<br/>(VLM via OpenRouter)"]

        CCONF{"Confidence >= 0.60"}

        MR["Manual Review Queue"]

        DONE["Extraction Complete"]

        R1 --> A
        R1 --> B
        R1 --> C

        A --> ACONF

        ACONF -- Yes --> DONE
        ACONF -- No --> B

        B --> BCONF

        BCONF -- Yes --> DONE
        BCONF -- No --> C

        C --> CCONF

        CCONF -- Yes --> DONE
        CCONF -- No --> MR

    end

    %% =========================
    %% Stage 3
    %% =========================
    subgraph S3["Stage 3 · Semantic Chunker"]

        LDU["LDU Generator"]

        RULES["Semantic Chunking Rules"]

        R1A["Tables stay attached to headers"]
        R1B["Figure captions preserved"]
        R1C["Lists remain atomic"]
        R1D["Content hashes for provenance"]

        LDU --> RULES

        RULES --> R1A
        RULES --> R1B
        RULES --> R1C
        RULES --> R1D

    end

    %% =========================
    %% Stage 4
    %% =========================
    subgraph S4["Stage 4 · Retrieval and Indexing"]

        P1["Hierarchical PageIndex"]
        P2["LLM Section Summaries"]
        P3["Scoped Vector Retrieval"]

        P1 --> P2
        P2 --> P3

    end

    %% =========================
    %% Metadata
    %% =========================
    subgraph META["Cross-Cutting Provenance"]

        M1["Page Number"]
        M2["Strategy Used"]
        M3["Confidence Score"]
        M4["Escalation History"]
        M5["Content Hash"]

    end

    DONE --> LDU

    RULES --> P1
```



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
