# DOMAIN_NOTES.md

## 1. Extraction Strategy Decision Tree

This tree guides which extraction method to use for each page based on its characteristics from pdfplumber analysis.

- **Step 1: Check character count**
  - `chars = 0` → Strategy C (Vision Model, page is scanned or empty)
  - `chars < 100` → Strategy C (possible title/cover pages or minimal text)
  - `chars >= 100` → Step 2

- **Step 2: Check whitespace ratio**
  - `whitespace_ratio > 0.9` → Strategy C (mostly blank / scan issues)
  - `whitespace_ratio 0.7–0.9` → Strategy A (fast text extraction may suffice)
  - `whitespace_ratio < 0.7` → Step 3

- **Step 3: Check layout complexity**
  - Table-heavy / multi-column → Strategy B (Docling / MinerU)
  - Single column → Strategy A (pdfplumber / PyMuPDF)
  - Unknown / ambiguous → Escalate to Strategy C (Vision Model)

**Example (CBE Annual Report 2006-7.pdf):**  
- Pages 1–4 → C  
- Pages 5–11 → B  
- Pages 12–36 → B  
- Pages 37–50 → B  
- Pages 51–58 → B  
- Pages 59–60 → C  

---

## 2. Failure Modes Observed

While testing pdfplumber and comparing to Docling:

| Failure Mode | Description | Example Pages | Impact |
|--------------|------------|---------------|--------|
| **Structure Collapse** | Tables or multi-column text lose alignment when extracting only text | Pages 11, 18, 23 | Fast text extraction produces jumbled content; meaning lost |
| **Context Poverty** | Important headings, captions, or section metadata lost | Pages 5, 9, 37 | Chunked text may not provide enough context for RAG |
| **Provenance Blindness** | No bounding box or page reference for extracted content | All pages with Strategy A | Makes it impossible to trace information back to the original document; critical for audit |

**Key Observation:**  
- pdfplumber is fast but fails on structure-heavy pages.  
- Docling preserves layout, tables, bounding boxes, and reading order.  
- Vision Model is expensive but required for scanned/handwritten pages.

---

## 3. Pipeline Diagram (Mermaid)

```mermaid
flowchart TD
    A[Document Input] --> B[Triage Agent: DocumentProfile]
    B --> C{Extraction Strategy?}
    C -->|Fast Text| D[Strategy A: pdfplumber / PyMuPDF]
    C -->|Layout-Aware| E[Strategy B: Docling / MinerU]
    C -->|Vision-Augmented| F[Strategy C: VLM]
    D --> G[ExtractedDocument Model]
    E --> G
    F --> G
    G --> H[Semantic Chunking Engine → Logical Document Units]
    H --> I[PageIndex Builder → Section Tree]
    I --> J[Query Interface Agent: LangGraph Tools]
    J --> K[User Queries with Provenance]