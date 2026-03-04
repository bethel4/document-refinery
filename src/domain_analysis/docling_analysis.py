from docling import DoclingDocument
from pathlib import Path

pdf_folder = Path("../../data/raw")
output_file = Path("../../.refinery/logs/docling_output.txt")

with open(output_file, "w") as log:
    for pdf_path in pdf_folder.glob("*.pdf"):
        doc = DoclingDocument(str(pdf_path))
        log.write(f"Document: {pdf_path.name}\n")
        log.write(f" Pages: {len(doc.pages)}\n")
        log.write(f" Tables found: {len(doc.tables)}\n")
        log.write(f" Figures found: {len(doc.figures)}\n")
        log.write(f" Text blocks: {len(doc.blocks)}\n")
        log.write("\n")

print(f"Docling analysis written to {output_file}")