import pdfplumber
from pathlib import Path

pdf_folder = Path("../../data/raw")  # adjust path
output_file = Path("../../.refinery/logs/pdf_analysis.txt")

with open(output_file, "w") as log:
    for pdf_path in pdf_folder.glob("*.pdf"):
        with pdfplumber.open(pdf_path) as pdf:
            log.write(f"Document: {pdf_path.name}\n")
            for i, page in enumerate(pdf.pages):
                chars = page.chars
                char_count = len(chars)
                bbox_areas = [ (float(c['x1'])-float(c['x0']))*(float(c['top'])-float(c['bottom'])) for c in chars ]
                total_bbox_area = sum(abs(a) for a in bbox_areas)
                page_area = page.width * page.height
                whitespace_ratio = 1 - (total_bbox_area / page_area if page_area > 0 else 0)
                log.write(
                    f" Page {i+1}: chars={char_count}, "
                    f"bbox_area={total_bbox_area:.2f}, "
                    f"whitespace_ratio={whitespace_ratio:.2f}\n"
                )
            log.write("\n")