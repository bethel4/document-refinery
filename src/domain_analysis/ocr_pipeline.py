import os
from pdf2image import convert_from_path
from pathlib import Path
import pytesseract

# Folder containing PDFs
pdf_folder = Path("../../data/raw")
# Folder to save page images
IMAGE_DIR = Path(".refinery/pages")
# Folder to save OCR text
OCR_DIR = Path(".refinery/ocr")

# Make sure output directories exist
os.makedirs(IMAGE_DIR, exist_ok=True)
os.makedirs(OCR_DIR, exist_ok=True)

# Loop over each PDF in the folder
for pdf_file in pdf_folder.glob("*.pdf"):
    print(f"Processing {pdf_file.name}...")
    
    try:
        # Convert PDF to images
        pages = convert_from_path(pdf_file, dpi=300)
    except Exception as e:
        print(f"Failed to convert {pdf_file.name}: {e}")
        continue

    # Save each page as image and run OCR
    for i, page in enumerate(pages, start=1):
        # Save image
        image_path = IMAGE_DIR / f"{pdf_file.stem}_page_{i}.png"
        page.save(image_path, "PNG")
        
        # Run OCR
        text = pytesseract.image_to_string(page)
        text_path = OCR_DIR / f"{pdf_file.stem}_page_{i}.txt"
        with open(text_path, "w", encoding="utf-8") as f:
            f.write(text)
    
    print(f"Finished OCR for {pdf_file.name}, {len(pages)} pages processed.")