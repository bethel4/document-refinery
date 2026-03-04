#!/bin/bash
# GPU Refinery Setup Script

echo "🚀 Setting up GPU Document Refinery..."

# Check GPU availability
python3 -c "import torch; print(f'GPU Available: {torch.cuda.is_available()}'); print(f'GPU Device: {torch.cuda.get_device_name(0) if torch.cuda.is_available() else \"None\"}')"

# Install dependencies
echo "📦 Installing dependencies..."
pip install torch torchvision --index-url https://download.pytorch.org/whl/cu121
pip install pdf2image pytesseract pdfplumber pymupdf docling pydantic pyyaml numpy opencv-python

# Install system dependencies
echo "🔧 Installing system dependencies..."
sudo apt-get update
sudo apt-get install -y tesseract-ocr tesseract-ocr-eng libtesseract-dev

# Create directories
mkdir -p .refinery/{profiles,extractions,pages,ocr,logs}

echo "✅ Setup complete!"
echo "🎯 Run with: python3 refinery.py --single 'your_document.pdf'"
echo "🔥 GPU will be used automatically if available!"
