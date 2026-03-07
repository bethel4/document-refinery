FROM python:3.14-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    poppler-utils \
    tesseract-ocr \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml uv.lock* ./
RUN pip install --no-cache-dir uv && uv sync --frozen || uv sync

COPY . .

CMD ["uv", "run", "python", "src/extraction/pipeline_runner.py", "--pdf-folder", "data/raw", "--workers", "1"]
