from __future__ import annotations

import json
import math
import os
import re
from collections import Counter
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np

from src.models.ldu import LDU


def _tokenize(text: str) -> List[str]:
    return [t for t in re.findall(r"[a-zA-Z0-9]+", text.lower()) if len(t) > 2]


def _hash_embedding(text: str, dim: int = 256) -> np.ndarray:
    vec = np.zeros(dim, dtype=np.float32)
    tokens = _tokenize(text)
    if not tokens:
        return vec
    counts = Counter(tokens)
    for token, weight in counts.items():
        idx = hash(token) % dim
        vec[idx] += float(weight)
    norm = np.linalg.norm(vec)
    if norm > 0:
        vec /= norm
    return vec


class VectorStoreIngestor:
    """Ingest LDUs into FAISS/Chroma if available, else local numpy store."""

    def __init__(self, base_dir: str = ".refinery/vector_store") -> None:
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def ingest(self, document_id: str, ldus: List[LDU]) -> Dict[str, str]:
        if not ldus:
            return {"backend": "none", "path": ""}

        embeddings = np.stack([_hash_embedding(ldu.text) for ldu in ldus])
        metadata = [
            {
                "ldu_id": ldu.ldu_id,
                "page_num": ldu.page_num,
                "content_hash": ldu.content_hash,
                "text": ldu.text,
                "bbox": (ldu.bbox or ldu.bounding_box).model_dump() if (ldu.bbox or ldu.bounding_box) else None,
            }
            for ldu in ldus
        ]

        backend = self._try_faiss(document_id, embeddings, metadata)
        if backend:
            return backend

        backend = self._try_chroma(document_id, embeddings, metadata)
        if backend:
            return backend

        return self._save_local(document_id, embeddings, metadata)

    def _try_faiss(self, document_id: str, embeddings: np.ndarray, metadata: List[Dict]) -> Dict[str, str] | None:
        try:
            import faiss  # type: ignore
        except Exception:
            return None

        index = faiss.IndexFlatIP(embeddings.shape[1])
        index.add(embeddings)
        faiss_path = self.base_dir / f"{document_id}.faiss"
        faiss.write_index(index, str(faiss_path))

        meta_path = self.base_dir / f"{document_id}.faiss.meta.json"
        meta_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")
        return {"backend": "faiss", "path": str(faiss_path)}

    def _try_chroma(self, document_id: str, embeddings: np.ndarray, metadata: List[Dict]) -> Dict[str, str] | None:
        try:
            import chromadb  # type: ignore
        except Exception:
            return None

        client = chromadb.PersistentClient(path=str(self.base_dir / "chroma"))
        collection = client.get_or_create_collection(name=document_id)

        ids = [m["ldu_id"] for m in metadata]
        docs = [m["text"] for m in metadata]
        metadatas = [{"page_num": int(m["page_num"]), "content_hash": m["content_hash"]} for m in metadata]
        collection.upsert(ids=ids, documents=docs, metadatas=metadatas, embeddings=embeddings.tolist())
        return {"backend": "chroma", "path": str(self.base_dir / "chroma")}

    def _save_local(self, document_id: str, embeddings: np.ndarray, metadata: List[Dict]) -> Dict[str, str]:
        emb_path = self.base_dir / f"{document_id}.npy"
        meta_path = self.base_dir / f"{document_id}.meta.json"
        np.save(emb_path, embeddings)
        meta_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")
        return {"backend": "numpy", "path": str(emb_path)}
