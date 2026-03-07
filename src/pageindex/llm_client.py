from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from typing import Optional


class GeminiSummarizer:
    """Tiny Gemini client for short section summaries."""

    def __init__(self, model: str = "gemini-2.0-flash", timeout_seconds: int = 20) -> None:
        self.api_key = os.getenv("GEMINI_API_KEY", "")
        self.model = model
        self.timeout_seconds = timeout_seconds

    @property
    def enabled(self) -> bool:
        return bool(self.api_key)

    def summarize(self, text: str, max_chars: int = 220) -> str:
        if not text.strip():
            return ""
        if not self.enabled:
            return self._fallback_summary(text, max_chars)

        prompt = (
            "Summarize this document section in one short factual paragraph for retrieval navigation. "
            "Focus on topic and key entities; no markdown.\n\n"
            f"Section:\n{text[:8000]}"
        )

        body = {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {
                "temperature": 0.1,
                "maxOutputTokens": 120,
            },
        }

        url = (
            f"https://generativelanguage.googleapis.com/v1beta/models/{self.model}:generateContent"
            f"?key={self.api_key}"
        )

        req = urllib.request.Request(
            url,
            data=json.dumps(body).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        try:
            with urllib.request.urlopen(req, timeout=self.timeout_seconds) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
            text_out = (
                payload.get("candidates", [{}])[0]
                .get("content", {})
                .get("parts", [{}])[0]
                .get("text", "")
                .strip()
            )
            if not text_out:
                return self._fallback_summary(text, max_chars)
            return text_out[:max_chars]
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, KeyError, IndexError, json.JSONDecodeError):
            return self._fallback_summary(text, max_chars)

    def _fallback_summary(self, text: str, max_chars: int) -> str:
        normalized = " ".join(text.split())
        return normalized[:max_chars]
