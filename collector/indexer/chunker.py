# -*- coding: utf-8 -*-
# Simple chunking utilities
from __future__ import annotations
from typing import Dict, Any, List
import hashlib

from collector.util.text_normalizer import normalize_text


def extract_text(doc: Dict[str, Any]) -> str:
    """Extract canonical text from a document. Adjust to your schema."""
    # TODO - you have to fill-out "fields" for the content will be embeddeding
    fields = (
       "name" 
    )
    parts: List[str] = []
    for k in fields:
        v = doc.get(k)
        parts.append(normalize_text(v) if isinstance(v, str) else "")
    text = "\n".join(parts)
    return text if text.strip() else normalize_text(str(doc))


def chunk_text(text: str, max_chars: int = 1000, overlap: int = 100) -> List[str]:
    """Split text into overlapping windows by characters."""
    if max_chars <= 0:
        return [text]
    n = len(text)
    out: List[str] = []
    start = 0
    while start < n:
        end = min(start + max_chars, n)
        out.append(text[start:end])
        if end == n:
            break
        start = max(0, end - overlap)
    return out


# noinspection PyTypeChecker
def make_chunk_id(doc_id: str, chunk_text_value: str, idx: int) -> str:
    """Return a deterministic chunk id based on doc_id + chunk text."""
    h = hashlib.sha256((doc_id + "|" + chunk_text_value).encode("utf-8")).hexdigest()
    return h[:24]
