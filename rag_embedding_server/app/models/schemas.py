from dataclasses import dataclass
from typing import Literal, Optional, Dict, Any
from pydantic import BaseModel


class TestRagChunk(BaseModel):
    chunk_id: str
    item_sequence: str
    name: str
    contents: str
    meta: Optional[Dict[str, Any]]


class RagChunkSearchResult(BaseModel):
    chunk: TestRagChunk
    score: float
