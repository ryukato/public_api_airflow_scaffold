from typing import Dict, Any

from app.models.schemas import TestRagChunk


class PayloadBuilder:
    @classmethod
    def build_payload(cls, chunk: TestRagChunk, default_embedding_version: int = 1) -> Dict[str, Any]:
        return {
            "docId": chunk.chunk_id,
            "itemSequence": chunk.item_sequence,
            "name": chunk.name,
            "contents": chunk.contents,
            "meta": chunk.meta,
        }
