from typing import Dict, Any

from app.models.schemas import TestRagChunk


class PayloadResolver:
    @classmethod
    def resolve_to_test_rag_chunk(cls, payload: Dict[str, Any]) -> TestRagChunk:
        return TestRagChunk(
            chunk_id=payload["docId"],
            item_sequence=payload["itemSequence"],
            name=payload["name"],
            contents=payload.get("contents", ""),
            meta=payload.get("meta", {}),
        )
