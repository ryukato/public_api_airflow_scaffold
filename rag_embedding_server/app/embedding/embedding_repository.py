import logging
import os
from typing import List, Dict, Any, Optional

from dotenv import load_dotenv
from qdrant_client import QdrantClient
from qdrant_client.http.models import PointStruct, Distance, VectorParams, Filter

from app.embedding.payload_resover import PayloadResolver
from app.embedding.pyalod_builder import PayloadBuilder
from app.models.schemas import TestRagChunk, RagChunkSearchResult

load_dotenv()

DISTANCE_MAP = {
    "Cosine": Distance.COSINE,
    "Euclid": Distance.EUCLID,
    "Dot": Distance.DOT,
}

logger = logging.getLogger(__name__)

# TODO: Qdrant 클라이언트도 async 클라이언트로 전환 가능 여부 확인 (예: REST API 직접 사용)
# TODO: retry/backoff 정책 추가 (Qdrant 장애 시 대비)
# TODO: bulk upsert 실패 항목에 대한 로깅 및 dead-letter 큐 구조 도입 가능

class EmbeddingRepository:
    def __init__(self):
        self.host = os.getenv("QDRANT_HOST", "localhost")
        self.port = int(os.getenv("QDRANT_PORT", "6333"))
        self.collection = os.getenv("QDRANT_COLLECTION", "test")
        self.embedding_version = int(os.getenv("EMBEDDING_VERSION", "1"))
        self.embedding_size = int(os.getenv("EMBEDDING_DIM", "384"))

        logger.info(f"QDRANT_HOST: {self.host}")
        logger.info(f"QDRANT_PORT: {self.port}")
        logger.info(f"QDRANT_COLLECTION: {self.collection}")
        logger.info(f"EMBEDDING_VERSION: {self.embedding_version}")
        logger.info(f"EMBEDDING_DIM: {self.embedding_size}")

        # ✅ 환경 변수 기반 distance
        distance_str = os.getenv("QDRANT_DISTANCE", "Cosine")
        if distance_str.capitalize() not in DISTANCE_MAP:
            logger.warning(f"Unknown QDRANT_DISTANCE '{distance_str}', defaulting to Cosine")
        self.distance = DISTANCE_MAP.get(distance_str.capitalize(), Distance.COSINE)
        # ✅ 환경 변수 기반 timeout
        self.timeout = int(os.getenv("QDRANT_TIMEOUT", "30"))

        self.client = QdrantClient(host=self.host, port=self.port, timeout=self.timeout)
        self.ensure_collection()

    def ensure_collection(self):
        if self.client.collection_exists(self.collection):
            return
        self.client.create_collection(
            collection_name=self.collection,
            vectors_config=VectorParams(size=self.embedding_size, distance=self.distance),
        )

    def upsert_many(self, chunks: List[TestRagChunk], vectors: List[List[float]]) -> int:
        assert len(chunks) == len(vectors), "chunks and vectors length mismatch"

        points = [
            PointStruct(
                id=chunk.chunk_id,
                vector=vector,
                payload=self._build_payload(chunk)
            )
            for chunk, vector in zip(chunks, vectors)
        ]

        self.client.upsert(collection_name=self.collection, points=points, wait=False)

        return len(points)

    def query_chunks(
            self,
            vector: List[float],
            limit: int = 5,
            filters: Optional[Filter] = None,
            with_payload: bool = True,
            with_vectors: bool = False,
            score_threshold: Optional[float] = None,
            offset: Optional[int] = None,
    ) -> list[RagChunkSearchResult]:  # type hint 개선 (PEP 604 사용 시)
        result = self.client.query_points(
            collection_name=self.collection,
            query_vector=vector,
            limit=limit,
            with_payload=with_payload,
            with_vectors=with_vectors,
            score_threshold=score_threshold,
            offset=offset,
            query_filter=filters,
        )

        return [
            RagChunkSearchResult(chunk=self._to_chunk_from_payload(p.payload), score=p.score)
            for p in result.points if p.payload is not None
        ]

    # noinspection PyMethodMayBeStatic
    def _build_payload(self, chunk: TestRagChunk) -> Dict[str, Any]:
        return PayloadBuilder.build_payload(chunk, self.embedding_version)

    # noinspection PyMethodMayBeStatic
    def _to_chunk_from_payload(self, payload: Dict[str, Any]) -> TestRagChunk:
        return PayloadResolver.resolve_to_test_rag_chunk(payload)
