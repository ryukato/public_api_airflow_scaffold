# collector/repository/qdrant_repository.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from qdrant_client import QdrantClient
from qdrant_client.http.models import Distance, VectorParams, QueryResponse, Filter, PointStruct, CollectionInfo, \
    OptimizersConfigDiff

_DISTANCE_MAP = {
    "cosine": Distance.COSINE,
    "euclid": Distance.EUCLID,
    "dot": Distance.DOT,
}


def _to_distance_enum(name: str) -> Distance:
    n = (name or "cosine").strip().lower()
    if n in ("cosine", "cos", "cos_dist"):
        return Distance.COSINE
    if n in ("dot", "dotproduct", "dot_product"):
        return Distance.DOT
    if n in ("l2", "euclid", "euclidean"):
        return Distance.EUCLID
    # 기본값
    return Distance.COSINE


def _extract_vector_schema(info: CollectionInfo) -> tuple[int, Distance]:
    """
    Return (size, distance) for single-vector OR first vector in named-vectors config.
    """
    vc = info.config.params.vectors
    # qdrant_client returns either VectorParams or dict[str, VectorParams]
    if isinstance(vc, dict):
        # named vectors: take the first one
        vp = next(iter(vc.values()))
        return vp.size, vp.distance
    else:
        # single vector
        return vc.size, vc.distance


logger = logging.getLogger(__name__)


class QdrantRepository:
    """
    Repository layer for Qdrant operations.
    Handles collection creation, schema management, and CRUD for points.
    """

    def __init__(
        self,
        collection: str,
        distance: str = "cosine",
        host: str = "localhost",
        port: int = 6333,
        grpc_port: int = 6334,
        prefer_grpc: bool = False,
        *,
        timeout_sec: float | None = None,     # NEW: REST timeout
    ) -> None:
        self.collection = collection
        self.distance = distance
        self.prefer_grpc = prefer_grpc

        # qdrant_client는 REST에서 timeout 인자를 받습니다.
        # prefer_grpc=True일 땐 gRPC 경로로 붙습니다(일부 환경서 초기화 지연 가능).
        if prefer_grpc:
            logger.info("QdrantRepository:init prefer_grpc=True host=%s grpc_port=%s", host, grpc_port)
            self.client = QdrantClient(host=host, port=grpc_port, prefer_grpc=True)
        else:
            t = float(timeout_sec or 3.0)
            logger.info("QdrantRepository:init REST host=%s port=%s timeout=%.1fs", host, port, t)
            self.client = QdrantClient(host=host, port=port, timeout=t)
    
    def ensure_collection(
            self,
            vector_size: int,
            distance: str = "cosine",
            on_mismatch: str = "recreate",  # "recreate" | "error"
    ) -> None:
        """
        Ensure the collection exists with the expected vector_size & distance.
        If mismatch:
          - on_mismatch="recreate": delete & create collection with new schema
          - on_mismatch="error": raise ValueError
        """
        dist = _to_distance_enum(distance)

        # 1) Try to get current collection info
        try:
            info = self.client.get_collection(self.collection)
        except Exception as e:
            # Not found → create
            msg = str(e).lower()
            if "not found" in msg or "404" in msg:
                logger.info(f"Create new collection: collection={self.collection}")
                self.client.create_collection(
                    collection_name=self.collection,
                    vectors_config=VectorParams(size=int(vector_size), distance=dist),
                    optimizers_config=OptimizersConfigDiff(),  # optional tuning
                )
                return
            # 다른 오류는 그대로 전달
            raise
        # 2) Exists → check schema
        current_size, current_distance = _extract_vector_schema(info)
        if current_size is None or current_distance is None:
            # 스키마를 해석할 수 없으면 정책에 따라 처리
            if on_mismatch == "recreate":
                self.client.delete_collection(self.collection)
                self.client.create_collection(
                    collection_name=self.collection,
                    vectors_config=VectorParams(size=int(vector_size), distance=dist),
                    optimizers_config=OptimizersConfigDiff(),
                )
                return
            if on_mismatch == "error":
                raise RuntimeError("Cannot determine existing vector schema (size/distance).")
            # ignore
            return

        if int(current_size) == int(vector_size) and current_distance == dist:
            return  # OK

        # 3) Mismatch → policy
        if on_mismatch == "recreate":
            self.client.delete_collection(self.collection)
            self.client.create_collection(
                collection_name=self.collection,
                vectors_config=VectorParams(size=int(vector_size), distance=dist),
                optimizers_config=OptimizersConfigDiff(),
            )
            return
        if on_mismatch == "error":
            raise RuntimeError(
                f"Qdrant schema mismatch: want size={vector_size}, dist={dist}; "
                f"got size={current_size}, dist={current_distance} (collection={self.collection})"
            )
        # ignore → 그대로 사용

    def upsert_points(self, points: List[Dict[str, Any]]) -> int:
        """
        Upsert a batch of points into the collection.
        Returns the number of points inserted/updated.
        """
        if not points:
            return 0

        if self.prefer_grpc:
            as_structs = [
                PointStruct(id=p["id"], vector=p["vector"], payload=p.get("payload"))
                for p in points
            ]
            res = self.client.upsert(
                collection_name=self.collection,
                points=as_structs,
                wait=True,
            )
            # res is UpdateResult(time, status, upserted)
            return len(points)
        else:
            self.client.upsert(
                collection_name=self.collection,
                points=points,
                wait=True
            )
            return len(points)

    def delete_points(self, point_ids: List[str]) -> int:
        """
        Delete points by IDs.
        Returns the number of points deleted.
        """
        if not point_ids:
            return 0

        self.client.delete(
            collection_name=self.collection,
            points_selector={"points": point_ids},
            wait=True
        )
        return len(point_ids)

    def query_points(
            self,
            vector: List[float],
            limit: int = 5,
            filters: Optional[Filter] = None,
            with_payload: bool = True,
            with_vectors: bool = False,
            score_threshold: Optional[float] = None,
            offset: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Return the SDK's QueryResponse so callers can access scores/payloads/offset."""
        return QdrantRepository.to_dicts(
            self.client.query_points(
                collection_name=self.collection,
                query=vector,
                limit=limit,
                query_filter=filters,
                with_payload=with_payload,
                with_vectors=with_vectors,
                score_threshold=score_threshold,
                offset=offset,
            )
        )

    # Optional: helper to convert QueryResponse → List[Dict] (if you prefer app-level DTOs)
    @staticmethod
    def to_dicts(resp: QueryResponse) -> List[Dict[str, Any]]:
        """Flatten QueryResponse into a list of dicts with id/score/payload/vector(opt)."""
        out: List[Dict[str, Any]] = []
        for sp in resp.points or []:  # type: ignore[attr-defined]
            # sp is ScoredPoint
            item: Dict[str, Any] = {
                "id": sp.id,
                "score": sp.score,
                "payload": getattr(sp, "payload", None),
            }
            if hasattr(sp, "vector") and sp.vector is not None:
                item["vector"] = sp.vector
            out.append(item)
        return out
