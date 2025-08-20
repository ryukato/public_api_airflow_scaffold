# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import socket
import time
import logging
from typing import Any, Dict, List, Optional

from qdrant_client.http.models import Filter

from .qdrant_repository import QdrantRepository

logger = logging.getLogger(__name__)


def _probe_tcp(host: str, port: int, timeout: float = 2.5) -> None:
    """빠른 TCP 프리플라이트. 실패 시 즉시 예외 발생."""
    with socket.create_connection((host, port), timeout=timeout):
        return


class QdrantIndexAdapter:
    """
    Thin adapter wrapping QdrantRepository.
    - Lazily creates repository (first use) to avoid early network blocking.
    - Optional fast TCP preflight for clearer errors when Qdrant is down.
    """

    def __init__(
        self,
        collection: str,
        distance: str = "cosine",
        ensure_collection: bool = True,
        host: str = "localhost",
        port: int = 6333,
        on_mismatch: str | None = None,
        *,
        connect_timeout: float | None = None,   # NEW: REST connect/read timeout(sec)
        preflight: bool = True,                 # NEW: TCP preflight toggle
    ) -> None:
        self.collection = collection
        self.distance = distance
        self.ensure_col = ensure_collection
        self.host = host
        self.port = port
        self.on_mismatch = on_mismatch or os.getenv("QDRANT_ON_SCHEMA_MISMATCH", "recreate")
        self.connect_timeout = float(os.getenv("QDRANT_TIMEOUT_SEC", str(connect_timeout or 3.0)))
        self.preflight = preflight
        self._repo: Optional[QdrantRepository] = None

        logger.info(
            "QdrantIndexAdapter:init host=%s port=%s collection=%s ensure=%s timeout=%.1fs preflight=%s",
            self.host, self.port, self.collection, self.ensure_col, self.connect_timeout, self.preflight
        )

    # --------- lazy repo ----------
    def ensure_repo(self) -> None:
        if self._repo is not None:
            return
        t0 = time.monotonic()

        if self.preflight:
            logger.info("QdrantIndexAdapter: TCP preflight to %s:%s ...", self.host, self.port)
            _probe_tcp(self.host, self.port, timeout=self.connect_timeout)
            logger.info("QdrantIndexAdapter: TCP preflight OK.")

        logger.info("QdrantIndexAdapter: creating repository (timeout=%.1fs)...", self.connect_timeout)
        self._repo = QdrantRepository(
            collection=self.collection,
            distance=self.distance,
            host=self.host,
            port=self.port,
            prefer_grpc=False,
            timeout_sec=self.connect_timeout,   # NEW: pass timeout to client
        )
        logger.info("QdrantIndexAdapter: repository created in %.2fs", time.monotonic() - t0)

    # ---------- schema ----------
    def ensure_collection(self, vector_size: int) -> None:
        if not self.ensure_col:
            logger.debug("QdrantIndexAdapter: ensure_collection skipped (ensure_col=False)")
            return
        self.ensure_repo()
        assert self._repo is not None
        self._repo.ensure_collection(
            vector_size=vector_size,
            distance=self.distance,
            on_mismatch=self.on_mismatch,
        )

    # ---------- write ----------
    def upsert_points(self, points: List[Dict[str, Any]], vector_size: int | None = None) -> int:
        """Idempotent batch upsert. Returns number of points written."""
        self.ensure_repo()
        assert self._repo is not None

        if self.ensure_col and vector_size is not None:
            self._repo.ensure_collection(
                vector_size=vector_size,
                distance=self.distance,
                on_mismatch=self.on_mismatch,
            )
        inserted = self._repo.upsert_points(points)
        return int(inserted)

    # ---------- read/query ----------
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
        self.ensure_repo()
        assert self._repo is not None
        return self._repo.query_points(
            vector=vector,
            limit=limit,
            filters=filters,
            with_payload=with_payload,
            with_vectors=with_vectors,
            score_threshold=score_threshold,
            offset=offset,
        )

