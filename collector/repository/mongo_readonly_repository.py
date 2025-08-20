from __future__ import annotations

from typing import Any, Dict, List, Optional, Union

from bson import ObjectId, errors as bson_errors
from pymongo.collection import Collection

Doc = Dict[str, Any]
Projection = Dict[str, Union[int, bool]]  # e.g. {"_id": 1, "indexed": 1}

class MongoReadonlyRepository:
    """
    Read-only repository for fetching source documents used by the indexing pipeline.
    This class must not perform any write operations.
    """

    def __init__(self, collection: Collection) -> None:
        self.col = collection
    
    # noinspection PyMethodMayBeStatic
    def _to_oid(self, doc_id: str) -> Optional[ObjectId]:
        """Convert id string to ObjectId, returning None if invalid."""
        if doc_id is None:
            return None
        if isinstance(doc_id, ObjectId):
            return doc_id
        try:
            return ObjectId(doc_id)
        except (bson_errors.InvalidId, TypeError):
            return None
    
     # ---------- pagination helpers used by adapter ----------
    def get_total_pages(self, query: Dict[str, Any], page_size: int) -> int:
        """Return total number of pages for the given query and page size."""
        page_size = int(page_size)
        if page_size <= 0:
            return 0
        total = self.col.count_documents(query)
        return (total + page_size - 1) // page_size 
    
    def list_ids_by_page(self, query: Dict[str, Any], page_no: int, page_size: int) -> List[str]:
        """
        Return _id strings for a specific page (1-based) using the given query.
        Sorted by _id ascending to make pagination deterministic.
        Note: Locks/leases may still introduce slight drift across concurrent workers,
        so the writer should still attempt per-id lease acquisition.
        """
        page_no = int(page_no)
        page_size = int(page_size)
        if page_no < 1 or page_size <= 0:
            return []
        skip = (page_no - 1) * page_size
        cursor = (
            self.col.find(query, {"_id": 1})
            .sort([("_id", 1)])  # stable order
            .skip(skip)
            .limit(page_size)
        )
        return [str(d["_id"]) for d in cursor]

    def list_ids_in_id_range(
            self,
            needs_query: Dict[str, Any],
            start_id: Any | None,
            end_id: Any | None,
            limit: int
    ) -> List[str]:
        q = dict(needs_query)
        rng: Dict[str, Any] = {}

        start_oid = self._to_oid(start_id)
        end_oid = self._to_oid(end_id)

        if start_oid is not None:
            rng["$gte"] = start_oid  # ✅ 포함
        if end_oid is not None:
            rng["$lt"] = end_oid  # ✅ 제외

        if rng:
            q["_id"] = rng

        cur = (
            self.col.find(q, {"_id": 1})
            .sort([("_id", 1)])
            .limit(int(limit))
        )
        return [str(d["_id"]) for d in cur]

    def fetch_data(
            self,
            page: int,
            page_size: int,
            query_filter: Optional[dict] = None,
            projection: Optional[dict] = None,
    ) -> list[dict]:
        skip = (page - 1) * page_size
        query = query_filter or {}  # 기본: 조건 없음
        cursor = self.col.find(
            query,
            projection
        ).skip(skip).limit(page_size)

        return list(cursor)

    def count_data(self, query_filter: Optional[dict] = None) -> int:
        query = query_filter or {}
        return self.col.count_documents(query)