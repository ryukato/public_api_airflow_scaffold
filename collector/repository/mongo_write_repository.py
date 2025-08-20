"""
Repository that performs bulk upsert against a Mongo collection (pymongo only).
- Unit-test friendly (mongomock).
"""

from __future__ import annotations
import logging
from typing import Dict, Iterable, List, Sequence, Optional, Any, Union
from bson import ObjectId, errors as bson_errors
from collector.util.time_utility import TimeUtility
from pymongo.collection import Collection
from pymongo import UpdateOne, ASCENDING

logger = logging.getLogger(__name__)


class MongoWriteRepository:
    def __init__(self, collection: Collection) -> None:
        self.col = collection

    @staticmethod
    def _to_safe_id(doc_id: str) -> Union[ObjectId, str]:
        """
        Try ObjectId(doc_id); if invalid, fall back to the original string.
        This allows collections that use string _id.
        """
        try:
            return ObjectId(doc_id)
        except (bson_errors.InvalidId, TypeError, ValueError):
            return doc_id

    def _id_filter(
        self, doc_id: str, extra: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Build a filter dict with a safe _id + optional extra predicates.
        """
        f: Dict[str, Any] = {"_id": self._to_safe_id(doc_id)}
        if extra:
            f.update(extra)
        return f

    def upsert_many(self, docs: Iterable[Dict], key_fields: Sequence[str]) -> int:
        """
        Bulk upsert by key_fields. Filters out None values in the key to avoid accidental null matches.
        """
        ops: List[UpdateOne] = []
        for d in docs:
            # Only include key fields that are present and not None
            _filter = {k: d.get(k) for k in key_fields if d.get(k) is not None}
            ops.append(UpdateOne(_filter, {"$set": d}, upsert=True))

        if not ops:
            return 0

        res = self.col.bulk_write(ops, ordered=False)
        logger.info(
            "Upsert many to %s, upserted_count=%s, modified_count=%s, inserted_count=%s",
            self.col,
            res.upserted_count,
            res.modified_count,
            getattr(res, "inserted_count", 0),
        )
        return (res.upserted_count or 0) + (res.modified_count or 0)

    # ---------- RAG Indexing helpers ---------- 
    def heartbeat_lease(self, doc_id: str, owner: str, lease_seconds: int = 60) -> bool:
        """
        Extend lease expiration for the current owner. Returns True if extended.
        """
        now = TimeUtility.now_utc()
        expires = TimeUtility.expire_from(now, lease_seconds=lease_seconds)
        res = self.col.update_one(
            {"_id": ObjectId(doc_id), "indexLease.owner": owner},
            {"$set": {"indexLease.expiresAt": expires, "indexLease.heartbeatAt": now}},
        )
        return res.modified_count > 0
    
    def release_lease(self, doc_id: str, owner: str) -> None:
        """
        Release lease only if the same owner holds it (idempotent).
        """
        self.col.update_one(
            {"_id": ObjectId(doc_id), "indexLease.owner": owner},
            {"$unset": {"indexLease": ""}},
        )

    def release_stale_leases(self) -> int:
        """
        Release leases where expiresAt <= now. Returns modified count.
        """
        now = TimeUtility.now_utc()
        res = self.col.update_many(
            {"indexLease.expiresAt": {"$lte": now}},
            {"$unset": {"indexLease": ""}},
        )
        return int(res.modified_count)
    
    def mark_indexed(self, doc_id: str, content_hash: Optional[str], embedding_version: int) -> None:
        """
        Mark document as indexed (success) and remove any lease.
        """
        now = TimeUtility.now_utc()
        ch = content_hash or ""
        set_fields: Dict[str, Any] = {
            "indexed": {
                "at": now,
                "embeddingVersion": embedding_version,
                "contentHash": ch,
            },
            "contentHash": ch,  # <-- 원본도 항상 업데이트
        }
        if content_hash is not None:
            # Keep the latest contentHash on the document
            set_fields["contentHash"] = content_hash

        self.col.update_one(
            {"_id": ObjectId(doc_id)},
            {"$set": set_fields, "$unset": {"indexLease": ""}},
        )

    # TODO separate failure of indexing
    '''
        "indexLease" and "indexLease.owner" have to be in a source(not to be separated) 
        collection_name: raw_drug_indexing_failure
        fields
            - ref_doc_id: doc_id of source (e.g. raw_drug_summary)
            - data_type: e.g. raw_drug_summary
            - lastIndexErrorAt
            - lastIndexError
    '''
    def mark_index_failed(self, doc_id: str, owner: Optional[str] = None, reason: Optional[str] = None) -> None:
        """
        Mark failure metadata and release lease (if owned). Idempotent.
        """
        update: Dict[str, Any] = {
            "$set": {"lastIndexErrorAt": TimeUtility.now_utc()},
            "$unset": {"indexLease": ""},
        }
        if reason:
            update["$set"]["lastIndexError"] = reason

        if owner:
            # Only release if the same owner holds the lease
            self.col.update_one({"_id": ObjectId(doc_id), "indexLease.owner": owner}, update)
        else:
            # Unconditional release (fallback)
            self.col.update_one({"_id": ObjectId(doc_id)}, update)
