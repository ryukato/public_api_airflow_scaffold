"""
Repository that performs bulk upsert against a Mongo collection (pymongo only).
- Unit-test friendly (mongomock).
"""

from __future__ import annotations
import logging
from typing import Dict, Iterable, List, Sequence, Optional, Any, Union
from bson import ObjectId, errors as bson_errors
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
