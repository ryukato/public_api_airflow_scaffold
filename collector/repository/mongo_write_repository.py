"""
Repository that performs bulk upsert against a Mongo collection (pymongo only).
- Unit-test friendly (mongomock).
"""
from __future__ import annotations
from typing import Dict, Iterable, List, Sequence
from pymongo.collection import Collection
from pymongo import UpdateOne, ASCENDING

class MongoWriteRepository:
    def __init__(self, collection: Collection) -> None:
        self.col = collection

    @staticmethod
    def _build_ops(docs: Iterable[Dict], key_fields: Sequence[str]) -> List[UpdateOne]:
        ops: List[UpdateOne] = []
        for d in docs:
            filt = {k: d.get(k) for k in key_fields if d.get(k) is not None}
            ops.append(UpdateOne(filt, {"$set": d}, upsert=True))
        return ops

    def ensure_index(self, key_fields: Sequence[str]) -> None:
        # Ensure unique index for given key_fields (order matters for compound keys)
        self.col.create_index([(k, ASCENDING) for k in key_fields], unique=True, background=True, name="_".join(key_fields))

    def upsert_many(self, docs: Iterable[Dict], key_fields: Sequence[str], batch_size: int | None = None) -> int:
        """
        Perform bulk upsert; optionally chunk operations when batch_size is provided.
        Returns number of upserted+modified documents.
        """
        ops = self._build_ops(docs, key_fields)
        if not ops:
            return 0
        self.ensure_index(key_fields)

        total = 0
        if not batch_size or batch_size <= 0 or len(ops) <= batch_size:
            res = self.col.bulk_write(ops, ordered=False)
            return (res.upserted_count or 0) + (res.modified_count or 0)

        for i in range(0, len(ops), batch_size):
            chunk = ops[i : i + batch_size]
            res = self.col.bulk_write(chunk, ordered=False)
            total += (res.upserted_count or 0) + (res.modified_count or 0)
        return total
