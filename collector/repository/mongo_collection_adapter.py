"""
Adapter that uses Airflow's MongoHook to obtain a collection
and delegates persistence to MongoWriteRepository.
- Exposes dataset-specific upsert methods (e.g., dataset_a_upsert_many).
"""
from __future__ import annotations
from typing import Dict, Iterable, Mapping, Tuple
from airflow.providers.mongo.hooks.mongo import MongoHook
from .mongo_write_repository import MongoWriteRepository

class MongoCollectionAdapter:
    # Dataset key -> (collection_name, key_fields)
    MODEL_CFG: Mapping[str, Tuple[str, Tuple[str, ...]]] = {
        "dataset_a": ("dataset_a_raw", ("id",)),
    }

    def __init__(self, mongo_conn_id: str = "mongo_default", database: str = "test") -> None:
        hook = MongoHook(mongo_conn_id=mongo_conn_id)
        client = hook.get_conn()
        self.db = client[database]

    def _repo_for(self, collection_name: str) -> MongoWriteRepository:
        col = self.db[collection_name]
        return MongoWriteRepository(col)

    def _upsert_with_cfg(self, model_key: str, docs: Iterable[Dict], batch_size: int | None = None) -> int:
        if model_key not in self.MODEL_CFG:
            raise KeyError(f"Unknown model_key={model_key}")
        collection, key_fields = self.MODEL_CFG[model_key]
        # Early warning: verify at least one key field exists in the first doc
        first = next(iter(docs), None)
        if first is not None and not any(k in first for k in key_fields):
            import logging
            logging.getLogger(__name__).warning("docs seem to miss key_fields %s for %s", key_fields, model_key)
        repo = self._repo_for(collection)
        return repo.upsert_many(docs, key_fields, batch_size=batch_size)

    def dataset_a_upsert_many(self, docs: Iterable[Dict], batch_size: int | None = None) -> int:
        return self._upsert_with_cfg("dataset_a", docs, batch_size=batch_size)
