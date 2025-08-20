"""
Adapter that uses Airflow's MongoHook to obtain a collection
and delegates persistence to MongoWriteRepository.
- Exposes dataset-specific upsert methods (e.g., dataset_a_upsert_many).
"""
import logging
import os
from __future__ import annotations
from typing import Any, Dict, Iterable, List, Mapping, Optional, Set, Tuple
from airflow.providers.mongo.hooks.mongo import MongoHook

from collector.repository.filter_provider import FilterProvider
from collector.repository.processed_page_repository import ProcessedPageRepository
from .mongo_write_repository import MongoWriteRepository
from .mongo_readonly_repository import MongoReadonlyRepository 

class MongoCollectionAdapter:
    # Processed marker collection name
    PROCESSED_COLL = "processed_pages"

    # Dataset key -> (collection_name, key_fields)
    MODEL_CFG: Mapping[str, Tuple[str, Tuple[str, ...]]] = {
        "dataset_a": ("dataset_a", ("id",)),
    }

    def __init__(
            self,
            mongo_conn_id: str = "mongo_default",
            database: str = "test",
            model_cfg: dict | None = None,
            env_overrides: dict | None = None
    ) -> None:
        hook = MongoHook(mongo_conn_id=mongo_conn_id)
        self.client = hook.get_conn()
        self.db = self.client[database]
        # 모델키 → (collection_name, key_fields)
        self.MODEL_CFG: Mapping[str, Tuple[str, Tuple[str, ...]]] = model_cfg or {
             "dataset_a": ("dataset_a", ("id",)),
        }

        # Optional env overrides for collection names (keep keys in sync if you use them)
        self.ENV_COLLECTION_OVERRIDES: Mapping[str, str] = env_overrides or {
            "dataset_a": os.getenv("DATASET_A", ""),
        }


    def __init__(self, mongo_conn_id: str = "mongo_default", database: str = "test") -> None:
        hook = MongoHook(mongo_conn_id=mongo_conn_id)
        client = hook.get_conn()
        self.db = client[database]

    def close(self) -> None:
        # Close underlying MongoClient (releases pooled sockets)
        try:
            self.client.close()
        except Exception:
            pass

    def __enter__(self):
        # Allow: with MongoCollectionAdapter(...) as adapter:
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()

    def _rw_repo(self, collection_name: str) -> MongoWriteRepository:
        col = self.db[collection_name]
        return MongoWriteRepository(col)
    
     # -------- repo factory (read-only) ----------------------------------------
    def _ro_repo(self, collection_name: str) -> MongoReadonlyRepository:
        """Build a read-only repository bound to the given collection name."""
        return MongoReadonlyRepository(self.db[collection_name])
    
    def _processed_repo(self) -> ProcessedPageRepository:
        """Return ProcessedPageRepository bound to 'processed_pages' collection."""
        return ProcessedPageRepository(self.db[self.PROCESSED_COLL])

    def _upsert_with_cfg(self, model_key: str, docs: Iterable[Dict]) -> int:
        if model_key not in self.MODEL_CFG:
            raise KeyError(f"Unknown model_key={model_key}")
        collection, key_fields = self.MODEL_CFG[model_key]
        # Early warning: verify at least one key field exists in the first doc
        first = next(iter(docs), None)
        if first is not None and not any(k in first for k in key_fields):
            import logging
            logging.getLogger(__name__).warning("docs seem to miss key_fields %s for %s", key_fields, model_key)
        repo = self._rw_repo(collection)
        return repo.upsert_many(docs, key_fields)

    def dataset_a_upsert_many(self, docs: Iterable[Dict]) -> int:
        return self._upsert_with_cfg("dataset_a", docs)

     # ---------- processed-page helpers (bypass to ProcessedPageRepository) ----------
    def list_processed_pages(self, api_name: str, run_date: str) -> Set[int]:
        """Bypass to ProcessedPageRepository.list_pages(...)."""
        return self._processed_repo().list_processed_pages(api_name, run_date)
        
    def is_processed(self, api_name: str, run_date: str, page_no: int) -> bool:
        """Bypass to ProcessedPageRepository.is_processed(...)."""
        return self._processed_repo().is_processed(api_name, run_date, page_no)

    def mark_processed(self, api_name: str, run_date: str, page_no: int, processed_size: int) -> None:
        """Bypass to ProcessedPageRepository.mark_processed(...)."""
        self._processed_repo().mark_processed(api_name, run_date, page_no, processed_size)

    def unmark_processed(self, api_name: str, run_date: str, page_no: int) -> int:
        """Bypass to ProcessedPageRepository.unmark_processed(...)."""
        return self._processed_repo().unmark_processed(api_name, run_date, page_no)

    # ---------- RAG Indexing helpers ----------
    def heartbeat_lease(self, collection_name: str, doc_id: str, owner: str, lease_seconds: int = 300) -> bool:
        return self._rw_repo(collection_name).heartbeat_lease(doc_id, owner, lease_seconds)
    
    def release_lease(self, collection_name: str, doc_id: str, owner: str) -> None:
        self._rw_repo(collection_name).release_lease(doc_id, owner)

    def release_stale_leases(self, collection_name: str) -> int:
        return self._rw_repo(collection_name).release_stale_leases()

    # ---------- Mark results ----------
    def mark_indexed(self, collection_name: str, doc_id: str, content_hash: Optional[str],
                     embedding_version: int) -> None:
        self._rw_repo(collection_name).mark_indexed(doc_id, content_hash, embedding_version)

    def mark_index_failed(self, collection_name: str, doc_id: str, owner: Optional[str] = None,
                          reason: Optional[str] = None) -> None:
        self._rw_repo(collection_name).mark_index_failed(doc_id, owner=owner, reason=reason)
    
    def build_page_anchors(self, model_key: str, embedding_version: int, page_size: int) -> List[Any]:
        """
        needs-index 필터로 전체를 _id 오름차순 단일 커서로 훑으며
        page_size마다 '시작 _id(anchor)'를 한 번씩만 추출한다.
        """
        needs = FilterProvider.build_needs_index_filter(embedding_version, safe_hash=True)
        collection_name = self.resolve_collection(model_key)

        # 배치/타임아웃은 환경변수로 조절 가능하게(없으면 보수적 기본값)
        batch_size = int(os.getenv("RAG_ANCHOR_BATCH", "10000"))
        max_time_ms = int(os.getenv("RAG_ANCHOR_MAX_TIME_MS", "120000"))  # 120s

        return self._ro_repo(collection_name).build_page_anchors(
            needs_filter=needs,
            page_size=page_size,
            batch_size=batch_size,
            max_time_ms=max_time_ms
        )