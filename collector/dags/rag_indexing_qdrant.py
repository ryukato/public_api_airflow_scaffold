from __future__ import annotations

import hashlib
import logging, os, json, requests
from datetime import timedelta
from typing import Any, Dict, List

import pendulum
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

from collector.indexer.chunker import extract_text, chunk_text
from collector.repository.filter_provider import FilterProvider
from collector.repository.mongo_collection_adapter import MongoCollectionAdapter
from collector.repository.qdrant_index_adapter import QdrantIndexAdapter
from collector.util.text_normalizer import normalize_tex, normalize_text


# _get_proxy_settings()
os.environ['NO_PROXY'] = '*'

# ---- Env / defaults -----------------------------------------------------------
MONGO_CONN_ID = os.getenv("MONGO_CONN_ID", "mongo_default")
MONGO_DB = os.getenv("MONGO_DB", "test")

# Which dataset (model_key) to index, and payload.type used in Qdrant
DATASET_MODEL_KEY = os.getenv("RAG_DATASET_MODEL_KEY", "dataset_model_key")
RAG_DATASET_TYPE = os.getenv("RAG_DATASET_TYPE", "data_type")

QDRANT_COLLECTION = os.getenv("QDRANT_COLLECTION", "qdrant_doc")
QDRANT_HOST = os.getenv("QDRANT_HOST", "localhost")
QDRANT_PORT = int(os.getenv("QDRANT_PORT", "6333"))
EMBEDDING_VERSION = int(os.getenv("EMBEDDING_VERSION", "1"))
EMBED_MODEL_NAME = os.getenv("RAG_EMBED_MODEL", "intfloat/multilingual-e5-small")  # 경량 모델
CACHE_DIR = os.getenv("HF_HOME") or os.path.expanduser("~/.cache/huggingface")
PAGE_SIZE = int(os.getenv("RAG_PAGE_SIZE", "500"))

CHUNK_MAX_CHARS = int(os.getenv("CHUNK_MAX_CHARS", "1600"))
CHUNK_OVERLAP = int(os.getenv("CHUNK_OVERLAP", "80"))
EMBED_BATCH_SIZE = int(os.getenv("EMBED_BATCH_SIZE", "512"))

STALE_LOCK_MINUTES = int(os.getenv("STALE_LOCK_MINUTES", "30"))  # unused below; leases auto-expire
LOCK_BATCH = int(os.getenv("RAG_LOCK_BATCH", "150"))
LEASE_SECONDS = int(os.getenv("RAG_LEASE_SECONDS", "300"))

seoul_tz = pendulum.timezone("Asia/Seoul")

# noinspection PyTypeChecker
def stable_chunk_hash(text: str) -> str:
    return hashlib.sha1(normalize_text(text).encode("utf-8")).hexdigest()  # ✅ 동일 정규화


# noinspection PyPep8Naming
def create_qdrant_index_adapter():
    logger = logging.getLogger(__name__)
    logger.info("Qdrant: host=%s port=%s collection=%s", QDRANT_HOST, QDRANT_PORT, QDRANT_COLLECTION)
    qdrant = QdrantIndexAdapter(
        collection=QDRANT_COLLECTION,
        distance="cosine",
        ensure_collection=True,
        host=QDRANT_HOST,
        port=QDRANT_PORT,
    )
    qdrant.ensure_repo()
    return qdrant


def build_payload(d, d_id, *, content_hash: str):
    return {
        "type": RAG_DATASET_TYPE,
        "docId": d_id,
        "embeddingVersion": EMBEDDING_VERSION,
        "chunkCount": 1,
        "chunkSize": CHUNK_MAX_CHARS,
        "overlap": CHUNK_OVERLAP,
        # add some properties from model

        # 필요시 원본 alias도 보관(선택)
        "_alias": {
            # Add alias for the items properties
        }
    }

def _chunk_document(src: Dict[str, Any]) -> List[str]:
    raw = extract_text(src)
    canon = normalize_text(raw)  # ✅ 청킹 입력 정규화
    return chunk_text(canon, max_chars=CHUNK_MAX_CHARS, overlap=CHUNK_OVERLAP)

def _heartbeat_all(adapter: MongoCollectionAdapter, collection: str, ids: List[str], owner: str, lease_seconds: int) -> None:
    """Best-effort heartbeat for a batch of leases."""
    for d_id in ids:
        try:
            adapter.heartbeat_lease(collection_name=collection, doc_id=d_id, owner=owner, lease_seconds=lease_seconds)
        except Exception:
            logging.getLogger(__name__).warning("Heartbeat failed for %s", d_id, exc_info=True)



def process_indexing_per_page(range_spec: dict) -> int:
    """
    페이지 단위 인덱싱 (문서 1건 = 포인트 1건):
      - 페이지 내 문서를 개별 처리(안정성↑)
      - 포인트 ID는 UUIDv5(doc_id|v{version})로 생성
      - 성공 시 Mongo에 indexed.embeddingVersion 마킹
    """
    import uuid
    import time
    import socket
    import logging

    logger = logging.getLogger(__name__)
    total_upserts = 0
    start = range_spec.get("start")
    end = range_spec.get("end")
    owner = f"{socket.gethostname()}:{os.getpid()}:start{start}:end{end}"

    # 내부 헬퍼: 임의의 문자열 doc_id -> 안정적인 UUIDv5, TODO re-check to use only doc_id for point_id 
    def _to_uuid_point_id(doc_id: str, *, version: int) -> str:
        return str(uuid.uuid5(uuid.NAMESPACE_URL, f"doc:{doc_id}|v{version}"))

    logger.debug("Starting process indexing for start=%s end=%s owner=%s", start, end, owner)

    with MongoCollectionAdapter(mongo_conn_id=MONGO_CONN_ID, database=MONGO_DB) as adapter:
        coll = adapter.resolve_collection(DATASET_MODEL_KEY)
        logger.info("Start processing indexing for start=%s end=%s collection=%s", start, end, coll)

        # 임베딩 팩토리 (env 자동 로딩/로깅)
        from collector.indexer.embedding_factory import EmbeddingFactory
        ef = EmbeddingFactory.from_env()
        logger.info("Completed creating EmbeddingFactory")

        # Qdrant 어댑터 (프리플라이트 포함)
        try:
            qdrant = create_qdrant_index_adapter()
            logger.info("Completed creating Qdrant index adapter")
        except Exception:
            logger.exception("Fail to create Qdrant index adapter")
            raise

        # Stale leases cleanup (best-effort)
        try:
            released = adapter.release_stale_leases(collection_name=coll)
            logger.debug("Released stale leases at start: %s", released)
        except Exception:
            logger.exception("Lease cleanup failed (continuing)")

        # 대상 문서 id 목록(해당 페이지)
        ids = adapter.list_ids_by_anchor_range(coll, EMBEDDING_VERSION, start, end, PAGE_SIZE)
        logger.info("Fetched candidate IDs: start=%s end=%s, collection=%s, size=%s", start, end, coll, len(ids))
        if not ids:
            return 0

        # 컬렉션 스키마 보장을 첫 성공 시 1회만 수행하기 위해 기억
        ensured_vector_dim: int | None = None

        # 페이지 통계
        page_stats = {
            "ids": len(ids),
            "leases_acquired": 0,
            "fetched": 0,
            "empty_text": 0,
            "embedded": 0,
            "upserted": 0,
            "marked_indexed": 0,
            "marked_failed": 0,
        }

        # 문서 단건 처리 루프
        t0 = time.monotonic()
        for doc_id in ids:
            # 리스 획득
            got = adapter.acquire_lease_if_needs_index(
                collection_name=coll,
                doc_id=doc_id,
                embedding_version=EMBEDDING_VERSION,
                owner=owner,
                lease_seconds=LEASE_SECONDS,
            )
            if not got:
                continue
            page_stats["leases_acquired"] += 1

            try:
                # 문서 조회
                d = adapter.fetch_full_by_id(coll, doc_id)
                if not d:
                    logger.warning("Doc not found: %s", doc_id)
                    continue
                page_stats["fetched"] += 1

                # 텍스트 준비 (extract_text 사용, 안전망으로 normalize)
                raw = extract_text(d)
                text = normalize_text(raw or "")
                # 텍스트 해시 (증분 판단 및 기록용)
                if not text.strip():
                    # 내용 없으면 인덱싱만 마킹하고 스킵(재시도 방지)
                    adapter.mark_indexed(
                        collection_name=coll,
                        doc_id=str(d["_id"]),
                        content_hash=d.get("contentHash"),
                        embedding_version=EMBEDDING_VERSION,
                    )
                    page_stats["empty_text"] += 1
                    logger.info("Empty text; marked indexed and skipped: %s", d["_id"])
                    continue

                doc_hash = stable_chunk_hash(text if text is not None else "")
                # (선택) 변경 감지: 동일 해시 & 동일 버전이면 스킵
                prev_idx = (d.get("indexed") or {})
                prev_hash = prev_idx.get("contentHash") or d.get("contentHash") or ""
                prev_ver = prev_idx.get("embeddingVersion")
                if prev_hash and prev_hash == doc_hash and prev_ver == EMBEDDING_VERSION:
                    # 이미 동일 콘텐츠/버전 처리 → 마킹만 최신화(타임스탬프 갱신 용도) 후 스킵
                    adapter.mark_indexed(
                        collection_name=coll,
                        doc_id=str(d["_id"]),
                        content_hash=doc_hash,
                        embedding_version=EMBEDDING_VERSION,
                    )
                    page_stats["skipped_same_hash"] += 1
                    logger.info("Same content/version; refreshed mark and skipped: %s", d["_id"])
                    continue

                # 단건 임베딩
                _heartbeat_all(adapter, coll, [doc_id], owner, LEASE_SECONDS)
                vec = ef.embed_docs([text])[0]
                vector_dim = len(vec)
                if not vector_dim:
                    raise RuntimeError("Vector dimension is 0")
                page_stats["embedded"] += 1

                # Qdrant 컬렉션 보장 (최초 1회)
                if ensured_vector_dim is None:
                    qdrant.ensure_collection(vector_size=vector_dim)
                    ensured_vector_dim = vector_dim
                elif ensured_vector_dim != vector_dim:
                    logger.warning("Vector dim changed %s -> %s; ensuring collection again",
                                   ensured_vector_dim, vector_dim)
                    qdrant.ensure_collection(vector_size=vector_dim)
                    ensured_vector_dim = vector_dim

                # 포인트 업서트 (UUIDv5 사용)
                pid = _to_uuid_point_id(str(d["_id"]), version=EMBEDDING_VERSION)
                payload = build_payload(d, str(d["_id"]), content_hash=doc_hash)
                upserted = qdrant.upsert_points(
                    points=[{"id": pid, "vector": vec, "payload": payload}],
                    vector_size=vector_dim,
                )
                total_upserts += int(upserted or 0)
                page_stats["upserted"] += int(upserted or 0)

                # 마킹 (동일 해시 기록)
                adapter.mark_indexed(
                    collection_name=coll,
                    doc_id=str(d["_id"]),
                    content_hash=doc_hash,
                    embedding_version=EMBEDDING_VERSION,
                )
                page_stats["marked_indexed"] += 1

            except Exception as e:
                logger.exception("Doc indexing failed for %s: %s", doc_id, e)
                try:
                    adapter.mark_index_failed(
                        collection_name=coll, doc_id=doc_id, owner=owner, reason=str(e)
                    )
                    page_stats["marked_failed"] += 1
                except Exception:
                    logger.warning("mark_index_failed also failed for %s", doc_id, exc_info=True)
                # 계속 다음 문서로 진행
            finally:
                # 리스 해제(베스트 에포트)
                try:
                    adapter.release_lease(collection_name=coll, doc_id=doc_id, owner=owner)
                except Exception:
                    pass

        # 문서 단위 통계 로그
        logger.info(
            "PAGE STATS elapsed=%.2fs start=%s end=%s :: ids=%s leases=%s fetched=%s empty_text=%s "
            "embedded=%s upserted=%s marked_indexed=%s marked_failed=%s",
            time.monotonic() - t0,
            start,
            end,
            page_stats["ids"],
            page_stats["leases_acquired"],
            page_stats["fetched"],
            page_stats["empty_text"],
            page_stats["embedded"],
            page_stats["upserted"],
            page_stats["marked_indexed"],
            page_stats["marked_failed"],
        )

    return int(total_upserts)

@dag(
    dag_id="rag_indexing_qdrant",
    schedule="@daily",
    start_date=pendulum.datetime(2025, 8, 1, tz=seoul_tz),
    catchup=False,
    tags=["rag", "qdrant", "indexing"],
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
)
def rag_indexing_qdrant():
    """RAG indexing DAG using Qdrant; all DB I/O goes through adapters."""

    @task(task_id="debug_env")
    def debug_env():
        import os, pathlib, json, platform
        keys = [
            "HF_HOME",
            "HUGGINGFACE_HUB_CACHE",
            "SENTENCE_TRANSFORMERS_HOME",
            "TORCH_HOME",
            "TRANSFORMERS_OFFLINE",
            "HF_HUB_OFFLINE",
            "RAG_EMBED_LOCAL_ONLY",
            "PYTHONPATH",
            "AIRFLOW_HOME",
        ]
        env = {k: os.getenv(k) for k in keys}
        home = os.path.expanduser("~")
        hf_home = env.get("HF_HOME") or str(pathlib.Path(home) / ".cache" / "huggingface")
        p = pathlib.Path(hf_home)
        sample_files = []
        if p.exists():
            for i, f in enumerate(p.rglob("*")):
                sample_files.append(str(f))
                if i >= 10:
                    break
        info = {
            "python": platform.python_version(),
            "home": home,
            "resolved_HF_HOME": hf_home,
            "exists": p.exists(),
            "env": env,
            "sample_files": sample_files,
        }
        logging.getLogger(__name__).info(json.dumps(info, ensure_ascii=False, indent=2))
        return info

     @task(
        task_id="xformers_smoketest",
        retries=3,
        retry_delay=pendulum.duration(seconds=30),
        execution_timeout=pendulum.duration(minutes=10),
    )
    def xformers_smoketest() -> int:
        """
        transformers만으로 로컬 캐시에서 모델 로딩 + mean-pool 임베딩 길이 확인.
        """
        import os, glob, pathlib, logging
        from transformers import AutoTokenizer, AutoModel
        import torch

        logger = logging.getLogger(__name__)

        base = os.getenv("HF_HOME") or os.path.expanduser("~/.cache/huggingface")
        repo = EMBED_MODEL_NAME
        pattern = str(pathlib.Path(base) / f"models--{repo.replace('/', '--')}" / "snapshots" / "*")
        snaps = sorted(glob.glob(pattern))
        if not snaps:
            raise RuntimeError(f"No local snapshot for {repo}; expected like {pattern}")
        # noinspection PyTypeChecker
        local_dir = snaps[-1]
        logger.info("Using local_dir=%s", local_dir)

        tok = AutoTokenizer.from_pretrained(local_dir, cache_dir=base, local_files_only=True, trust_remote_code=True)
        mdl = AutoModel.from_pretrained(local_dir, cache_dir=base, local_files_only=True, trust_remote_code=True)
        mdl.eval()
        with torch.no_grad():
            b = tok(["query: warmup"], padding=True, truncation=True, return_tensors="pt")
            out = mdl(**b)
            mask = b["attention_mask"].unsqueeze(-1).expand(out.last_hidden_state.size()).float()
            summed = (out.last_hidden_state * mask).sum(dim=1)
            counts = mask.sum(dim=1).clamp(min=1e-9)
            vec = summed / counts
            vec = vec / (vec.norm(p=2, dim=-1, keepdim=True).clamp(min=1e-9))
            dim = int(vec.shape[-1])
        logger.info("transformers warmup ok. dim=%d", dim)
        return dim
    
    @task(
        task_id="warmup_embeddings",
        retries=3,
        retry_exponential_backoff=True,
        retry_delay=pendulum.duration(seconds=30),
        execution_timeout=pendulum.duration(minutes=10),
    )
    def warmup_embeddings():
        import os, pathlib, logging, glob, json

        logger = logging.getLogger(__name__)

        # 1) 태스크 실행 시점에 환경변수 확정
        airflow_home = os.getenv("AIRFLOW_HOME") or os.path.expanduser("~/airflow")
        hf_home = os.getenv("HF_HOME") or os.path.join(airflow_home, ".hf_cache")
        os.environ["HF_HOME"] = hf_home
        os.environ.setdefault("TRANSFORMERS_OFFLINE", "1")
        os.environ.setdefault("HF_HUB_OFFLINE", "1")
        os.environ.setdefault("RAG_EMBED_LOCAL_ONLY", "true")
        os.environ.setdefault("RAG_EMBED_DEVICE", "cpu")

        pathlib.Path(hf_home).mkdir(parents=True, exist_ok=True)

        # 2) 적용된 핵심 환경변수 값 로깅
        env_keys = [
            "AIRFLOW_HOME",
            "HF_HOME",
            "HUGGINGFACE_HUB_CACHE",
            "SENTENCE_TRANSFORMERS_HOME",
            "TORCH_HOME",
            "TRANSFORMERS_OFFLINE",
            "HF_HUB_OFFLINE",
            "RAG_EMBED_LOCAL_ONLY",
            "RAG_EMBED_DEVICE",
            "RAG_EMBED_MODEL",
        ]
        applied_env = {k: os.getenv(k) for k in env_keys if os.getenv(k) is not None}
        logger.info("warmup_embeddings: Applied ENV → %s", json.dumps(applied_env, indent=2))

        # 3) 로컬 스냅샷 존재 여부 확인
        pat = str(pathlib.Path(hf_home) / f"models--{EMBED_MODEL_NAME.replace('/', '--')}" / "snapshots" / "*")
        snaps = sorted(glob.glob(pat))
        logger.info("warmup_embeddings: model=%s, HF_HOME=%s, local_snapshots=%d", EMBED_MODEL_NAME, hf_home, len(snaps))
        if not snaps and os.getenv("TRANSFORMERS_OFFLINE", "0") == "1":
            logger.warning("No local snapshot found at %s (TRANSFORMERS_OFFLINE=1). Warmup may fail.", pat)

        # 4) 명시 cache_dir로 로딩
        from collector.indexer.embedding_factory import EmbeddingFactory
        ef = EmbeddingFactory.from_env()
        ef.warmup()
        logger.info("EmbeddingFactory warmup completed. dim=%d", ef.dim())

    @task(
        retries=5,
        retry_exponential_backoff=True,
        retry_delay=pendulum.duration(seconds=10),
        execution_timeout=pendulum.duration(minutes=10),
    )
    def release_stale_leases_task() -> int:
        """Release expired leases before starting indexing fan-out."""
        logger = logging.getLogger(__name__)
        with MongoCollectionAdapter(mongo_conn_id=MONGO_CONN_ID, database=MONGO_DB) as adapter:
            coll = adapter.resolve_collection(DATASET_MODEL_KEY)
            released = adapter.release_stale_leases(collection_name=coll)
            logger.info("Released stale leases: collection=%s, released=%s", coll, released)
            return int(released)

    @task(
        retries=5,
        retry_exponential_backoff=True,
        retry_delay=pendulum.duration(seconds=10),
        execution_timeout=pendulum.duration(minutes=10),
    )
    def get_page_anchors() -> List[dict]:
        import logging, json
        logger = logging.getLogger(__name__)

        with MongoCollectionAdapter(mongo_conn_id=MONGO_CONN_ID, database=MONGO_DB) as adapter:
            anchors = adapter.build_page_anchors(DATASET_MODEL_KEY, EMBEDDING_VERSION, PAGE_SIZE)

            # [(start, end)] 형태로 변환
            ranges = []
            for i, s in enumerate(anchors):
                e = anchors[i + 1] if i + 1 < len(anchors) else None
                ranges.append({"start": s, "end": e})

            # noinspection PyTypeChecker
            logger.info("get_page_anchors: anchors=%d, page_size=%d, first=%s, last=%s",
                     len(anchors), PAGE_SIZE, anchors[0] if anchors else None, anchors[-1] if anchors else None)
            logger.debug("get_page_anchors:ranges sample=%s", json.dumps(ranges[:3], ensure_ascii=False))
            return ranges
    
    @task(
        retries=10,
        retry_exponential_backoff=True,
        retry_delay=pendulum.duration(seconds=10),
        execution_timeout=pendulum.duration(minutes=30),
        pool="rag_indexing",
        pool_slots=1,
    )
    def index_page(range_spec: dict) -> int:
        return process_indexing_per_page(range_spec)
    
    
    @task(
        task_id="raw_data_indexing",
        retries=0,
        execution_timeout=pendulum.duration(minutes=5),
    )
    def raw_data_indexing() -> dict:
        """
        Qdrant / Mongo 요약치를 집계해 최종 로그로 남긴다.
          - Qdrant: type=final_summary, embeddingVersion=EMBEDDING_VERSION 포인트 수
          - Mongo : indexed.embeddingVersion == EMBEDDING_VERSION 문서 수
          - Mongo : needs-index 잔여 수 (0이어야 OK)
        """
        log = logging.getLogger(__name__)
        # --- Qdrant 집계 ---
        qhost = os.getenv("QDRANT_HOST", QDRANT_HOST)
        qport = int(os.getenv("QDRANT_PORT", str(QDRANT_PORT)))
        qcoll = os.getenv("QDRANT_COLLECTION", QDRANT_COLLECTION)

        qurl = f"http://{qhost}:{qport}/collections/{qcoll}/points/count"
        qpayload = {
            "filter": {
                "must": [
                    {"key": "type", "match": {"value": RAG_DATASET_TYPE}},
                    {"key": "embeddingVersion", "match": {"value": EMBEDDING_VERSION}},
                ]
            },
            "exact": True,
        }
        try:
            r = requests.post(qurl, json=qpayload, timeout=10)
            r.raise_for_status()
            qdrant_points = int(r.json()["result"]["count"])
        except Exception as e:
            log.exception("Qdrant count failed")
            qdrant_points = -1  # 에러 표식

        # --- Mongo 집계 ---
        with MongoCollectionAdapter(mongo_conn_id=MONGO_CONN_ID, database=MONGO_DB) as adapter:
            coll = adapter.resolve_collection(DATASET_MODEL_KEY)
            # 1) indexed 완료 수
            indexed_query = {"indexed.embeddingVersion": EMBEDDING_VERSION}
            indexed_count = adapter.db[coll].count_documents(indexed_query)

            # 2) needs-index 잔여 수
            needs_query = FilterProvider.build_needs_index_filter(EMBEDDING_VERSION, safe_hash=True)
            needs_count = adapter.db[coll].count_documents(needs_query)

        final_summary = {
            "qdrant": {
                "collection": qcoll,
                "points_count": qdrant_points,
                "type": RAG_DATASET_TYPE,
                "embeddingVersion": EMBEDDING_VERSION,
            },
            "mongo": {
                "collection": coll,
                "indexed_count": indexed_count,
                "needs_index_remaining": needs_count,
            },
            "page_size": PAGE_SIZE,
        }

        # 한 줄 요약 로그
        log.info(
            "SUMMARY | Qdrant(points=%s, type=%s, v=%s) | Mongo(indexed=%s, needs=%s) | page_size=%s",
            qdrant_points, RAG_DATASET_TYPE, EMBEDDING_VERSION, indexed_count, needs_count, PAGE_SIZE
        )
        # 상세(JSON) 로그
        log.debug("SUMMARY_JSON: %s", json.dumps(final_summary, ensure_ascii=False, indent=2))
        return final_summary
    
     # 간단 체인: 환경 확인 → transformers 스모크 → 워밍업
    # debug_env() >> xformers_smoketest() >> warmup_embeddings()
    # 실제 인덱싱 체인은 필요 시 아래로 확장
    start = EmptyOperator(task_id="s")
    end = EmptyOperator(task_id="e", trigger_rule=TriggerRule.ALL_DONE)
    cleaned = release_stale_leases_task()
    page_anchors = get_page_anchors()
    indexing_per_page = index_page.expand(range_spec=page_anchors)
    summary = raw_data_indexing()
    start >> warmup_embeddings() >> cleaned >> page_anchors >> indexing_per_page >> summary >> end
    # s >> warmup_embeddings() >> e


dag = rag_indexing_qdrant()