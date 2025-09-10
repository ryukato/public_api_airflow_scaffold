import os
import time
import logging
from typing import List
from fastapi import APIRouter, Request
from app.models.schemas import TestRagChunk

router = APIRouter()
MAX_BATCH_ITEMS = int(os.getenv("MAX_BATCH_ITEMS", 500))
MAX_PAYLOAD_BYTES = int(os.getenv("MAX_PAYLOAD_BYTES", 5242880))

logger = logging.getLogger(__name__)

# TODO: 입력 유효성 검사 강화 (e.g., chunk_id 형식, 중복 제거 등)
# TODO: inference 요청에 대한 API rate-limit 또는 인증 기능 추가 고려
# TODO: Enqueue 성공 시 job_id 반환하고 async 상태 조회 가능하게 개선 (future work)

@router.post("/test/embed")
async def embed_chunks(chunks: List[TestRagChunk], request: Request):
    start = time.time()
    logger.info(f"[{request.client.host}] FastAPI handler entry.")
    if len(chunks) > MAX_BATCH_ITEMS:
            raise ValueError(f"Too many chunks. Max allowed is {MAX_BATCH_ITEMS}")
    
    total_bytes = sum(len(chunk.paragraph_contents.encode("utf-8")) for chunk in chunks)
    if total_bytes > MAX_PAYLOAD_BYTES:
        raise ValueError(f"Payload too large: {total_bytes} bytes, max-limit: {MAX_PAYLOAD_BYTES}")
    
    client_host = request.client.host
    count = await request.app.state.embedding_worker.enqueue(chunks, client_host)
    logger.info(f"[{request.client.host}] Enqueue complete. Handler duration: {time.time() - start:.3f}s")
    return {"status": "accepted", "count": count}


@router.get("/test/status")
async def get_embedding_status(request: Request, threshold: float = 0.7):

    worker = request.app.state.embedding_worker
    return worker.get_queue_status(usage_threshold=threshold)

@router.get("/model-info")
async def get_embedding_model_info():
    from app.embedding.embedding_factory import get_model_info
    return get_model_info()
