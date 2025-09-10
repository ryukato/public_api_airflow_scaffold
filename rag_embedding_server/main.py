import os
from dotenv import load_dotenv
import logging
import threading
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.embedding.embedding_factory import get_embedding_model
from app.embedding.embedding_repository import EmbeddingRepository
from app.embedding.embedding_worker import EmbeddingWorker
from app.routes.api import router

# TODO: FastAPI 실행 시 ENV 기반으로 worker_count 조절할 수 있도록 설정 연동
# TODO: lifespan 외에 exception 핸들링 글로벌 훅 추가 (e.g., logging uncaught exceptions)
# TODO: Uvicorn 재시작 없이 model hot-swap 가능하도록 구조 개선 (future work)

load_dotenv()

EMBEDDING_WORKDER_COUNT = int(os.getenv("EMBEDDING_WORKDER_COUNT", 5))

# ✅ 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)

logger = logging.getLogger(__name__)


def build_embedding_repository() -> EmbeddingRepository:
    return EmbeddingRepository()


def build_embedding_worker(
    embedding_repository: EmbeddingRepository,
) -> EmbeddingWorker:
    embedding_worker = EmbeddingWorker(repository=embedding_repository, worker_count=EMBEDDING_WORKDER_COUNT)

    # embedding_repository.ensure_collection()
    return embedding_worker


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("App startup: Initializing embedding worker...")
    embedding_repository = build_embedding_repository()
    app.state.embedding_repository = embedding_repository
    worker = build_embedding_worker(embedding_repository)
    await worker.start()
    app.state.embedding_worker = worker
    logger.info("Embedding worker started.")
    yield
    logger.info("App shutdown: Stopping embedding worker...")
    await app.state.embedding_worker.graceful_shutdown()
    logger.info("Embedding worker stopped.")


app = FastAPI(
    title="RAG Embedding Server",
    description="Handles batch embedding requests with queue-based backpressure",
    version="0.1.0",
    lifespan=lifespan,
)

app.include_router(router)
