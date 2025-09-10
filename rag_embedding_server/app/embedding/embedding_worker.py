import asyncio
import logging
import os
import time
# from queue import Queue, Full
from asyncio import Queue
from typing import List

from dotenv import load_dotenv

from app.embedding.embedding_factory import EmbeddingFactory, get_embedding_factory
from app.embedding.embedding_repository import EmbeddingRepository
from app.embedding.embedding_vector_transformer import EmbeddingVectorTransformer
from app.models.schemas import TestRagChunk

load_dotenv()

MAX_BATCH_ITEMS = int(os.getenv("MAX_BATCH_ITEMS", 500))
MAX_PAYLOAD_BYTES = int(os.getenv("MAX_PAYLOAD_BYTES", 100000))
MAX_QUEUE_SIZE = int(os.getenv("MAX_QUEUE_SIZE", 500))

logger = logging.getLogger(__name__)

# TODO: [✓] graceful shutdown 완료
# TODO: embedding_factory.encode가 blocking이라면 run_in_executor로 분리 고려
# TODO: self.repository.upsert_many도 I/O이므로 async 버전으로 개선 가능
# TODO: queue full 시 exponential backoff + 재시도 로직 추가 고려
# TODO: queue size 모니터링용 Prometheus metrics 또는 OpenTelemetry 연동 고려

class EmbeddingWorker:
    def __init__(
            self,
            repository: EmbeddingRepository = None,
            embedding_factory: EmbeddingFactory = None,
            worker_count: int = 4
    ):
        self._shutdown_event = asyncio.Event()
        self._loop_task = None
        self.queue = Queue(maxsize=MAX_QUEUE_SIZE)
        if repository is not None:
            self.repository = repository
        else:
            self.repository = EmbeddingRepository()

        if embedding_factory is not None:
            self.embedding_factory = embedding_factory
        else:
            self.embedding_factory = get_embedding_factory()
        
        self.worker_count = worker_count
        self._workers: List[asyncio.Task] = []
    
    async def start(self):
        for i in range(self.worker_count):
            worker_task = asyncio.create_task(self._run_worker(i))
            self._workers.append(worker_task)
        logger.info(f"🔄 Started {self.worker_count} embedding workers.")

    async def enqueue(self, chunks: List[TestRagChunk], client_host: str) -> int:
        logger.info(f"Accepted chunks - size={len(chunks)}")

        try:
            # await self.queue.put((client_host, chunks))
            await asyncio.wait_for(self.queue.put((client_host, chunks)), timeout=5.0)
        except Exception as e:
            logger.exception("Async queue put failed")
            raise e

        return len(chunks) 

    @staticmethod
    def get_queue_size():
        return MAX_QUEUE_SIZE
    
    def get_queue_status(self, usage_threshold: float = 0.7) -> dict:
        """
        Returns current queue usage and availability based on threshold.
        """
        current_size = self.queue.qsize()
        available = current_size < int(MAX_QUEUE_SIZE * usage_threshold)
        return {
            "max_queue_size": MAX_QUEUE_SIZE,
            "current_task_queue_size": current_size,
            "queue_available": available,
            "threshold_ratio": usage_threshold,
        }

    def get_current_enqueue_count(self):
        return {
            "current_task_queue_size": self.queue.qsize()
        }
    
    async def _run_worker(self, worker_id: int):
        logger.info(f"🚀 Worker-{worker_id} started.")
        while not self._shutdown_event.is_set():
            try:
                client_host, chunks = await self.queue.get()
                logger.info(f"[Worker-{worker_id}] Starting embedding process {len(chunks)} chunks from {client_host}")
                await self._process_chunks(chunks, client_host, worker_id)
                self.queue.task_done()
            except Exception as e:
                logger.exception(f"[Worker-{worker_id}] Failed to process embedding job: {e}")

    async def _process_chunks(self, chunks: List[TestRagChunk], client_host: str, worker_id: int):
        start_time = time.perf_counter()
        try:
            texts = [EmbeddingVectorTransformer.transform(c) for c in chunks]
            vectors = await asyncio.to_thread(self.embedding_factory.encode, texts)
            # vectors = self.embedding_factory.encode(texts)
            await asyncio.to_thread(self.repository.upsert_many, chunks, vectors)
            # self.repository.upsert_many(chunks, vectors)
        except Exception as e:
            logger.exception(f"[Worker-{worker_id}] Embedding failed.")
        finally:
            duration = time.perf_counter() - start_time
            logger.info(f"[Worker-{worker_id}] Processed {len(chunks)} chunks. duration={duration:.2f} sec, remained: {self.queue.qsize()}")


    async def graceful_shutdown(self):
        logger.info("🛑 Graceful shutdown: stopping embedding workers...")
        self._shutdown_event.set()

        # Wait for all workers to finish
        await self.queue.join()
        logger.info("✅ All tasks in queue have been processed.")

        for task in self._workers:
            task.cancel()

        for task in self._workers:
            try:
                await task
            except asyncio.CancelledError:
                logger.info("🔁 Worker cancelled.")
