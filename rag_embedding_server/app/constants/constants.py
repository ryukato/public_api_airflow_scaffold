import os

MAX_BATCH_ITEMS = int(os.getenv("MAX_BATCH_ITEMS", "100"))
MAX_PAYLOAD_BYTES = int(os.getenv("MAX_PAYLOAD_BYTES", str(1 * 1024 * 1024)))  # 1MB
MAX_QUEUE_SIZE = int(os.getenv("MAX_QUEUE_SIZE", "100"))

QDRANT_HOST = os.getenv("QDRANT_HOST", "localhost")
QDRANT_PORT = int(os.getenv("QDRANT_PORT", "6333"))
