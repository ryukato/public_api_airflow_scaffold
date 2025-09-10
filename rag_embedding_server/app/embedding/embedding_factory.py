import logging
import os

from typing import List, Literal, Optional

import numpy as np
import torch
from dotenv import load_dotenv
from sentence_transformers import SentenceTransformer
from transformers import PreTrainedTokenizer, PreTrainedModel

# _get_proxy_settings()
os.environ["NO_PROXY"] = "*"
load_dotenv()
# 가벼운 헬퍼만 모듈 레벨에 유지
Family = Literal["minilm", "e5", "bge"]

logger = logging.getLogger(__name__)

# DEFAULT CONFIG OPTIONS
DEFAULT_RAG_EMBED_MODEL = "sentence-transformers/all-MiniLM-L6-v2"
DEFAULT_RAG_EMBED_DEVICE = "cpu"
DEFAULT_DIM = 384


# ------------ global functions ----------------
def get_env(name: str, default: str = None) -> str:
    value = os.getenv(name, default)
    if value is None:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def get_device(prefer_device: str, skip_mps_check: bool = False) -> str:
    """Returns best available device based on preference."""
    prefer_device = prefer_device.lower()
    if prefer_device == "cuda" and torch.cuda.is_available():
        return "cuda"
    elif prefer_device == "mps" and torch.backends.mps.is_available():
        # check if MPS is usable
        if skip_mps_check:
            return "mps"
        try:
            _ = torch.tensor([1.0], device="mps")
            return "mps"
        except Exception as e:
            logger.warning(f"MPS check failed: {e}")
    return "cpu"


def get_embedding_model():
    model_name = os.getenv("RAG_EMBED_MODEL", "sentence-transformers/all-MiniLM-L6-v2")
    # device = os.getenv("RAG_EMBED_DEVICE", "cpu")
    device = get_device(os.getenv("RAG_EMBED_DEVICE", "cpu"), skip_mps_check=False)
    if device == "mps" and not torch.backends.mps.is_available():
        logger.warning("MPS not available. Falling back to CPU.")
        device = "cpu"

    model = SentenceTransformer(model_name, device=device)
    logger.info(f"Using torch device: {device}, model: {model_name}")
    return model


def get_model_info():
    model_name = os.getenv("RAG_EMBED_MODEL", "sentence-transformers/all-MiniLM-L6-v2")
    device = os.getenv("RAG_EMBED_DEVICE", "cpu")
    return {
        "model_name": model_name,
        "device": device,
    }


class EmbeddingFactory:
    """
    Transformer-only embedding client:
    - HF transformers (AutoTokenizer/AutoModel)
    - Mean-pooling + L2 normalize
    - E5 prefix 자동 처리 (query:/passage:)
    - (embed_docs, embed_query, dim, warmup, config, log_config)
    """

    # ---------- 표준 생성자 ----------
    def __init__(
        self,
        model_name: Optional[str] = None,
        device: Optional[str] = None,
        skip_mps_check: bool = False,
    ):
        self.model_name = model_name or get_env(
            "RAG_EMBED_MODEL", DEFAULT_RAG_EMBED_MODEL
        )
        device = get_device(
            device or get_env("RAG_EMBED_DEVICE", DEFAULT_RAG_EMBED_DEVICE),
            skip_mps_check=skip_mps_check
            or get_env("RAG_EMBED_SKIP_MPS_CHECK", "false").lower() == "true",
        )
        self.device = device
        self.model = get_embedding_model()
        self.dim = int(os.getenv("EMBEDDING_DIM", DEFAULT_DIM))
        max_queue_size = get_env("MAX_QUEUE_SIZE", 1000)
        # 로그 출력
        logger.info(f"EmbeddingFactory initialized:")
        logger.info(f"  self.model_name: {self.model_name}")
        logger.info(f"  self.device: {self.device}")
        logger.info(f"  self.dim: {self.dim}")
        logger.info(f"  max-queue-size: {max_queue_size}")

    def encode(self, texts: List[str]) -> np.ndarray:
        if not texts:
            logger.warning("embed_docs(): received empty input")
            return np.empty((0, self.dim), dtype=np.float32)
        try:
            return self.model.encode(
                sentences=texts,
                normalize_embeddings=True,
                convert_to_numpy=True,
                show_progress_bar=False
            )
        except Exception:
            logger.exception("embed_docs() failed")
            raise

    def embed_query(self, text: str) -> np.ndarray:
        try:
            return self.model.encode(
                sentences=[text],
                normalize_embeddings=True,
                convert_to_numpy=True,
                show_progress_bar=False
            )

        except Exception:
            logger.exception("embed_query() failed")
            raise

    def warmup(self) -> None:
        try:
            _ = self.embed_query("warmup")
        except Exception:
            logger.exception("Warmup failed")
            raise


def get_embedding_factory() -> EmbeddingFactory:
    _embedder_instance = EmbeddingFactory()
    _embedder_instance.warmup()  # 최초 warmup
    return _embedder_instance
