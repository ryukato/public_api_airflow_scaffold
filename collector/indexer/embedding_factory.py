# -*- coding: utf-8 -*-
# EmbeddingFactory (transformers-only backend), normalized vectors, env-aware factory
from __future__ import annotations
import logging
import os
import json
from typing import List, Literal, Optional, Dict

# 가벼운 헬퍼만 모듈 레벨에 유지
Family = Literal["minilm", "e5", "bge"]
logger = logging.getLogger(__name__)

def _detect_family(model_name: str) -> Family:
    name = (model_name or "").lower()
    if "e5" in name: return "e5"
    if "bge" in name: return "bge"
    return "minilm"


class EmbeddingFactory:
    """
    Transformer-only embedding client:
    - HF transformers (AutoTokenizer/AutoModel)
    - Mean-pooling + L2 normalize
    - E5 prefix 자동 처리 (query:/passage:)
    - (embed_docs, embed_query, dim, warmup, config, log_config)
    - from_env(): 환경변수 기반 생성기 (권장)
    """

    # ---------- 권장 진입점 ----------
    # noinspection PyBroadException
    @classmethod
    def from_env(cls, *, model_name=None, cache_dir=None, normalize=None,
                 device=None, local_only=None, trust_remote_code=None,
                 batch_size=None, max_length=None, log_env: bool=True) -> "EmbeddingFactory":
        import json, glob, pathlib
        if log_env:
            env_keys = [
                "AIRFLOW_HOME","HF_HOME","HUGGINGFACE_HUB_CACHE",
                "SENTENCE_TRANSFORMERS_HOME","TORCH_HOME",
                "TRANSFORMERS_OFFLINE","HF_HUB_OFFLINE",
                "RAG_EMBED_LOCAL_ONLY","RAG_EMBED_DEVICE","RAG_EMBED_SKIP_MPS_CHECK",
                "RAG_EMBED_MODEL","RAG_EMBED_BATCH","RAG_EMBED_MAX_LENGTH","RAG_TRUST_REMOTE_CODE",
            ]
            snap = {k: os.getenv(k) for k in env_keys if os.getenv(k) is not None}
            logger.info("EmbeddingFactory.from_env: ENV → %s", json.dumps(snap, indent=2, ensure_ascii=False))

        model_name = model_name or os.getenv("RAG_EMBED_MODEL", "intfloat/multilingual-e5-small")
        cache_dir  = cache_dir  or os.getenv("HF_HOME") or os.path.expanduser("~/.cache/huggingface")
        normalize  = bool(normalize) if normalize is not None else True
        device     = device or os.getenv("RAG_EMBED_DEVICE")
        # offline 신호가 있으면 local_only 자동 True
        offline = os.getenv("TRANSFORMERS_OFFLINE","0") == "1" or os.getenv("HF_HUB_OFFLINE","0") == "1"
        local_only = (local_only if local_only is not None
                      else (os.getenv("RAG_EMBED_LOCAL_ONLY","false").lower() in ("1","true","yes") or offline))
        trust_remote_code = (trust_remote_code if trust_remote_code is not None
                             else os.getenv("RAG_TRUST_REMOTE_CODE","true").lower() in ("1","true","yes"))
        batch_size = batch_size or int(os.getenv("RAG_EMBED_BATCH","64"))
        max_length = max_length or int(os.getenv("RAG_EMBED_MAX_LENGTH","512"))

        # 스냅샷 존재 여부 힌트
        try:
            base = pathlib.Path(cache_dir)
            pat = str(base / f"models--{model_name.replace('/','--')}" / "snapshots" / "*")
            snaps = glob.glob(pat)
            logger.info("EmbeddingFactory.from_env: local snapshots for %s → %d (cache_dir=%s)",
                        model_name, len(snaps), cache_dir)
        except Exception:
            logger.debug("Snapshot probe skipped", exc_info=True)

        return cls(model_name=model_name, cache_dir=cache_dir, normalize=normalize,
                   device=device, local_only=local_only, trust_remote_code=trust_remote_code,
                   batch_size=batch_size, max_length=max_length)


    # ---------- 표준 생성자 ----------
    def __init__(self, model_name=None, cache_dir=None, normalize: bool=True, *,
                 device: Optional[str]=None, local_only: bool=False, trust_remote_code: bool=True,
                 batch_size: int=64, max_length: int=512):
        from transformers import AutoTokenizer, AutoModel  # 지연 import
        logger.info("EF:init: begin (model=%s)", model_name or "intfloat/multilingual-e5-small")

        self.model_name = model_name or "intfloat/multilingual-e5-small"
        self.family: Family = _detect_family(self.model_name)
        self.cache_dir = cache_dir or os.path.expanduser("~/.cache/huggingface")
        self.local_only = bool(local_only)
        self.trust_remote_code = bool(trust_remote_code)
        self.normalize = bool(normalize)
        self.batch_size = int(batch_size)
        self.max_length = int(max_length)

        # torch import & device 결정
        logger.info("EF:init: importing torch...")
        import torch  # noqa
        logger.info("EF:init: torch imported. resolving device...")

        # MPS 체크 스킵 옵션
        skip_mps = os.getenv("RAG_EMBED_SKIP_MPS_CHECK", "0") in ("1","true","yes")
        self.device = self._resolve_device(device, torch, skip_mps=skip_mps)
        logger.info("EF:init: device resolved → %s (skip_mps_check=%s)", self.device, skip_mps)

        # 토크나이저/모델 로드
        logger.info("EF:init: loading tokenizer (local_only=%s, cache_dir=%s)...", self.local_only, self.cache_dir)
        self._tok = AutoTokenizer.from_pretrained(
            self.model_name, cache_dir=self.cache_dir, local_files_only=self.local_only,
            trust_remote_code=self.trust_remote_code,
        )
        logger.info("EF:init: tokenizer loaded: %s", self._tok.__class__.__name__)

        logger.info("EF:init: loading model...")
        mdl = AutoModel.from_pretrained(
            self.model_name, cache_dir=self.cache_dir, local_files_only=self.local_only,
            trust_remote_code=self.trust_remote_code,
        )
        logger.info("EF:init: model weights loaded. moving to device...")
        self._mdl = mdl.to(self.device)
        self._mdl.eval()
        logger.info("EF:init: model moved to %s and set eval()", self.device)

        # 구성 로그
        self.log_config(extra={
            "transformers_offline": os.getenv("TRANSFORMERS_OFFLINE"),
            "hf_hub_offline": os.getenv("HF_HUB_OFFLINE"),
        })

    # ---------- 내부 헬퍼 ----------
    @staticmethod
    def _auto_device(torch_mod, *, skip_mps: bool=False) -> str:
        if getattr(torch_mod, "cuda", None) and torch_mod.cuda.is_available():
            return "cuda"
        if not skip_mps:
            mps_ok = getattr(torch_mod.backends, "mps", None) and torch_mod.backends.mps.is_available()
            if mps_ok:
                return "mps"
        return "cpu"


    def _resolve_device(self, desired: Optional[str], torch_mod, *, skip_mps: bool=False) -> str:
        if desired:
            d = desired.lower()
            if d == "cuda":
                if torch_mod.cuda.is_available():
                    return "cuda"
                logger.warning("RAG_EMBED_DEVICE=cuda 지정됐지만 CUDA 미가용 → auto")
                return self._auto_device(torch_mod, skip_mps=skip_mps)
            if d == "mps":
                if not skip_mps:
                    mps_ok = getattr(torch_mod.backends, "mps", None) and torch_mod.backends.mps.is_available()
                    if mps_ok:
                        return "mps"
                logger.warning("RAG_EMBED_DEVICE=mps 지정됐지만 MPS 미가용/스킵 → auto")
                return self._auto_device(torch_mod, skip_mps=skip_mps)
            if d == "cpu":
                return "cpu"
            logger.warning("알 수 없는 RAG_EMBED_DEVICE=%s → auto", desired)
        return self._auto_device(torch_mod, skip_mps=skip_mps)
    
    @staticmethod
    def _l2norm_tensor(torch_mod, x):
        return x / (x.norm(p=2, dim=-1, keepdim=True).clamp(min=1e-9))

    @staticmethod
    def _pool(torch_mod, last_hidden_state, attention_mask):
        mask = attention_mask.unsqueeze(-1).expand(last_hidden_state.size()).float()
        summed = (last_hidden_state * mask).sum(dim=1)
        counts = mask.sum(dim=1).clamp(min=1e-9)
        return summed / counts

    # ---------- public API ----------
    def config(self) -> Dict[str, object]:
        return {
            "model_name": self.model_name,
            "family": self.family,
            "device": self.device,
            "cache_dir": self.cache_dir,
            "local_only": self.local_only,
            "trust_remote_code": self.trust_remote_code,
            "normalize": self.normalize,
            "batch_size": self.batch_size,
            "max_length": self.max_length,
        }

    def log_config(self, extra: Optional[Dict[str, object]] = None) -> None:
        cfg = self.config()
        if extra:
            cfg = {**cfg, **extra}
        logger.info("EmbeddingFactory: %s", json.dumps(cfg, indent=2, ensure_ascii=False))

    def _encode_tf(self, texts: List[str]) -> List[List[float]]:
        import torch  # 지연 import
        out_vecs: List[List[float]] = []
        self._mdl.eval()

        for i in range(0, len(texts), self.batch_size):
            chunk = texts[i : i + self.batch_size]
            with torch.no_grad():
                batch = self._tok(
                    chunk,
                    padding=True,
                    truncation=True,
                    max_length=self.max_length,
                    return_tensors="pt",
                )
                batch = {k: v.to(self.device) for k, v in batch.items()}
                outputs = self._mdl(**batch)
                pooled = self._pool(torch, outputs.last_hidden_state, batch["attention_mask"])
                if self.normalize:
                    pooled = self._l2norm_tensor(torch, pooled)
                out_vecs.extend(pooled.cpu().tolist())
        return out_vecs

    def embed_docs(self, docs: List[str]) -> List[List[float]]:
        if self.family == "e5":
            docs = [f"passage: {t}" for t in docs]
        return self._encode_tf(docs)

    def embed_query(self, text: str) -> List[float]:
        if self.family == "e5":
            text = f"query: {text}"
        return self._encode_tf([text])[0]

    # noinspection PyBroadException
    def dim(self) -> int:
        try:
            hidden = getattr(self._mdl.config, "hidden_size", None)
            if hidden:
                return int(hidden)
            return len(self.embed_query("dim-probe"))
        except Exception:
            m = self.model_name.lower()
            if "small" in m or "minilm" in m: return 384
            if "base" in m: return 768
            if "large" in m: return 1024
            return int(os.getenv("EMBEDDING_DIM", "768"))

    def warmup(self) -> None:
        try:
            _ = self.embed_docs(["warmup"])
        except Exception:
            logger.exception("Warmup failed")
            raise

