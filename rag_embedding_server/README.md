# RAG Embedding Server

## Description

RAG Embedding Server는 Batch 기반 임베딩 요청을 수신하고, 내부 임베딩 워커 큐를 통해 순차적으로 처리한 뒤 Qdrant에 저장하는 백엔드 서버입니다.  
처리량 제어(backpressure), 큐 사이즈 제한, 요청 크기 제한 등을 통해 안정적인 벡터화 처리를 지원합니다.

- Framework: FastAPI
- Embedding Model: `sentence-transformers` 호환 모델 (ex. MiniLM, E5 등)
- Vector DB: Qdrant (local or remote)
- Device 지원: CPU / MPS (macOS) / CUDA (Linux)
- 확장 지향 구조: 백엔드 전환 (Kafka, Redis, DB), 임베딩 전략 교체 용이

---

## Setup

### Python venv

```bash
# Python 3.10.14 기준
python3.10 -m venv .venv
source .venv/bin/activate
```

### Install dependencies
```
pip install --upgrade pip
pip install -r requirements.txt
```

## How to run
### 설정 파일 준비
.env 파일을 다음과 같이 구성합니다

```
# Embedding 처리 관련
RAG_EMBED_MODEL=sentence-transformers/all-MiniLM-L6-v2
RAG_EMBED_DEVICE=cpu            # 또는 mps, cuda
RAG_EMBED_SKIP_MPS_CHECK=true
RAG_EMBED_LOCAL_ONLY=true

# Backpressure 및 요청 제한
MAX_BATCH_ITEMS=100
MAX_PAYLOAD_BYTES=100000
MAX_QUEUE_SIZE=500

# Qdrant 설정
QDRANT_HOST=http://localhost:6333
QDRANT_COLLECTION=test-rag-chunks
```
### 서버 실행
```
./scripts/start-rag-embedding-server.sh
```

> Note
> 직접 실행 
> ```
> uvicorn app.main:app --reload
> ```

## Available Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | /embed | EmbeddingChunk 목록 전송 및 처리 큐 등록|
| GET| /model-info | 현재 모델 및 디바이스 설정 반환|
| GET| /limits |최대 큐 사이즈, 요청 사이즈 등 반환|
| GET| /current-task-count | 현재 큐에 적재된 작업 수 반환|

## TODO
확장 포인트
* 임베딩 모델 확장: get_embedding_model() 커스터마이징으로 다양한 모델 도입 가능
* 멀티워커 구조: 별도 프로세스로 워커 실행 (Docker Compose 기반)
* 비동기 응답 처리: REST 응답은 202, 결과는 후속 저장 or 콜백으로
* 벡터 저장소 전환: Qdrant 외 Pinecone, Weaviate 등 확장 가능
* 메시지 큐 연동: Kafka / Redis PubSub 기반 이벤트 처리 구조 도입
* API 인증 추가: Token 기반 요청 인증 지원
* Prometheus / Grafana 모니터링: 처리율, 지연, 실패율 추적