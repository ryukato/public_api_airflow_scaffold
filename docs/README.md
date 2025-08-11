# Public API Ingestion Scaffold (Airflow)

이 리포지토리는 **공공/외부 API 데이터를 수집하기 위한 Airflow 스캐폴드**입니다.  
민감정보는 예시에서도 모두 마스킹/플레이스홀더로만 제공합니다(**실제 키 X**).  
모델은 도메인 의존성을 제거하기 위해 **Dummy 모델**로 대체되어 있습니다.



## 설치
설치 방법 및 설정 그리고 실행 방법은 [installation guide](./installation_guide.md)를 참고해주세요.

## 구성 요약
- 클라이언트(`collector/fetcher/api_client.py`) — 재시도/세션 재사용 포함
- 파서(`collector/parser/record_parser.py`) — 아이템 단위 파싱 + Dummy 모델(Pydantic v2)
- 저장소/어댑터
  - `collector/repository/mongo_write_repository.py` — pymongo 전용 레포(단위테스트 용이)
  - `collector/repository/mongo_collection_adapter.py` — Airflow MongoHook 사용, 데이터셋별 upsert 진입점
- DAG(`dags/public_api_pipeline.py`) — TaskFlow + Dynamic Task Mapping
- 테스트(`tests/*`) — 파서/어댑터 스모크 테스트

## 빠른 시작 (Docker 권장)
1) `.env.example`를 복사해 `.env`로 생성하고 값을 채웁니다(예: `API_KEY=****`).
2) `docker compose up -d` 로 Mongo/Airflow를 기동합니다.
3) Airflow 초기화/계정 생성:
   ```bash
   docker compose run --rm airflow-webserver airflow db init
   docker compose run --rm airflow-webserver airflow users create \     --username admin --firstname a --lastname b --role Admin --email admin@example.com --password admin
   ```
4) 웹 UI: http://localhost:8080 (기본 계정: admin/admin)

## 환경 변수(.env)
- `API_BASE_URL` — 수집 대상 API 베이스 URL
- `API_KEY` — **플레이스홀더만 사용**, 실제 키는 개인 환경에서만 설정
- `MONGO_CONN_ID` — Airflow 커넥션 ID (예: `mongo_default`)
- `MONGO_DB` — DB명(예: `test`)

> ⚠️ **주의**: 실제 키/비밀번호를 이 저장소에 커밋하지 마세요. `.env`는 반드시 로컬 전용.

## 테스트
```bash
pip install -r requirements.txt
pytest -q
```

## 라이선스
MIT
