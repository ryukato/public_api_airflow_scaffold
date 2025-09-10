import hashlib
import requests
import time

# 단일 row (예시 CSV에서 추출)
paragraph_row = {
    "item_sequence": "201707337",
    "name": "test",
    "contents": "test"
}

# ID 생성 함수
def make_hash_id(item_seq, name) -> str:
    raw = f"{item_seq}-{name}"
    return hashlib.md5(raw.encode("utf-8")).hexdigest()

# EmbeddingChunk 생성 함수
def build_chunk(row: dict) -> dict:
    return {
        "id": make_hash_id(
            row["item_sequence"],
            row["name"],
        ),
        "text": row["contents"]
    }

# 테스트 실행
URL = "http://localhost:8000/embed"

for i in range(1, 101):
    try:
        chunk = build_chunk(paragraph_row)
        response = requests.post(URL, json=[chunk])  # <- list of one chunk
        print(f"[{i}] Status: {response.status_code}, Response: {response.json()}")
    except Exception as e:
        print(f"[{i}] Error: {e}")
    time.sleep(0.1)  # throttle a bit