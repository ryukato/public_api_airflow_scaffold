import datetime
from typing import Dict, Any, Optional, List


class FilterProvider:

    # noinspection PyTypeChecker
    @staticmethod
    def build_needs_index_filter(embedding_version: int, *, safe_hash: bool = True) -> Dict[str, Any]:
        """
        needs-index 조건:
          - indexed 없음 OR indexed.embeddingVersion ∉ {v, str(v)}
          - (옵션) 해시 불일치:
              safe_hash=True  → 둘 다 '존재&비어있지 않음'이면 값 비교 + '존재/부재 XOR'도 포함
              safe_hash=False → ifNull("")로 직접 비교(초기 데이터 과검출 가능)
        """
        v = embedding_version
        base_or: List[Dict[str, Any]] = [
            {"indexed": {"$exists": False}},
            {"indexed.embeddingVersion": {"$nin": [v, str(v)]}},  # ✅ 타입 혼재 방어
        ]

        if safe_hash:
            # 둘 다 존재&비공백 → 값 비교
            both_present_and_diff = {
                "$and": [
                    {"contentHash": {"$exists": True, "$ne": ""}},
                    {"indexed.contentHash": {"$exists": True, "$ne": ""}},
                    {"$expr": {"$ne": ["$indexed.contentHash", "$contentHash"]}},
                ]
            }
            # 존재/부재 XOR → 재인덱스 필요
            presence_xor = {
                "$expr": {
                    "$ne": [
                        {"$and": [{"$gt": [{"$strLenCP": {"$ifNull": ["$contentHash", ""]}}, 0]}]},
                        {"$and": [{"$gt": [{"$strLenCP": {"$ifNull": ["$indexed.contentHash", ""]}}, 0]}]},
                    ]
                }
            }
            base_or.extend([both_present_and_diff, presence_xor])  # ✅ XOR 추가
        else:
            base_or.append({
                "$expr": {
                    "$ne": [
                        {"$ifNull": ["$indexed.contentHash", ""]},
                        {"$ifNull": ["$contentHash", ""]},
                    ]
                }
            })

        return {"$or": base_or}


    @staticmethod
    def build_lease_free_filter(now_utc: datetime.datetime) -> Dict[str, Any]:
        """리스가 없거나 만료된 문서만 통과시키는 필터."""
        return {
            "$or": [
                {"indexLease": {"$exists": False}},
                {"indexLease.expiresAt": {"$lte": now_utc}},
            ]
        }

