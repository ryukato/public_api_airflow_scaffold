from __future__ import annotations
from typing import Optional
from datetime import datetime, timezone
from pymongo.collection import Collection
from pymongo import ASCENDING

class ProcessedPageRepository:
    def __init__(self, collection: Collection) -> None:
        self.col = collection
        # Ensure unique index on (apiName, runDate, pageNo)
        self.col.create_index(
            [("apiName", ASCENDING), ("runDate", ASCENDING), ("pageNo", ASCENDING)],
            unique=True,
            background=True,
            name="uq_api_runDate_pageNo",
        )
        self.col.create_index("processedAt", expireAfterSeconds=90*24*3600, background=True)

    def is_processed(self, api_name: str, run_date: str, page_no: int) -> bool:
        """Return True if already processed for (api_name, run_date, page_no)."""
        return self.col.find_one(
            {"apiName": api_name, "runDate": run_date, "pageNo": int(page_no)},
            projection={"_id": 1},
        ) is not None

    def mark_processed(self, api_name: str, run_date: str, page_no: int, when: Optional[datetime] = None) -> None:
        """Upsert a processed marker document."""
        when = when or datetime.now(tz=timezone.utc)
        self.col.update_one(
            {"apiName": api_name, "runDate": run_date, "pageNo": int(page_no)},
            {"$set": {"processedAt": when}},
            upsert=True,
        )

    def unmark_processed(self, api_name: str, run_date: str, page_no: int) -> int:
        """Delete processed marker; return deleted count."""
        res = self.col.delete_one(
            {"apiName": api_name, "runDate": run_date, "pageNo": int(page_no)}
        )
        return int(res.deleted_count or 0)

    def list_processed_pages(self, api_name: str, run_date: str) -> Set[int]:
        """
        Return a set of processed page numbers for (apiName, runDate).
        - Single query with projection to minimize payload
        """
        cur = self.col.find(
            {"apiName": api_name, "runDate": run_date},
            projection={"pageNo": 1, "_id": 0},
        )
        return {int(doc["pageNo"]) for doc in cur if "pageNo" in doc}
