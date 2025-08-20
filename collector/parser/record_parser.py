"""
Parsing layer: convert raw JSON records to documents for persistence.
- Item-level parsing with partial-failure tolerance.
- Dummy Pydantic model to avoid domain-specific dependencies.
"""
from __future__ import annotations
import logging
from typing import Any, Dict, Iterable, List, Optional, Callable, TypeVar
from pydantic import BaseModel, Field, ConfigDict, model_validator

from collector.util.hash_util import HashUtil

logger = logging.getLogger(__name__)
Doc = Dict[str, Any]
T = TypeVar("T", bound=Doc)

class DummyItem(BaseModel):
    """Minimal example model; replace fields/aliases for your dataset."""
    # Example: dataset uses alias "ITEM_SEQ" but we normalize to "id"
    id: str = Field(alias="ITEM_SEQ")
    name: Optional[str] = Field(default=None, alias="ITEM_NAME")
    vendor: Optional[str] = Field(default=None, alias="ENTP_NAME")

    model_config = ConfigDict(populate_by_name=True)

    content_hash: Optional[str] = None  # 자동 생성 필드

    @model_validator(mode="before")
    def generate_item_key(cls, values):
        all_values_hash_value = HashUtil.generate_hashed_item_key(
            values=values,
            exclude=["content_hash", "contentHash"]
        )
        values["content_hash"] = all_values_hash_value
        return values

def parse_records(
    records: Iterable[Dict[str, Any]],
    item_parser: Callable[[Dict[str, Any]], Optional[T]],
) -> List[T]:
    """Iterate over records and apply item_parser per item; skip None and log exceptions."""
    out: List[T] = []
    for i, rec in enumerate(records):
        try:
            doc = item_parser(rec)
            if doc:
                out.append(doc)
        except Exception as e:
            logger.exception("parse error idx=%s rec=%s cause=%s", i, str(rec)[:500], e)
    return out

def parse_item_dummy(rec: Dict[str, Any]) -> Optional[Doc]:
    """Validate with DummyItem and return normalized dict (python field names, not aliases)."""
    model = DummyItem(**rec)
    doc = model.model_dump(by_alias=False)
    if "id" in doc and doc["id"] is not None:
        doc["id"] = str(doc["id"])
    return doc
