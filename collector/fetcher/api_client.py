"""
Generic HTTP client for fetching public data (paged).
- Reuses a single requests.Session with retry/backoff.
- Dataset-agnostic paging helpers.
"""
from __future__ import annotations
import os
import logging
from typing import Any, Dict, Optional, Tuple
import requests
from requests.adapters import HTTPAdapter, Retry

os.environ.setdefault("NO_PROXY", "*")
logger = logging.getLogger(__name__)

class PublicApiClient:
    def __init__(
        self,
        base_url: str,
        service_key: Optional[str] = None,
        timeout: int = 20,
        max_retries: int = 3,
        backoff_factor: float = 0.5,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.service_key = service_key or os.getenv("API_KEY")
        self.timeout = timeout

        sess = requests.Session()
        retries = Retry(
            total=max_retries,
            backoff_factor=backoff_factor,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET", "POST"),
            respect_retry_after_header=True,
        )
        adapter = HTTPAdapter(max_retries=retries, pool_connections=20, pool_maxsize=50)
        sess.mount("http://", adapter)
        sess.mount("https://", adapter)
        self.session = sess

    def _build_params(self, page_no: int, num_of_rows: int, extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        # Build public API params; customize as needed
        params: Dict[str, Any] = {
            "serviceKey": self.service_key,
            "type": "json",
            "pageNo": page_no,
            "numOfRows": num_of_rows,
        }
        if extra:
            params.update(extra)
        return params

    def _request_json(self, endpoint: str, params: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        resp = self.session.get(url, params=params, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()

    def fetch_page(
        self,
        endpoint: str,
        page_no: int,
        num_of_rows: int,
        extra_params: Optional[Dict[str, Any]] = None,
        records_path: Tuple[str, ...] = ("body", "items"),
        total_count_path: Tuple[str, ...] = ("body", "totalCount"),
    ) -> Dict[str, Any]:
        # Fetch a single page and extract records/total_count using path parameters
        params = self._build_params(page_no=page_no, num_of_rows=num_of_rows, extra=extra_params)
        payload = self._request_json(endpoint, params)

        def dig(d: Dict[str, Any], path: Tuple[str, ...], default=None):
            cur: Any = d
            for k in path:
                if not isinstance(cur, dict) or k not in cur:
                    return default
                cur = cur[k]
            return cur

        records = dig(payload, records_path, default=[]) or []
        if not isinstance(records, list):
            # Normalize {"item": [...]} shape
            if isinstance(records, dict) and "item" in records and isinstance(records["item"], list):
                records = records["item"]
            else:
                logger.warning("Unexpected records shape at %s: %s", records_path, type(records))
                records = []

        total_count = dig(payload, total_count_path, default=None)
        return {"records": records, "total_count": total_count}
