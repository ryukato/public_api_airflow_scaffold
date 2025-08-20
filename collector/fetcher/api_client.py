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
import xml.etree.ElementTree as et
from requests.adapters import HTTPAdapter, Retry

os.environ.setdefault("NO_PROXY", "*")
logger = logging.getLogger(__name__)

SERVICE_KEY_PARAM_NAME = "serviceKey"


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

    def close(self) -> None:
        # Close underlying HTTP session to release sockets
        self.session.close()

    def __enter__(self):
        # Enable usage: with RawDrugDataClient(...) as client:
        return self

    def __exit__(self, exc_type, exc, tb):
        # Ensure session is closed on context exit
        self.close()

    def build_params(
        self,
        page_no: int,
        num_of_rows: int,
        extra: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {
            SERVICE_KEY_PARAM_NAME: self.service_key,
            "type": "json",
            "pageNo": page_no,
            "numOfRows": num_of_rows,
        }
        if extra:
            params.update(extra)
        return params

    def build_params_for_xml(
        self,
        page_no: int,
        num_of_rows: int,
        extra: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {
            SERVICE_KEY_PARAM_NAME: self.service_key,
            "pageNo": page_no,
            "numOfRows": num_of_rows,
        }
        if extra:
            params.update(extra)
        return params

    def _request_json(self, endpoint: str, params: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        masked_params = params.copy()
        if SERVICE_KEY_PARAM_NAME in masked_params:
            key = masked_params[SERVICE_KEY_PARAM_NAME]
            masked_params[SERVICE_KEY_PARAM_NAME] = (
                f"{key[:5]}...{key[-3:]}" if isinstance(key, str) else "***MASKED***"
            )

        logger.info(f"Request to url={url}, params={masked_params}")
        resp = self.session.get(url, params=params, timeout=self.timeout)
        resp.raise_for_status()
        try:
            return resp.json()
        except ValueError as e:
            logger.error(
                "Non-JSON response. status=%s text_prefix=%s",
                resp.status_code,
                resp.text[:200],
            )
            raise

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
        params = self._build_params(
            page_no=page_no, num_of_rows=num_of_rows, extra=extra_params
        )
        payload = self._request_json(endpoint, params)

        def dig(d: Dict[str, Any], path: Tuple[str, ...], default=None):
            cur: Any = d
            for k in path:
                if not isinstance(cur, dict) or k not in cur:
                    return default
                cur = cur[k]
            return cur

        records = dig(payload, records_path, default=[])
        total_count = dig(payload, total_count_path, default=None)
        if records is None:
            records = []
        if not isinstance(records, list):
            # some APIs return object with 'item' key
            if (
                isinstance(records, dict)
                and "item" in records
                and isinstance(records["item"], list)
            ):
                records = records["item"]
            else:
                logger.warning(
                    "Unexpected records shape at %s: %s", records_path, type(records)
                )
                records = []

        return {"records": records, "total_count": total_count}

    def fetch_page_in_xml(
        self,
        endpoint: str,
        page_no: int,
        num_of_rows: int,
        extra_params: Optional[Dict[str, Any]] = None,
        records_path: Tuple[str, ...] = ("body", "items", "item"),
        total_count_path: Tuple[str, ...] = ("body", "totalCount"),
    ) -> Dict[str, Any]:
        """Fetch a single page and parse XML; returns {'records': list, 'total_count': int}"""
        params = self.build_params_for_xml(
            page_no=page_no, num_of_rows=num_of_rows, extra=extra_params
        )
        # params["type"] = "xml"  # ensure type is XML

        try:
            response = requests.get(
                f"{self.base_url}/{endpoint}", params=params, timeout=(10, 10)
            )
            response.raise_for_status()
            root = et.fromstring(response.content)
        except Exception as e:
            logger.exception("Failed to fetch or parse XML from %s: %s", endpoint, e)
            return {"records": [], "total_count": None}

        def dig_xml(element: et.Element, path: Tuple[str, ...]) -> Optional[et.Element]:
            cur = element
            for tag in path:
                cur = cur.find(tag)
                if cur is None:
                    return None
            return cur

        # extract totalCount
        total_count_elem = dig_xml(root, total_count_path)
        total_count = (
            int(total_count_elem.text)
            if total_count_elem is not None and total_count_elem.text
            else None
        )

        # extract record list
        records_container = dig_xml(root, records_path[:-1])
        record_tag = records_path[-1]
        records = []

        if records_container is not None:
            for item in records_container.findall(record_tag):
                # convert each <item> into a dict
                record = {child.tag: child.text for child in item}
                records.append(record)

        return {"records": records, "total_count": total_count}
