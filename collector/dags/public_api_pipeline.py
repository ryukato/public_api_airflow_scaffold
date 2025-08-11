from __future__ import annotations
# Public API ingestion DAG: fetch -> parse -> upsert (Mongo)
import logging
import os
from datetime import timedelta
import pendulum
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from collector.fetcher.api_client import PublicApiClient
from collector.parser.record_parser import parse_records, parse_item_dummy
from collector.repository.mongo_collection_adapter import MongoCollectionAdapter

# Avoid proxy issues on some macOS/dev setups
os.environ.setdefault("NO_PROXY", "*")

API_BASE_URL = os.getenv("API_BASE_URL", "http://apis.data.go.kr")
ENDPOINT = "/v1/example"  # Replace with your dataset endpoint
API_KEY = os.getenv("API_KEY")  # masked in examples; set real key only in local .env
NUM_OF_ROWS = int(os.getenv("PUBLIC_API_PAGE_SIZE", "100"))  # many public APIs limit to 100
MONGO_CONN_ID = os.getenv("MONGO_CONN_ID", "mongo_default")
MONGO_DB = os.getenv("MONGO_DB", "test")

logger = logging.getLogger(__name__)
seoul_tz = pendulum.timezone("Asia/Seoul")

@dag(
    dag_id="public_api_pipeline",
    schedule="@daily",
    start_date=pendulum.datetime(2025, 8, 1, tz=seoul_tz),
    catchup=False,
    tags=["public-data", "scaffold"],
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
)
def public_api_pipeline():
    @task(
        retries=3,
        retry_exponential_backoff=True,
        retry_delay=timedelta(seconds=10),
        execution_timeout=timedelta(minutes=1),
        pool="public_api_pool",
        pool_slots=1,
    )
    def get_total_pages() -> list[int]:
        """Resolve totalCount from first page; return [1..N] for dynamic mapping."""
        client = PublicApiClient(base_url=API_BASE_URL, service_key=API_KEY)
        first = client.fetch_page(
            endpoint=ENDPOINT,
            page_no=1,
            num_of_rows=NUM_OF_ROWS,
            records_path=("body", "items"),
            total_count_path=("body", "totalCount"),
        )
        total = first.get("total_count")
        if isinstance(total, str) and total.isdigit():
            total = int(total)
        if not isinstance(total, int) or total <= 0:
            return [1]
        page_count = (total + NUM_OF_ROWS - 1) // NUM_OF_ROWS
        page_count = int(page_count)  # ensure SupportsIndex
        return list(range(1, page_count + 1))

    @task(
        retries=3,
        retry_exponential_backoff=True,
        retry_delay=timedelta(seconds=10),
        execution_timeout=timedelta(minutes=2),
        pool="public_api_pool",
        pool_slots=1,
    )
    def process_page(page_no: int) -> bool:
        """Fetch -> parse -> upsert for one page; return True on any change."""
        client = PublicApiClient(base_url=API_BASE_URL, service_key=API_KEY)
        payload = client.fetch_page(
            endpoint=ENDPOINT,
            page_no=page_no,
            num_of_rows=NUM_OF_ROWS,
            records_path=("body", "items"),
            total_count_path=("body", "totalCount"),
        )
        raw_records = payload.get("records", [])
        logger.info("Fetched page=%s size=%s total=%s", page_no, len(raw_records), payload.get("total_count"))

        docs = parse_records(raw_records, parse_item_dummy)
        if not docs:
            logger.warning("No docs parsed for page=%s", page_no)
            return True

        adapter = MongoCollectionAdapter(mongo_conn_id=MONGO_CONN_ID, database=MONGO_DB)
        n = adapter.dataset_a_upsert_many(docs)  # generic dataset key
        logger.info("Upserted/Updated=%s page=%s", n, page_no)
        return n > 0

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    pages = get_total_pages()
    processed = process_page.expand(page_no=pages)

    start >> pages >> processed >> end

dag = public_api_pipeline()
