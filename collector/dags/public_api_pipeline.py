from __future__ import annotations

# Public API ingestion DAG: fetch -> parse -> upsert (Mongo)
import logging
import os
from dotenv import load_dotenv
from datetime import timedelta
import pendulum
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import get_current_context
from airflow.utils.trigger_rule import TriggerRule

from collector.fetcher.api_client import PublicApiClient
from collector.parser.record_parser import parse_records, parse_item_dummy
from collector.repository.mongo_collection_adapter import MongoCollectionAdapter
from collector.util.paging_util import PagingUtil
from collector.util.time_utility import TimeUtility

# Avoid proxy issues on some macOS/dev setups
os.environ.setdefault("NO_PROXY", "*")
load_dotenv()

API_BASE_URL = os.getenv("API_BASE_URL", "http://apis.data.go.kr")
ENDPOINT = "/v1/example"  # Replace with your dataset endpoint
API_KEY = os.getenv("API_KEY")  # masked in examples; set real key only in local .env
NUM_OF_ROWS = int(
    os.getenv("PUBLIC_API_PAGE_SIZE", "100")
)  # many public APIs limit to 100
MONGO_CONN_ID = os.getenv("MONGO_CONN_ID", "mongo_default")
MONGO_DB = os.getenv("MONGO_DB", "test")

logger = logging.getLogger(__name__)
seoul_tz = pendulum.timezone("Asia/Seoul")

PAGE_BATCH_SIZE = 100
RAW_DATA_TYPE_NAME = "DummyData"


def fetch_first_page_for_pagination(client: PublicApiClient):
    # First call to resolve totalCount
    first = client.fetch_page(
        endpoint=ENDPOINT,
        page_no=1,
        num_of_rows=NUM_OF_ROWS,
        records_path=("body", "items"),
        total_count_path=("body", "totalCount"),
    )
    return first


@dag(
    dag_id="public_api_pipeline",
    schedule="@daily",
    start_date=pendulum.datetime(2025, 8, 1, tz=seoul_tz),
    catchup=False,
    tags=["public-data", "scaffold"],
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
)
def public_api_pipeline():
    def get_run_date_kst_str() -> str:
        ctx = get_current_context()
        return TimeUtility.get_date_kst_str(context=ctx)

    """
    get_page_batches to divide a large number of pages into smaller batches, 
    allowing us to process them in manageable chunks and avoid exceeding Airflow’s task argument size or mapping limits. 
    """

    @task(
        retries=3,
        retry_exponential_backoff=True,
        retry_delay=timedelta(seconds=10),
        execution_timeout=timedelta(minutes=1),
        pool="raw_data_api",
        pool_slots=1,
    )
    def get_page_batches() -> list[dict]:
        client = PublicApiClient(base_url=API_BASE_URL, service_key=API_KEY)
        first = fetch_first_page_for_pagination(client)
        total = first.get("total_count")
        page_count = PagingUtil.get_page_count(total=total, page_size=NUM_OF_ROWS)
        return [
            {"page_batch": list(range(i, min(i + PAGE_BATCH_SIZE, page_count + 1)))}
            for i in range(1, page_count + 1, PAGE_BATCH_SIZE)
        ]

    @task(
        pool="raw_data_api",
        pool_slots=1,
        max_active_tis_per_dag=5,
        retries=5,
        retry_exponential_backoff=True,
        retry_delay=timedelta(seconds=10),
        execution_timeout=timedelta(minutes=2),
    )
    def process_page_batch(page_batch: list[int]) -> int:
        logger = logging.getLogger(__name__)
        client = PublicApiClient(base_url=API_BASE_URL, service_key=API_KEY)
        adapter = MongoCollectionAdapter(mongo_conn_id=MONGO_CONN_ID, database=MONGO_DB)
        total_upserted = 0

        try:
            for page_no in page_batch:
                n = process_page(page_no=page_no, client=client, adapter=adapter)
                total_upserted += n

            return total_upserted
        except Exception as error:
            logger.exception("Failed to process batch: %s", error)
            return total_upserted
        finally:
            client.close()
            adapter.close()

    @task(
        retries=3,
        retry_exponential_backoff=True,
        retry_delay=timedelta(seconds=10),
        execution_timeout=timedelta(minutes=2),
        pool="raw_data_api",
        pool_slots=1,
    )
    def process_page(
        page_no: int, client: PublicApiClient, adapter: MongoCollectionAdapter
    ) -> int:
        logger = logging.getLogger(__name__)
        """Fetch -> parse -> upsert for one page; return True on any change."""
        run_date_kst = get_run_date_kst_str()
        try:
            # skip if already processed
            if adapter.is_processed(
                api_name=ENDPOINT, run_date=run_date_kst, page_no=page_no
            ):
                logger.info(
                    "Skip already processed page=%s runDate=%s", page_no, run_date_kst
                )
                return 0
            # Fetch
            logger.info(f"STEP A: {RAW_DATA_TYPE_NAME} fetch start")
            payload = client.fetch_page(
                endpoint=ENDPOINT,
                page_no=page_no,
                num_of_rows=NUM_OF_ROWS,
                records_path=("body", "items"),
                total_count_path=("body", "totalCount"),
            )
            raw_records = payload.get("records", [])
            logger.info(
                f"fetched {RAW_DATA_TYPE_NAME}: page={page_no}, fetched-size={len(raw_records)}, total_count:{payload['total_count']}"
            )

            # Parse (dataset-specific logic picked by key)
            docs = parse_records(raw_records, parse_item_dummy)
            if not docs:
                logger.warning("No docs parsed for page=%s", page_no)
                return True

            logger.info(f"STEP C: {RAW_DATA_TYPE_NAME} upsert start")
            n = adapter.dataset_a_upsert_many(docs)  # generic dataset key
            run_date_kst = get_run_date_kst_str()
            adapter.mark_processed(
                ENDPOINT, run_date_kst, page_no, processed_size=len(docs)
            )
            logger.info("Upserted/Updated=%s page=%s", n, page_no)
            return n
        except Exception as e:
            # This catches Python exceptions; native signals still kill the process.
            logger.exception("Task failed at page=%s: %s", page_no, e)
            return 0
        finally:
            logger.info(f"STEP D: {RAW_DATA_TYPE_NAME} cleanup")

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE)
    batches = get_page_batches()
    processed = process_page_batch.expand_kwargs(batches)
    start >> batches >> processed >> end


dag = public_api_pipeline()
