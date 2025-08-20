import datetime
import pendulum
from airflow.utils.context import Context

class TimeUtility:
    @staticmethod
    def now_utc() -> datetime.datetime:
        """Return tz-aware UTC datetime to avoid naive/aware mismatches."""
        return datetime.datetime.now(tz=pendulum.timezone("UTC"))

    @staticmethod
    def expire_from_now(lease_seconds: int) -> datetime:
        now = TimeUtility.now_utc()
        return now + datetime.timedelta(seconds=lease_seconds)

    @staticmethod
    def expire_from(now: datetime, lease_seconds: int) -> datetime:
        return now + datetime.timedelta(seconds=lease_seconds)

    @staticmethod
    def get_date_kst_str(context: Context) -> str:
        """
        Returns the DAG logical_date as a KST (Asia/Seoul) date string (YYYY-MM-DD).
            Useful for daily job partitioning.
        """
        return context["logical_date"].in_timezone("Asia/Seoul").to_date_string()
