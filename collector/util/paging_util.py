from typing import Any


class PagingUtil:
    @staticmethod
    def get_page_count(total: Any, page_size: int) -> int:
        if isinstance(total, str) and total.isdigit():
            total = int(total)
        if total <= 0 or page_size <= 0:
            return 0
        if total <= page_size:
            return 1

        page_count = (total + page_size - 1) // page_size
        page_count = int(page_count)
        return page_count
